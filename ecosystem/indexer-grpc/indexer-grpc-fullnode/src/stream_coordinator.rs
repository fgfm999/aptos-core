// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    convert::convert_transaction,
    counters::{observe_api_conversion_work, UNABLE_TO_FETCH_TRANSACTION},
    runtime::{DEFAULT_NUM_RETRIES, RETRY_TIME_MILLIS},
};
use aptos_api::context::Context;
use aptos_api_types::{AsConverter, Transaction as APITransaction, TransactionOnChainData};
use aptos_indexer_grpc_utils::{
    chunk_transactions,
    constants::MESSAGE_SIZE_LIMIT,
    counters::{log_grpc_step_fullnode, observe_indexer_stage, IndexerGrpcStep},
};
use aptos_logger::{error, info, sample, sample::SampleRate, warn};
use aptos_protos::{
    indexer::v1::ProcessedRange,
    internal::fullnode::v1::{
        transactions_from_node_response, TransactionsFromNodeResponse, TransactionsOutput,
    },
    transaction::v1::{
        EventSizeInfo, Transaction as TransactionPB, TransactionSizeInfo, WriteOpSizeInfo,
    },
    util::timestamp::Timestamp,
};
use aptos_transaction_filter::{BooleanTransactionFilter, Filterable};
use itertools::Itertools;
use serde::Serialize;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tonic::Status;

type EndVersion = u64;

#[derive(Debug)]
pub struct StreamResponse {
    pub response: TransactionsFromNodeResponse,
    pub processed_range: Option<ProcessedRange>,
}

impl From<TransactionsFromNodeResponse> for StreamResponse {
    fn from(response: TransactionsFromNodeResponse) -> Self {
        Self {
            response,
            processed_range: None,
        }
    }
}

/// Checks if an error message indicates that the requested version has been pruned.
fn is_pruned_error(err: &anyhow::Error) -> bool {
    let err_str = err.to_string();
    err_str.contains("is pruned")
}

const SERVICE_TYPE: &str = "indexer_fullnode";
const MINIMUM_TASK_LOAD_SIZE_IN_BYTES: usize = 100_000;

// Basically a handler for a single GRPC stream request
pub struct IndexerStreamCoordinator {
    pub current_version: u64,
    pub end_version: u64,
    pub processor_task_count: u16,
    pub processor_batch_size: u16,
    pub output_batch_size: u16,
    pub highest_known_version: u64,
    pub context: Arc<Context>,
    pub transactions_sender: mpsc::Sender<Result<StreamResponse, tonic::Status>>,
    pub filter: Option<BooleanTransactionFilter>,
    abort_handle: Option<Arc<AtomicBool>>,
    wait_stage: &'static str,
}

// Single batch of transactions to fetch, convert, and stream
#[derive(Clone, Copy)]
pub struct TransactionBatchInfo {
    pub start_version: u64,
    pub head_version: u64,
    pub num_transactions_to_fetch: u16,
}

impl IndexerStreamCoordinator {
    /// Coordinates the fetching, processing, and streaming of transactions
    pub fn new(
        context: Arc<Context>,
        request_start_version: u64,
        end_version: u64,
        processor_task_count: u16,
        processor_batch_size: u16,
        output_batch_size: u16,
        transactions_sender: mpsc::Sender<Result<StreamResponse, tonic::Status>>,
        filter: Option<BooleanTransactionFilter>,
        abort_handle: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self {
            current_version: request_start_version,
            end_version,
            processor_task_count,
            processor_batch_size,
            output_batch_size,
            highest_known_version: 0,
            context,
            transactions_sender,
            filter,
            abort_handle,
            wait_stage: "fullnode_wait_ledger",
        }
    }

    /// Fans out a bunch of threads and processes transactions in parallel.
    /// Pushes results in parallel to the stream, but only return that the batch is
    /// fully completed if every job in the batch is successful
    /// Processing transactions in 4 stages:
    /// 1. Fetch transactions from storage
    /// 2. Convert transactions to rust objects (for example stringifying move structs into json)
    /// 3. Convert into protobuf objects
    /// 4. Encode protobuf objects (base64)
    pub async fn process_next_batch(&mut self) -> Vec<Result<EndVersion, Status>> {
        let fetching_start_time = std::time::Instant::now();
        // Stage 1: fetch transactions from storage.
        let sorted_transactions_from_storage_with_size =
            match self.fetch_transactions_from_storage().await {
                Ok(txns) => txns,
                Err(status) => {
                    // Send the error to the client via the channel.
                    let _ = self.transactions_sender.send(Err(status.clone())).await;
                    return vec![Err(status)];
                },
            };
        if sorted_transactions_from_storage_with_size.is_empty() {
            return vec![];
        }
        let first_version = sorted_transactions_from_storage_with_size
            .first()
            .map(|(txn, _)| txn.version)
            .unwrap() as i64;
        let end_version = sorted_transactions_from_storage_with_size
            .last()
            .map(|(txn, _)| txn.version)
            .unwrap() as i64;
        let num_transactions = sorted_transactions_from_storage_with_size.len();
        let highest_known_version = self.highest_known_version as i64;
        let (_, _, block_event) = self
            .context
            .db
            .get_block_info_by_version(end_version as u64)
            .unwrap_or_else(|_| {
                panic!(
                    "[Indexer Fullnode] Could not get block_info for version {}",
                    end_version,
                )
            });
        let last_transaction_timestamp_in_microseconds = block_event.proposed_time();
        let last_transaction_timestamp = Some(Timestamp {
            seconds: (last_transaction_timestamp_in_microseconds / 1_000_000) as i64,
            nanos: ((last_transaction_timestamp_in_microseconds % 1_000_000) * 1000) as i32,
        });

        log_grpc_step_fullnode(
            IndexerGrpcStep::FullnodeFetchedBatch,
            Some(first_version),
            Some(end_version),
            last_transaction_timestamp.as_ref(),
            Some(highest_known_version),
            None,
            Some(fetching_start_time.elapsed().as_secs_f64()),
            Some(num_transactions as i64),
        );
        // Stage 2: convert transactions to rust objects. CPU-bound load.
        let decoding_start_time = std::time::Instant::now();
        let mut task_batches = vec![];
        let mut current_batch = vec![];
        let mut current_batch_size = 0;
        for (txn, size) in sorted_transactions_from_storage_with_size {
            current_batch.push(txn);
            current_batch_size += size;
            if current_batch_size > MINIMUM_TASK_LOAD_SIZE_IN_BYTES {
                task_batches.push(current_batch);
                current_batch = vec![];
                current_batch_size = 0;
            }
        }
        if !current_batch.is_empty() {
            task_batches.push(current_batch);
        }

        let output_batch_size = self.output_batch_size;
        let ledger_chain_id = self.context.chain_id().id();
        let filter = self.filter.clone();
        let mut tasks = vec![];
        for batch in task_batches {
            let context = self.context.clone();
            let filter = filter.clone();
            let queued_at = std::time::Instant::now();
            let task = tokio::task::spawn_blocking(move || {
                let start_version = batch.first().unwrap().version;
                let end_version = batch.last().unwrap().version;
                observe_indexer_stage(
                    "conversion_queue",
                    start_version,
                    end_version,
                    queued_at.elapsed(),
                );
                let api_start = std::time::Instant::now();
                let raw_txns = batch;
                let api_txns = Self::convert_to_api_txns(context, raw_txns);
                observe_indexer_stage(
                    "api_conversion",
                    start_version,
                    end_version,
                    api_start.elapsed(),
                );
                let pb_start = std::time::Instant::now();
                let pb_txns = Self::convert_to_pb_txns(api_txns);
                observe_indexer_stage(
                    "protobuf_construction",
                    start_version,
                    end_version,
                    pb_start.elapsed(),
                );
                let response_start = std::time::Instant::now();
                // Apply filter if present.
                let pb_txns = if let Some(ref filter) = filter {
                    pb_txns
                        .into_iter()
                        .filter(|txn| filter.matches(txn))
                        .collect::<Vec<_>>()
                } else {
                    pb_txns
                };
                let responses = responses_with_ranges(
                    pb_txns,
                    start_version,
                    end_version,
                    output_batch_size as usize,
                    ledger_chain_id as u32,
                    MESSAGE_SIZE_LIMIT,
                );
                observe_indexer_stage(
                    "filter_and_chunk",
                    start_version,
                    end_version,
                    response_start.elapsed(),
                );
                (
                    responses,
                    std::time::Instant::now(),
                    start_version,
                    end_version,
                )
            });
            tasks.push(task);
        }
        let timings =
            match send_ordered_batches(tasks, &self.transactions_sender, decoding_start_time).await
            {
                Ok(Some(timings)) => timings,
                Ok(None) => return vec![],
                Err(status) => return vec![Err(status)],
            };
        log_grpc_step_fullnode(
            IndexerGrpcStep::FullnodeDecodedBatch,
            Some(first_version),
            Some(end_version),
            last_transaction_timestamp.as_ref(),
            Some(highest_known_version),
            None,
            Some(timings.0.as_secs_f64()),
            Some(num_transactions as i64),
        );
        log_grpc_step_fullnode(
            IndexerGrpcStep::FullnodeSentBatch,
            Some(first_version),
            Some(end_version),
            last_transaction_timestamp.as_ref(),
            Some(highest_known_version),
            None,
            Some(timings.1.as_secs_f64()),
            Some(num_transactions as i64),
        );
        vec![Ok(end_version as u64)]
    }

    /// Fetches transactions from storage with each transaction's size.
    /// Results are transactions sorted by version.
    /// Returns an error if the transactions cannot be fetched (e.g., if the version is pruned).
    async fn fetch_transactions_from_storage(
        &mut self,
    ) -> Result<Vec<(TransactionOnChainData, usize)>, Status> {
        let batches = self.get_batches().await;
        let mut storage_fetch_tasks = vec![];
        let ledger_version = self.highest_known_version;
        for batch in batches {
            let context = self.context.clone();
            let queued_at = std::time::Instant::now();
            let task = tokio::spawn(async move {
                observe_indexer_stage(
                    "storage_queue",
                    batch.start_version,
                    batch.start_version + u64::from(batch.num_transactions_to_fetch) - 1,
                    queued_at.elapsed(),
                );
                Self::fetch_raw_txns_with_retries(context.clone(), ledger_version, batch).await
            });
            storage_fetch_tasks.push(task);
        }

        let task_results = match futures::future::try_join_all(storage_fetch_tasks).await {
            Ok(res) => res,
            Err(err) => {
                return Err(Status::internal(format!(
                    "[Indexer Fullnode] Error fetching transaction batches: {:?}",
                    err
                )));
            },
        };

        // Check each task result for errors.
        let mut transactions_from_storage = Vec::new();
        for result in task_results {
            match result {
                Ok(txns) => transactions_from_storage.extend(txns),
                Err(status) => return Err(status),
            }
        }

        let prepare_start = std::time::Instant::now();
        let transactions = transactions_from_storage
            .into_iter()
            .sorted_by(|a, b| a.version.cmp(&b.version))
            .map(|txn| {
                let size = bcs::serialized_size(&txn).expect("Unable to serialize txn");
                (txn, size)
            })
            .collect::<Vec<_>>();
        if let (Some((first, _)), Some((last, _))) = (transactions.first(), transactions.last()) {
            observe_indexer_stage(
                "sort_and_size",
                first.version,
                last.version,
                prepare_start.elapsed(),
            );
        }
        Ok(transactions)
    }

    /// Gets the last version of the batch if the entire batch is successful, otherwise return error
    pub fn get_max_batch_version(
        results: Vec<Result<EndVersion, Status>>,
    ) -> Result<EndVersion, Status> {
        let mut max_version = 0;
        for result in results {
            match result {
                Ok(version) => {
                    max_version = std::cmp::max(max_version, version);
                },
                Err(err) => {
                    return Err(err);
                },
            }
        }
        Ok(max_version)
    }

    /// This will create batches based on the configuration of the request
    async fn get_batches(&mut self) -> Vec<TransactionBatchInfo> {
        if !self.ensure_highest_known_version().await {
            return vec![];
        }

        let mut starting_version = self.current_version;
        let mut num_fetches = 0;
        let mut batches = vec![];
        let end_version = std::cmp::min(self.end_version, self.highest_known_version + 1);

        while num_fetches < self.processor_task_count && starting_version < end_version {
            let num_transactions_to_fetch = std::cmp::min(
                self.processor_batch_size as u64,
                end_version - starting_version,
            ) as u16;

            batches.push(TransactionBatchInfo {
                start_version: starting_version,
                head_version: self.highest_known_version,
                num_transactions_to_fetch,
            });
            starting_version += num_transactions_to_fetch as u64;
            num_fetches += 1;
        }
        batches
    }

    pub async fn fetch_raw_txns_with_retries(
        context: Arc<Context>,
        ledger_version: u64,
        batch: TransactionBatchInfo,
    ) -> Result<Vec<TransactionOnChainData>, Status> {
        let mut retries = 0;
        loop {
            let read_start = std::time::Instant::now();
            let result = context.get_transactions(
                batch.start_version,
                batch.num_transactions_to_fetch,
                ledger_version,
            );
            observe_indexer_stage(
                "storage_read_attempt",
                batch.start_version,
                batch.start_version + u64::from(batch.num_transactions_to_fetch) - 1,
                read_start.elapsed(),
            );
            match result {
                Ok(raw_txns) => return Ok(raw_txns),
                Err(err) => {
                    UNABLE_TO_FETCH_TRANSACTION.inc();

                    // Check if this is a pruning error - don't retry, return immediately.
                    if is_pruned_error(&err) {
                        warn!(
                            starting_version = batch.start_version,
                            num_transactions = batch.num_transactions_to_fetch,
                            error = format!("{:?}", err),
                            "[Indexer Fullnode] Requested version is pruned",
                        );
                        return Err(Status::out_of_range(format!(
                            "Transaction data for version {} is not available. The data may have \
                             been pruned. If this node uses fast sync, older transactions are not \
                             available. Error: {}",
                            batch.start_version, err
                        )));
                    }

                    retries += 1;

                    if retries >= DEFAULT_NUM_RETRIES {
                        error!(
                            starting_version = batch.start_version,
                            num_transactions = batch.num_transactions_to_fetch,
                            error = format!("{:?}", err),
                            "Could not fetch transactions: retries exhausted",
                        );
                        return Err(Status::internal(format!(
                            "Could not fetch {} transactions after {} retries, starting at {}: {:?}",
                            batch.num_transactions_to_fetch, retries, batch.start_version, err
                        )));
                    } else {
                        error!(
                            starting_version = batch.start_version,
                            num_transactions = batch.num_transactions_to_fetch,
                            error = format!("{:?}", err),
                            "Could not fetch transactions: will retry",
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(300)).await;
                },
            }
        }
    }

    fn convert_to_api_txns(
        context: Arc<Context>,
        raw_txns: Vec<TransactionOnChainData>,
    ) -> Vec<(APITransaction, TransactionSizeInfo)> {
        if raw_txns.is_empty() {
            return vec![];
        }
        let start_millis = chrono::Utc::now().naive_utc();

        let first_version = raw_txns.first().map(|txn| txn.version).unwrap();
        let last_version = raw_txns.last().unwrap().version;
        let num_transactions = raw_txns.len() as u64;
        let setup_start = std::time::Instant::now();
        let state_view = context.latest_state_view().unwrap();
        let converter = state_view.as_converter(context.db.clone(), context.indexer_reader.clone());
        let setup_duration = setup_start.elapsed();
        let block_start = std::time::Instant::now();

        // Enrich data with block metadata
        let (_, _, block_event) = context
            .db
            .get_block_info_by_version(first_version)
            .unwrap_or_else(|_| {
                panic!(
                    "[Indexer Fullnode] Could not get block_info for start version {}",
                    first_version,
                )
            });
        let block_duration = block_start.elapsed();
        let mut metadata_start = std::time::Instant::now();
        let mut timestamp = block_event.proposed_time();
        let mut epoch = block_event.epoch();
        let mut epoch_bcs = aptos_api_types::U64::from(epoch);
        let mut block_height = block_event.height();
        let mut block_height_bcs = aptos_api_types::U64::from(block_height);

        let mut transactions = vec![];
        let mut metadata_duration = metadata_start.elapsed();
        let mut size_duration = Duration::ZERO;
        let mut content_duration = Duration::ZERO;
        let mut assembly_duration = Duration::ZERO;
        let mut accounting_duration = Duration::ZERO;
        let mut transaction_bytes = 0u64;
        let mut event_bytes = 0u64;
        let mut write_bytes = 0u64;
        let mut events = 0u64;
        let mut writes = 0u64;
        for (ind, raw_txn) in raw_txns.into_iter().enumerate() {
            metadata_start = std::time::Instant::now();
            let txn_version = raw_txn.version;
            // Do not update block_height if first block is block metadata
            if ind > 0 {
                // Update the timestamp if the next block occurs
                if let Some(txn) = raw_txn.transaction.try_as_block_metadata_ext() {
                    timestamp = txn.timestamp_usecs();
                    epoch = txn.epoch();
                    epoch_bcs = aptos_api_types::U64::from(epoch);
                    block_height += 1;
                    block_height_bcs = aptos_api_types::U64::from(block_height);
                } else if let Some(txn) = raw_txn.transaction.try_as_block_metadata() {
                    timestamp = txn.timestamp_usecs();
                    epoch = txn.epoch();
                    epoch_bcs = aptos_api_types::U64::from(epoch);
                    block_height += 1;
                    block_height_bcs = aptos_api_types::U64::from(block_height);
                }
            }
            metadata_duration += metadata_start.elapsed();
            let size_start = std::time::Instant::now();
            let size_info = Self::get_size_info(&raw_txn);
            size_duration += size_start.elapsed();
            let accounting_start = std::time::Instant::now();
            transaction_bytes += u64::from(size_info.transaction_bytes);
            events += size_info.event_size_info.len() as u64;
            writes += size_info.write_op_size_info.len() as u64;
            event_bytes += size_info
                .event_size_info
                .iter()
                .map(|event| u64::from(event.total_bytes))
                .sum::<u64>();
            write_bytes += size_info
                .write_op_size_info
                .iter()
                .map(|write| u64::from(write.key_bytes) + u64::from(write.value_bytes))
                .sum::<u64>();
            accounting_duration += accounting_start.elapsed();
            let content_start = std::time::Instant::now();
            let converted = converter.try_into_onchain_transaction(timestamp, raw_txn);
            content_duration += content_start.elapsed();
            let assembly_start = std::time::Instant::now();
            let res = converted.map(|mut txn| {
                match txn {
                    APITransaction::PendingTransaction(_) => {
                        unreachable!(
                            "[Indexer Fullnode] Indexer should never see pending transactions"
                        )
                    },
                    APITransaction::UserTransaction(ref mut ut) => {
                        ut.info.block_height = Some(block_height_bcs);
                        ut.info.epoch = Some(epoch_bcs);
                    },
                    APITransaction::GenesisTransaction(ref mut gt) => {
                        gt.info.block_height = Some(block_height_bcs);
                        gt.info.epoch = Some(epoch_bcs);
                    },
                    APITransaction::BlockMetadataTransaction(ref mut bmt) => {
                        bmt.info.block_height = Some(block_height_bcs);
                        bmt.info.epoch = Some(epoch_bcs);
                    },
                    APITransaction::StateCheckpointTransaction(ref mut sct) => {
                        sct.info.block_height = Some(block_height_bcs);
                        sct.info.epoch = Some(epoch_bcs);
                    },
                    APITransaction::BlockEpilogueTransaction(ref mut bet) => {
                        bet.info.block_height = Some(block_height_bcs);
                        bet.info.epoch = Some(epoch_bcs);
                    },
                    APITransaction::ValidatorTransaction(ref mut vt) => {
                        let info = vt.transaction_info_mut();
                        info.block_height = Some(block_height_bcs);
                        info.epoch = Some(epoch_bcs);
                    },
                };
                txn
            });
            match res {
                Ok(transaction) => transactions.push((transaction, size_info)),
                Err(err) => {
                    UNABLE_TO_FETCH_TRANSACTION.inc();
                    error!(
                        version = txn_version,
                        error = format!("{:?}", err),
                        "[Indexer Fullnode] Could not convert from OnChainTransactions",
                    );
                    // IN CASE WE NEED TO SKIP BAD TXNS
                    // continue;
                    panic!(
                        "[Indexer Fullnode] Could not convert txn {} from OnChainTransactions: {:?}",
                        txn_version, err
                    );
                },
            }
            assembly_duration += assembly_start.elapsed();
        }

        for (stage, elapsed) in [
            ("api_setup", setup_duration),
            ("api_block_metadata_read", block_duration),
            ("api_metadata_tracking", metadata_duration),
            ("api_size_calculation", size_duration),
            ("api_transaction_conversion", content_duration),
            ("api_result_assembly", assembly_duration),
            ("api_work_accounting", accounting_duration),
        ] {
            observe_indexer_stage(stage, first_version, last_version, elapsed);
        }
        for (kind, count) in [
            ("transactions", num_transactions),
            ("events", events),
            ("write_ops", writes),
            ("transaction_bytes", transaction_bytes),
            ("event_bytes", event_bytes),
            ("write_bytes", write_bytes),
        ] {
            observe_api_conversion_work(kind, count);
        }
        aptos_logger::debug!(
            start_version = first_version,
            end_version = last_version,
            transactions = num_transactions,
            events = events,
            write_ops = writes,
            transaction_bytes = transaction_bytes,
            event_bytes = event_bytes,
            write_bytes = write_bytes,
            "Indexer API conversion workload"
        );

        if transactions.is_empty() {
            panic!("[Indexer Fullnode] No transactions!");
        }

        let fetch_millis = (chrono::Utc::now().naive_utc() - start_millis).num_milliseconds();

        info!(
            start_version = first_version,
            end_version = transactions
                .last()
                .map(|(txn, _size_info)| txn.version().unwrap())
                .unwrap_or(0),
            num_of_transactions = transactions.len(),
            fetch_duration_in_ms = fetch_millis,
            service_type = SERVICE_TYPE,
            "[Indexer Fullnode] Successfully converted transactions",
        );

        transactions
    }

    fn ser_size_u32<T: Serialize>(t: &T) -> u32 {
        bcs::serialized_size(t).expect("serialized_size() failed") as u32
    }

    fn get_size_info(raw_txn: &TransactionOnChainData) -> TransactionSizeInfo {
        TransactionSizeInfo {
            transaction_bytes: Self::ser_size_u32(&raw_txn.transaction),
            event_size_info: raw_txn
                .events
                .iter()
                .map(|event| EventSizeInfo {
                    type_tag_bytes: Self::ser_size_u32(event.type_tag()),
                    total_bytes: event.size() as u32,
                })
                .collect(),
            write_op_size_info: raw_txn
                .changes
                .write_op_iter()
                .map(|(state_key, write_op)| WriteOpSizeInfo {
                    key_bytes: Self::ser_size_u32(state_key),
                    value_bytes: write_op.bytes_size() as u32,
                })
                .collect(),
        }
    }

    fn convert_to_pb_txns(
        api_txns: Vec<(APITransaction, TransactionSizeInfo)>,
    ) -> Vec<TransactionPB> {
        api_txns
            .into_iter()
            .map(|(txn, size_info)| {
                let info = txn.transaction_info().unwrap();
                convert_transaction(
                    &txn,
                    info.block_height.unwrap().0,
                    info.epoch.unwrap().0,
                    size_info,
                )
            })
            .collect()
    }

    pub fn set_highest_known_version(&mut self) -> anyhow::Result<()> {
        let info = self.context.get_latest_ledger_info_wrapped()?;
        let latest_table_info_version = self
            .context
            .indexer_reader
            .as_ref()
            .expect("Table info reader not set")
            .get_latest_table_info_ledger_version()?
            .expect("Table info ledger version not set");

        self.wait_stage =
            if self.current_version > info.ledger_version.0 || info.ledger_version.0 == 0 {
                "fullnode_wait_ledger"
            } else {
                "fullnode_wait_table_info"
            };
        self.highest_known_version =
            std::cmp::min(info.ledger_version.0, latest_table_info_version);

        Ok(())
    }

    /// Will keep looping and checking the latest ledger info to see if there are new transactions
    /// If there are, it will set the highest known version
    async fn ensure_highest_known_version(&mut self) -> bool {
        let mut empty_loops = 0;
        let mut waits = [Duration::ZERO; 3];
        while self.highest_known_version == 0 || self.current_version > self.highest_known_version {
            if let Some(abort_handle) = self.abort_handle.as_ref() {
                if abort_handle.load(Ordering::SeqCst) {
                    return false;
                }
            }
            let poll_start = std::time::Instant::now();
            let wait_index = match self.wait_stage {
                "fullnode_wait_ledger" => 0,
                "fullnode_wait_table_info" => 1,
                _ => 2,
            };
            if empty_loops > 0 {
                tokio::time::sleep(Duration::from_millis(RETRY_TIME_MILLIS)).await;
            }
            empty_loops += 1;
            let result = self.set_highest_known_version();
            if empty_loops > 1 {
                waits[wait_index] += poll_start.elapsed();
            }
            if let Err(err) = result {
                self.wait_stage = "fullnode_wait_error";
                error!(
                    error = format!("{:?}", err),
                    "[Indexer Fullnode] Failed to set highest known version"
                );
                continue;
            } else {
                sample!(
                    SampleRate::Frequency(10),
                    info!(
                        highest_known_version = self.highest_known_version,
                        "[Indexer Fullnode] Found new highest known version",
                    )
                );
            }
        }
        for (stage, elapsed) in [
            "fullnode_wait_ledger",
            "fullnode_wait_table_info",
            "fullnode_wait_error",
        ]
        .into_iter()
        .zip(waits)
        {
            if !elapsed.is_zero() {
                observe_indexer_stage(stage, self.current_version, self.current_version, elapsed);
            }
        }
        true
    }
}

type ConvertedBatch = (Vec<StreamResponse>, std::time::Instant, u64, u64);

struct ConversionTasks(Vec<tokio::task::JoinHandle<ConvertedBatch>>);

impl Drop for ConversionTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            // Running blocking tasks finish normally; queued tasks can be cancelled.
            task.abort();
        }
    }
}

async fn send_ordered_batches(
    tasks: Vec<tokio::task::JoinHandle<ConvertedBatch>>,
    sender: &mpsc::Sender<Result<StreamResponse, Status>>,
    conversion_started: std::time::Instant,
) -> Result<Option<(Duration, Duration)>, Status> {
    let mut tasks = ConversionTasks(tasks);
    let mut conversion_duration = Duration::ZERO;
    let mut sending_duration = Duration::ZERO;
    for task in &mut tasks.0 {
        let result = tokio::select! {
            biased;
            _ = sender.closed() => return Ok(None),
            result = task => result,
        };
        let (responses, completed_at, first, last) = match result {
            Ok(batch) => batch,
            Err(err) => {
                let status =
                    Status::internal(format!("Transaction conversion task failed: {}", err));
                let _ = sender.send(Err(status.clone())).await;
                return Err(status);
            },
        };
        conversion_duration =
            conversion_duration.max(completed_at.duration_since(conversion_started));
        observe_indexer_stage(
            "conversion_ready_to_send",
            first,
            last,
            completed_at.elapsed(),
        );
        for response in responses {
            let started = std::time::Instant::now();
            let result = sender.send(Ok(response)).await;
            let elapsed = started.elapsed();
            sending_duration += elapsed;
            observe_indexer_stage("internal_send", first, last, elapsed);
            if result.is_err() {
                return Ok(None);
            }
        }
    }
    Ok(Some((conversion_duration, sending_duration)))
}

#[cfg(test)]
mod ordered_delivery_tests {
    use super::*;
    use tokio::sync::oneshot;

    fn batch(version: u64) -> ConvertedBatch {
        (
            vec![TransactionsFromNodeResponse {
                chain_id: version as u32,
                ..Default::default()
            }
            .into()],
            std::time::Instant::now(),
            version,
            version,
        )
    }

    #[tokio::test]
    async fn sends_first_batch_before_later_conversion_finishes() {
        let (release, wait) = oneshot::channel();
        let tasks = vec![
            tokio::spawn(async { batch(1) }),
            tokio::spawn(async {
                wait.await.unwrap();
                batch(2)
            }),
        ];
        let (tx, mut rx) = mpsc::channel(1);
        let worker = tokio::spawn(async move {
            send_ordered_batches(tasks, &tx, std::time::Instant::now()).await
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .response
                .chain_id,
            1
        );
        release.send(()).unwrap();
        assert_eq!(rx.recv().await.unwrap().unwrap().response.chain_id, 2);
        assert!(worker.await.unwrap().unwrap().is_some());
    }

    #[tokio::test]
    async fn keeps_order_when_later_task_finishes_first() {
        let (release, wait) = oneshot::channel();
        let later = tokio::spawn(async { batch(2) });
        let later_batch = later.await.unwrap();
        let tasks = vec![
            tokio::spawn(async {
                wait.await.unwrap();
                batch(1)
            }),
            tokio::spawn(async move { later_batch }),
        ];
        let (tx, mut rx) = mpsc::channel(1);
        let started = std::time::Instant::now() - Duration::from_secs(1);
        let worker = tokio::spawn(async move { send_ordered_batches(tasks, &tx, started).await });
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_err());
        release.send(()).unwrap();
        assert_eq!(rx.recv().await.unwrap().unwrap().response.chain_id, 1);
        assert_eq!(rx.recv().await.unwrap().unwrap().response.chain_id, 2);
        assert!(worker.await.unwrap().unwrap().is_some());
    }

    #[tokio::test]
    async fn reports_failure_after_delivered_prefix() {
        let tasks = vec![
            tokio::spawn(async { batch(1) }),
            tokio::spawn(async { panic!("conversion failed") }),
            tokio::spawn(async { batch(3) }),
        ];
        let (tx, mut rx) = mpsc::channel(4);
        let result = send_ordered_batches(tasks, &tx, std::time::Instant::now()).await;
        assert!(result.is_err());
        assert_eq!(rx.recv().await.unwrap().unwrap().response.chain_id, 1);
        assert_eq!(
            rx.recv().await.unwrap().unwrap_err().code(),
            tonic::Code::Internal
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn disconnect_does_not_wait_for_conversion() {
        let task = tokio::spawn(std::future::pending::<ConvertedBatch>());
        let abort = task.abort_handle();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            send_ordered_batches(vec![task], &tx, std::time::Instant::now()),
        )
        .await
        .unwrap();
        assert!(result.unwrap().is_none());
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
    }

    #[tokio::test]
    async fn preserves_chunks_and_empty_filtered_batches_under_backpressure() {
        let started = std::time::Instant::now();
        let tasks = vec![
            tokio::spawn(async { (vec![], std::time::Instant::now(), 0, 0) }),
            tokio::spawn(async {
                let mut first = batch(1);
                first.0.extend(batch(2).0);
                first.3 = 2;
                first
            }),
            tokio::spawn(async { batch(3) }),
        ];
        let (tx, mut rx) = mpsc::channel(1);
        let worker = tokio::spawn(async move { send_ordered_batches(tasks, &tx, started).await });
        for version in 1..=3 {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .response
                    .chain_id,
                version
            );
        }
        assert!(worker.await.unwrap().unwrap().is_some());
        assert!(rx.recv().await.is_none());
    }
}

fn responses_with_ranges(
    transactions: Vec<TransactionPB>,
    first_version: u64,
    last_version: u64,
    output_batch_size: usize,
    chain_id: u32,
    message_size_limit: usize,
) -> Vec<StreamResponse> {
    let mut next_version = first_version;
    let mut responses = vec![];
    for chunk in transactions.chunks(output_batch_size) {
        for chunk in chunk_transactions(chunk.to_vec(), message_size_limit) {
            let end = chunk.last().unwrap().version;
            responses.push(StreamResponse {
                response: TransactionsFromNodeResponse {
                    chain_id,
                    response: Some(transactions_from_node_response::Response::Data(
                        TransactionsOutput {
                            transactions: chunk,
                        },
                    )),
                },
                processed_range: Some(ProcessedRange {
                    first_version: next_version,
                    last_version: end,
                }),
            });
            next_version = end + 1;
        }
    }
    if let Some(last) = responses.last_mut() {
        last.processed_range.as_mut().unwrap().last_version = last_version;
    } else {
        responses.push(StreamResponse {
            response: TransactionsFromNodeResponse {
                chain_id,
                response: Some(transactions_from_node_response::Response::Data(
                    TransactionsOutput {
                        transactions: vec![],
                    },
                )),
            },
            processed_range: Some(ProcessedRange {
                first_version,
                last_version,
            }),
        });
    }
    responses
}

#[cfg(test)]
mod processed_range_tests {
    use super::*;

    fn transactions(versions: &[u64]) -> Vec<TransactionPB> {
        versions
            .iter()
            .map(|v| TransactionPB {
                version: *v,
                ..Default::default()
            })
            .collect()
    }
    fn ranges(responses: &[StreamResponse]) -> Vec<(u64, u64)> {
        responses
            .iter()
            .map(|r| {
                let r = r.processed_range.as_ref().unwrap();
                (r.first_version, r.last_version)
            })
            .collect()
    }
    #[test]
    fn filtered_final_chunk_includes_scanned_tail() {
        let responses = responses_with_ranges(
            transactions(&[102, 105, 108]),
            100,
            110,
            2,
            1,
            MESSAGE_SIZE_LIMIT,
        );
        assert_eq!(ranges(&responses), vec![(100, 105), (106, 110)]);
        let data: Vec<_> = responses
            .into_iter()
            .flat_map(|r| match r.response.response.unwrap() {
                transactions_from_node_response::Response::Data(d) => d
                    .transactions
                    .into_iter()
                    .map(|t| t.version)
                    .collect::<Vec<_>>(),
                _ => panic!("expected data"),
            })
            .collect();
        assert_eq!(data, vec![102, 105, 108]);
    }
    #[test]
    fn all_filtered_still_returns_original_range() {
        let responses = responses_with_ranges(vec![], 100, 110, 2, 1, MESSAGE_SIZE_LIMIT);
        assert_eq!(ranges(&responses), vec![(100, 110)]);
        match responses[0].response.response.as_ref().unwrap() {
            transactions_from_node_response::Response::Data(d) => {
                assert!(d.transactions.is_empty())
            },
            _ => panic!("expected data"),
        }
    }
    #[test]
    fn byte_splitting_preserves_original_scanned_range() {
        let responses = responses_with_ranges(transactions(&[102, 105, 108]), 100, 110, 100, 1, 3);
        assert!(responses.len() > 1);
        let ranges = ranges(&responses);
        assert_eq!(ranges.first().unwrap().0, 100);
        assert_eq!(ranges.last().unwrap().1, 110);
        assert!(ranges.windows(2).all(|w| w[0].1 + 1 == w[1].0));
    }
    #[test]
    fn adjacent_tasks_publish_independent_ranges() {
        let mut responses =
            responses_with_ranges(transactions(&[102]), 100, 105, 2, 1, MESSAGE_SIZE_LIMIT);
        responses.extend(responses_with_ranges(
            vec![],
            106,
            110,
            2,
            1,
            MESSAGE_SIZE_LIMIT,
        ));
        assert_eq!(ranges(&responses), vec![(100, 105), (106, 110)]);
    }
}
