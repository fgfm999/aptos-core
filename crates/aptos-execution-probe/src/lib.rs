// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

pub mod compare;
pub mod decibel;
mod filter;
mod packets;
pub mod payload;
pub mod wire;

use anyhow::Result;
use aptos_crypto::HashValue;
use aptos_executor_types::execution_output::ExecutionOutput;
use aptos_metrics_core::{
    register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec,
};
use aptos_types::transaction::{Transaction, TransactionStatus};
use once_cell::sync::Lazy;
use std::{
    collections::HashMap,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::{
    wrappers::{ReceiverStream, TcpListenerStream},
    Stream,
};
use tonic::{Request, Response, Status};
use wire::{
    execution_probe_server::{ExecutionProbe, ExecutionProbeServer},
    ProbeEvent, SubscribeRequest, TransactionId,
};

static COUNTERS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "execution_probe_events_total",
        "Probe observations and loss",
        &["kind"]
    )
    .unwrap()
});
static DURATIONS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "execution_probe_duration_seconds",
        "Probe delivery preparation",
        &["stage"],
        vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 1.0]
    )
    .unwrap()
});

struct Observation {
    block: HashValue,
    parent: HashValue,
    output: ExecutionOutput,
    at: Instant,
    unix_nanos: u64,
    sequence: u64,
}
struct Hub {
    session: String,
    chain: u32,
    feeds: Mutex<HashMap<String, Weak<Feed>>>,
    active_feeds: AtomicUsize,
    dropped: AtomicU64,
    include_data: bool,
    data_format: wire::DataFormat,
    bcs_schema_version: u32,
    filter: Option<filter::RawFilter>,
    filter_json: String,
}
// Equal validated filters share matching and serialization. Weak registry entries
// disappear after the last subscriber/worker reference, so disconnects release feeds.
struct Feed {
    events: broadcast::Sender<ProbeEvent>,
    filter: Option<filter::RawFilter>,
    dropped: AtomicU64,
    hub: Weak<Hub>,
}
impl Drop for Feed {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.active_feeds.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
impl Hub {
    fn subscribe(
        self: &Arc<Self>,
        request: &str,
    ) -> Result<(Arc<Feed>, broadcast::Receiver<ProbeEvent>), Box<Status>> {
        let (requested, canonical) = if request.is_empty() {
            (None, String::new())
        } else {
            let (filter, canonical) = filter::RawFilter::compile_subscription(request)
                .map_err(|e| Box::new(Status::invalid_argument(format!("{e:#}"))))?;
            (Some(filter), canonical)
        };
        let mut feeds = self.feeds.lock().expect("probe feed registry");
        feeds.retain(|_, feed| feed.strong_count() > 0);
        if let Some(feed) = feeds.get(&canonical).and_then(Weak::upgrade) {
            let receiver = feed.events.subscribe();
            return Ok((feed, receiver));
        }
        if feeds.len() >= 8 {
            return Err(Box::new(Status::resource_exhausted(
                "at most 8 distinct probe subscription filters",
            )));
        }
        let filter = match (self.filter.clone(), requested) {
            (Some(startup), Some(requested)) => {
                Some(filter::RawFilter::And(vec![startup, requested]))
            },
            (startup, requested) => startup.or(requested),
        };
        let (events, receiver) = broadcast::channel(if canonical.is_empty() { 256 } else { 32 });
        let feed = Arc::new(Feed {
            events,
            filter,
            dropped: AtomicU64::new(0),
            hub: Arc::downgrade(self),
        });
        self.active_feeds.fetch_add(1, Ordering::Relaxed);
        feeds.insert(canonical, Arc::downgrade(&feed));
        Ok((feed, receiver))
    }

    fn live_feeds(&self) -> Vec<Arc<Feed>> {
        let mut feeds = self.feeds.lock().expect("probe feed registry");
        feeds.retain(|_, feed| feed.strong_count() > 0);
        feeds.values().filter_map(Weak::upgrade).collect()
    }
}

fn publish_observation(o: &Observation, hub: &Hub, context: Option<&payload::DataContext>) {
    for feed in hub.live_feeds() {
        match packets::build(o, hub, &feed, context) {
            Ok(events) => {
                for event in events {
                    COUNTERS
                        .with_label_values(&["transactions"])
                        .inc_by(event.transactions.len() as u64);
                    let _ = feed.events.send(event);
                }
                COUNTERS.with_label_values(&["blocks"]).inc();
            },
            Err(error) => {
                feed.dropped.fetch_add(1, Ordering::Relaxed);
                COUNTERS.with_label_values(&["dropped_blocks"]).inc();
                COUNTERS.with_label_values(&["payload_block_errors"]).inc();
                aptos_logger::warn!("Execution probe subscription payload rejected: {}", error);
            },
        }
    }
}

#[derive(Clone)]
pub struct Publisher {
    sender: mpsc::Sender<Observation>,
    hub: Arc<Hub>,
    sequence: Arc<AtomicU64>,
}
impl Publisher {
    pub fn interested(&self) -> bool {
        self.hub.active_feeds.load(Ordering::Relaxed) > 0
    }

    /// Called after execution succeeds. Never waits on the worker or a subscriber.
    pub fn publish(&self, block: HashValue, parent: HashValue, output: ExecutionOutput) {
        let at = Instant::now();
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        if !self.interested() {
            return;
        }
        let observation = Observation {
            block,
            parent,
            output,
            at,
            sequence,
            unix_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        };
        if self.sender.try_send(observation).is_err() {
            self.hub.dropped.fetch_add(1, Ordering::Relaxed);
            COUNTERS.with_label_values(&["dropped_blocks"]).inc();
        }
        DURATIONS
            .with_label_values(&["enqueue"])
            .observe(at.elapsed().as_secs_f64());
    }
}

pub fn start(address: SocketAddr, chain_id: u32) -> Result<Publisher> {
    start_configured(address, chain_id, None, None)
}

pub fn start_configured(
    address: SocketAddr,
    chain_id: u32,
    context: Option<payload::DataContext>,
    transaction_filter: Option<aptos_config::config::ExecutionProbeFilter>,
) -> Result<Publisher> {
    let listener = std::net::TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    aptos_logger::info!("Execution probe listening on {}", listener.local_addr()?);
    start_listener_with_data(listener, chain_id, context, transaction_filter)
}

#[cfg(test)]
fn start_listener(listener: std::net::TcpListener, chain_id: u32) -> Result<Publisher> {
    start_listener_with_data(listener, chain_id, None, None)
}

fn start_listener_with_data(
    listener: std::net::TcpListener,
    chain_id: u32,
    context: Option<payload::DataContext>,
    transaction_filter: Option<aptos_config::config::ExecutionProbeFilter>,
) -> Result<Publisher> {
    let filter = transaction_filter
        .as_ref()
        .map(filter::RawFilter::compile)
        .transpose()?;
    let filter_json = transaction_filter
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?
        .unwrap_or_default();
    anyhow::ensure!(
        filter_json.len() <= 16 * 1024,
        "probe filter metadata exceeds 16 KiB"
    );
    let hub = Arc::new(Hub {
        session: uuid::Uuid::new_v4().to_string(),
        chain: chain_id,
        feeds: Mutex::new(HashMap::new()),
        active_feeds: AtomicUsize::new(0),
        dropped: AtomicU64::new(0),
        include_data: context.is_some(),
        filter,
        filter_json,
        data_format: context
            .as_ref()
            .map_or(wire::DataFormat::Json, |c| c.wire_format()),
        bcs_schema_version: context.as_ref().map_or(0, |c| {
            if c.wire_format() == wire::DataFormat::Bcs {
                wire::BCS_SCHEMA_VERSION
            } else {
                0
            }
        }),
    });
    let (sender, mut receiver) = mpsc::channel::<Observation>(16);
    let publisher = Publisher {
        sender,
        hub: hub.clone(),
        sequence: Arc::new(AtomicU64::new(0)),
    };
    std::thread::Builder::new().name("execution-probe".into()).spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("probe runtime");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("probe listener");
            let worker_hub = hub.clone();
            let worker = async move {
                while let Some(o) = receiver.recv().await {
                    DURATIONS.with_label_values(&["queue"]).observe(o.at.elapsed().as_secs_f64());
                    let start = Instant::now();
                    let execution_at = o.at;
                    if let Some(context) = context.clone() {
                        let hub = worker_hub.clone();
                        if let Err(error) = tokio::task::spawn_blocking(move || publish_observation(&o,&hub,Some(&context))).await {
                            worker_hub.dropped.fetch_add(1,Ordering::Relaxed);
                            COUNTERS.with_label_values(&["dropped_blocks"]).inc();
                            aptos_logger::warn!("Execution probe worker failed: {}",error);
                        }
                    } else { publish_observation(&o,&worker_hub,None); }
                    DURATIONS.with_label_values(&["prepare_publish"]).observe(start.elapsed().as_secs_f64());
                    DURATIONS.with_label_values(&["execution_to_publish"]).observe(execution_at.elapsed().as_secs_f64());
                }
            };
            let server = tonic::transport::Server::builder().add_service(ExecutionProbeServer::new(Service(hub))).serve_with_incoming(TcpListenerStream::new(listener));
            tokio::select! { _ = worker => {}, result = server => { if let Err(e) = result { aptos_logger::error!("execution probe server failed: {}", e); } } }
        });
    })?;
    Ok(publisher)
}

fn successful_ids(
    output: &aptos_executor_types::transactions_with_output::TransactionsWithOutput,
    filter: Option<&filter::RawFilter>,
) -> Vec<TransactionId> {
    let mut examined = 0;
    let mut rejected = 0;
    let ids = output
        .transactions
        .iter()
        .zip(&output.transaction_outputs)
        .enumerate()
        .filter_map(|(index, (txn, result))| {
            if let Transaction::UserTransaction(user) = txn
                && matches!(result.status(), TransactionStatus::Keep(status) if status.is_success())
            {
                if let Some(filter) = filter {
                    examined += 1;
                    if !filter.matches(user, result) {
                        rejected += 1;
                        return None;
                    }
                }
                Some(TransactionId {
                    hash: txn.committed_hash().to_vec(),
                    transaction_index: index as u32,
                })
            } else {
                None
            }
        })
        .collect();
    if filter.is_some() {
        COUNTERS
            .with_label_values(&["filter_examined_transactions"])
            .inc_by(examined);
        COUNTERS
            .with_label_values(&["filtered_transactions"])
            .inc_by(rejected);
    }
    ids
}

struct Service(Arc<Hub>);
#[tonic::async_trait]
impl ExecutionProbe for Service {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<ProbeEvent, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner();
        let (feed, mut source) = self
            .0
            .subscribe(&request.transaction_filter_json)
            .map_err(|status| *status)?;
        let hub = self.0.clone();
        let (sender, receiver) = mpsc::channel(8);
        sender
            .send(Ok(ProbeEvent {
                ready: true,
                session_id: hub.session.clone(),
                chain_id: hub.chain,
                dropped_blocks: hub
                    .dropped
                    .load(Ordering::Relaxed)
                    .saturating_add(feed.dropped.load(Ordering::Relaxed)),
                include_data: hub.include_data,
                data_format: hub.data_format as i32,
                bcs_schema_version: hub.bcs_schema_version,
                transaction_filter_json: hub.filter_json.clone(),
                subscription_filter_json: request.transaction_filter_json,
                ..Default::default()
            }))
            .await
            .map_err(|_| Status::cancelled("subscriber closed"))?;
        tokio::spawn(async move {
            let _feed = feed;
            loop {
                let next =
                    tokio::select! { _ = sender.closed() => break, item = source.recv() => item };
                let item = match next {
                    Ok(event) => Ok(event),
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        COUNTERS.with_label_values(&["lagged_subscribers"]).inc();
                        let _ = sender
                            .send(Err(Status::resource_exhausted(
                                "execution probe subscriber lagged",
                            )))
                            .await;
                        break;
                    },
                };
                if sender.send(item).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use aptos_crypto::{
        ed25519::{Ed25519PrivateKey, Ed25519PublicKey},
        SigningKey,
    };
    use aptos_executor_types::transactions_with_output::TransactionsWithOutput;
    use aptos_types::{
        account_address::AccountAddress,
        chain_id::ChainId,
        transaction::{
            ExecutionStatus, RawTransaction, Script, SignedTransaction, TransactionAuxiliaryData,
            TransactionOutput, TransactionPayload,
        },
        write_set::WriteSet,
    };
    use tokio_stream::StreamExt;

    pub(super) fn user() -> Transaction {
        user_at(AccountAddress::ONE, vec![])
    }
    fn user_at(sender: AccountAddress, code: Vec<u8>) -> Transaction {
        let key = Ed25519PrivateKey::try_from(&[7u8; 32][..]).unwrap();
        let raw = RawTransaction::new(
            sender,
            1,
            TransactionPayload::Script(Script::new(code, vec![], vec![])),
            1000,
            1,
            1000,
            ChainId::test(),
        );
        let signature = key.sign(&raw).unwrap();
        Transaction::UserTransaction(SignedTransaction::new(
            raw,
            Ed25519PublicKey::from(&key),
            signature,
        ))
    }
    #[test]
    fn only_successful_user_outputs_and_exact_hash() {
        let tx = user();
        let failure = TransactionOutput::new(
            WriteSet::default(),
            vec![],
            0,
            TransactionStatus::Keep(ExecutionStatus::OutOfGas),
            TransactionAuxiliaryData::None,
        );
        let retry = TransactionOutput::new(
            WriteSet::default(),
            vec![],
            0,
            TransactionStatus::Retry,
            TransactionAuxiliaryData::None,
        );
        let output = TransactionsWithOutput {
            transactions: vec![
                tx.clone(),
                tx.clone(),
                tx.clone(),
                Transaction::StateCheckpoint(HashValue::zero()),
            ],
            transaction_outputs: vec![
                TransactionOutput::new_empty_success(),
                failure,
                retry,
                TransactionOutput::new_empty_success(),
            ],
            persisted_auxiliary_infos: vec![],
        };
        let ids = successful_ids(&output, None);
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].hash, tx.committed_hash().to_vec());
        assert_eq!(ids[0].transaction_index, 0);
    }
    #[test]
    fn full_queue_drops_without_waiting() {
        let hub = Arc::new(Hub {
            session: "test".into(),
            chain: 4,
            feeds: Mutex::new(HashMap::new()),
            active_feeds: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            include_data: false,
            data_format: wire::DataFormat::Json,
            bcs_schema_version: 0,
            filter: None,
            filter_json: String::new(),
        });
        let (_feed, _sub) = hub.subscribe("").unwrap();
        let (sender, _receiver) = mpsc::channel(16);
        let permits: Vec<_> = (0..16).map(|_| sender.try_reserve().unwrap()).collect();
        let publisher = Publisher {
            sender: sender.clone(),
            hub: hub.clone(),
            sequence: Arc::new(AtomicU64::new(0)),
        };
        publisher.publish(
            HashValue::zero(),
            HashValue::zero(),
            ExecutionOutput::new_dummy_with_input_txns(vec![]),
        );
        assert_eq!(hub.dropped.load(Ordering::Relaxed), 1);
        drop(permits);
    }
    #[tokio::test]
    async fn lag_is_an_error_not_silent_loss() {
        let hub = Arc::new(Hub {
            session: "test".into(),
            chain: 4,
            feeds: Mutex::new(HashMap::new()),
            active_feeds: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            include_data: false,
            data_format: wire::DataFormat::Json,
            bcs_schema_version: 0,
            filter: None,
            filter_json: String::new(),
        });
        let mut stream = Service(hub.clone())
            .subscribe(Request::new(SubscribeRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(stream.next().await.unwrap().unwrap().ready);
        for _ in 0..300 {
            hub.live_feeds()[0]
                .events
                .send(ProbeEvent::default())
                .unwrap();
        }
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    }
    #[tokio::test]
    async fn actual_grpc_ready_hashes_and_chunking() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let publisher = start_listener(listener, 4).unwrap();
        let mut client = wire::execution_probe_client::ExecutionProbeClient::connect(format!(
            "http://{address}"
        ))
        .await
        .unwrap();
        let mut stream = client
            .subscribe(SubscribeRequest::default())
            .await
            .unwrap()
            .into_inner();
        let ready = stream.message().await.unwrap().unwrap();
        assert!(ready.ready);
        assert_eq!(ready.chain_id, 4);
        let tx = user();
        publisher.publish(
            HashValue::zero(),
            HashValue::zero(),
            ExecutionOutput::new_dummy_with_input_txns(vec![tx.clone(); 1001]),
        );
        let first = stream.message().await.unwrap().unwrap();
        let last = stream.message().await.unwrap().unwrap();
        assert_eq!(first.transactions.len(), 1000);
        assert_eq!(last.transactions.len(), 1);
        assert!(!first.last_chunk);
        assert!(last.last_chunk);
        assert_eq!(last.chunk_index, 1);
        assert_eq!(first.sequence, last.sequence);
        assert_eq!(first.transactions[0].hash, tx.committed_hash().to_vec());
        publisher.publish(
            HashValue::zero(),
            HashValue::zero(),
            ExecutionOutput::new_dummy_with_input_txns(vec![]),
        );
        let empty = stream.message().await.unwrap().unwrap();
        assert!(empty.transactions.is_empty());
        assert!(empty.last_chunk);
    }
    #[tokio::test]
    async fn actual_grpc_data_mode_carries_typed_output() {
        grpc_data_mode(aptos_config::config::ExecutionProbeDataFormat::Json).await;
    }
    #[tokio::test]
    async fn actual_grpc_bcs_mode_never_reads_database() {
        grpc_data_mode(aptos_config::config::ExecutionProbeDataFormat::Bcs).await;
    }
    async fn grpc_data_mode(format: aptos_config::config::ExecutionProbeDataFormat) {
        struct NoReadDb;
        impl aptos_storage_interface::DbReader for NoReadDb {}
        struct OutputDb(aptos_storage_interface::state_store::state::State);
        impl aptos_storage_interface::DbReader for OutputDb {
            fn get_persisted_state(&self) -> aptos_storage_interface::Result<(Arc<dyn aptos_storage_interface::state_store::state_view::hot_state_view::HotStateView>,aptos_storage_interface::state_store::state::State)>{
                Ok((Arc::new(aptos_storage_interface::state_store::state_view::hot_state_view::EmptyHotState),self.0.clone()))
            }
        }
        let output = ExecutionOutput::new_dummy_with_input_txns(vec![user()]);
        let db = Arc::new(OutputDb(output.result_state.latest().clone()));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let publisher = start_listener_with_data(
            listener,
            4,
            Some(payload::DataContext {
                db: if format == aptos_config::config::ExecutionProbeDataFormat::Bcs {
                    Arc::new(NoReadDb)
                } else {
                    db
                },
                indexer_reader: None,
                format,
            }),
            None,
        )
        .unwrap();
        let mut client = wire::execution_probe_client::ExecutionProbeClient::connect(format!(
            "http://{address}"
        ))
        .await
        .unwrap();
        let mut stream = client
            .subscribe(SubscribeRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert!(stream.message().await.unwrap().unwrap().include_data);
        publisher.publish(HashValue::zero(), HashValue::zero(), output);
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(event.include_data);
        if format == aptos_config::config::ExecutionProbeDataFormat::Bcs {
            assert_eq!(event.data_format, wire::DataFormat::Bcs as i32);
            assert_eq!(event.bcs_schema_version, wire::BCS_SCHEMA_VERSION);
            let data = &event.transaction_data[0];
            let transaction: Transaction = bcs::from_bytes(&data.transaction_bcs).unwrap();
            assert_eq!(transaction.committed_hash().to_vec(), data.hash);
            let output: TransactionOutput = bcs::from_bytes(&data.execution_output_bcs).unwrap();
            assert_eq!(
                output.status(),
                &TransactionStatus::Keep(ExecutionStatus::Success)
            );
            assert!(data.events.is_empty() && data.write_set_changes.is_empty());
        }
        assert_eq!(event.transaction_data.len(), 1);
        assert_eq!(event.transaction_data[0].hash, event.transactions[0].hash);
        assert!(
            event.transaction_data[0].data_complete,
            "{}",
            event.transaction_data[0].conversion_error
        );
    }
    #[tokio::test]
    async fn grpc_filter_skips_oversized_rejections_and_preserves_empty_block_progress() {
        use aptos_config::config::{ExecutionProbeDataFormat, ExecutionProbeFilter};
        struct NoReadDb;
        impl aptos_storage_interface::DbReader for NoReadDb {}
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let filter = ExecutionProbeFilter::Sender {
            address: "0x1".into(),
        };
        let publisher = start_listener_with_data(
            listener,
            4,
            Some(payload::DataContext {
                db: Arc::new(NoReadDb),
                indexer_reader: None,
                format: ExecutionProbeDataFormat::Bcs,
            }),
            Some(filter.clone()),
        )
        .unwrap();
        let mut client =
            wire::execution_probe_client::ExecutionProbeClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
        let mut stream = client
            .subscribe(SubscribeRequest::default())
            .await
            .unwrap()
            .into_inner();
        let ready = stream.message().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<ExecutionProbeFilter>(&ready.transaction_filter_json).unwrap(),
            filter
        );
        let kept = user();
        let rejected = user_at(
            AccountAddress::from_hex_literal("0x2").unwrap(),
            vec![1; 3 * 1024 * 1024],
        );
        for (i, txns) in [
            vec![
                rejected.clone(),
                kept.clone(),
                rejected.clone(),
                kept.clone(),
            ],
            vec![rejected],
            vec![kept.clone()],
        ]
        .into_iter()
        .enumerate()
        {
            publisher.publish(
                HashValue::zero(),
                HashValue::zero(),
                ExecutionOutput::new_dummy_with_input_txns(txns),
            );
            let frame = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(frame.sequence, i as u64 + 1);
            assert_eq!(frame.chunk_index, 0);
            assert!(frame.last_chunk);
            assert_eq!(frame.dropped_blocks, 0);
            assert!(frame.transaction_filter_json.is_empty());
            let indices = match i {
                0 => vec![1, 3],
                1 => vec![],
                2 => vec![0],
                _ => unreachable!(),
            };
            assert_eq!(
                frame
                    .transactions
                    .iter()
                    .map(|t| t.transaction_index)
                    .collect::<Vec<_>>(),
                indices
            );
            assert_eq!(frame.transaction_data.len(), indices.len());
            for (id, data) in frame.transactions.iter().zip(&frame.transaction_data) {
                assert!(data.data_complete);
                assert_eq!(data.hash, id.hash);
                assert_eq!(data.transaction_index, id.transaction_index);
                assert_eq!(id.hash, kept.committed_hash().to_vec());
                let tx: Transaction = bcs::from_bytes(&data.transaction_bcs).unwrap();
                assert_eq!(tx, kept);
            }
        }
    }

    #[tokio::test]
    async fn identity_filter_keeps_original_indices() {
        use aptos_config::config::ExecutionProbeFilter;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let publisher = start_listener_with_data(
            listener,
            4,
            None,
            Some(ExecutionProbeFilter::Sender {
                address: "0x1".into(),
            }),
        )
        .unwrap();
        let mut client =
            wire::execution_probe_client::ExecutionProbeClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
        let mut stream = client
            .subscribe(SubscribeRequest::default())
            .await
            .unwrap()
            .into_inner();
        let ready = stream.message().await.unwrap().unwrap();
        assert!(!ready.include_data && !ready.transaction_filter_json.is_empty());
        publisher.publish(
            HashValue::zero(),
            HashValue::zero(),
            ExecutionOutput::new_dummy_with_input_txns(vec![
                user_at(AccountAddress::from_hex_literal("0x2").unwrap(), vec![]),
                user(),
            ]),
        );
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(frame.transactions.len(), 1);
        assert_eq!(frame.transactions[0].transaction_index, 1);
        assert!(frame.transaction_data.is_empty());
    }
    #[tokio::test]
    async fn fully_filtered_json_block_never_reads_state() {
        use aptos_config::config::{ExecutionProbeDataFormat, ExecutionProbeFilter};
        struct NoReadDb;
        impl aptos_storage_interface::DbReader for NoReadDb {}
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let publisher = start_listener_with_data(
            listener,
            4,
            Some(payload::DataContext {
                db: Arc::new(NoReadDb),
                indexer_reader: None,
                format: ExecutionProbeDataFormat::Json,
            }),
            Some(ExecutionProbeFilter::Sender {
                address: "0x2".into(),
            }),
        )
        .unwrap();
        let mut client =
            wire::execution_probe_client::ExecutionProbeClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
        let mut stream = client
            .subscribe(SubscribeRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert!(stream.message().await.unwrap().unwrap().ready);
        publisher.publish(
            HashValue::zero(),
            HashValue::zero(),
            ExecutionOutput::new_dummy_with_input_txns(vec![user()]),
        );
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            frame.last_chunk && frame.transactions.is_empty() && frame.transaction_data.is_empty()
        );
        assert_eq!(frame.dropped_blocks, 0);
    }

    #[test]
    fn scripts_do_not_match_entry_filters_and_failure_is_never_selected() {
        use aptos_config::config::ExecutionProbeFilter;
        let config = ExecutionProbeFilter::Not {
            filter: Box::new(ExecutionProbeFilter::EntryFunction {
                address: Some(crate::decibel::DECIBEL.into()),
                module: None,
                function: None,
            }),
        };
        let filter = filter::RawFilter::compile(&config).unwrap();
        let txn = user();
        let output = TransactionOutput::new_empty_success();
        assert!(filter.matches(txn.try_as_signed_user_txn().unwrap(), &output));
        let failed = TransactionOutput::new(
            Default::default(),
            vec![],
            0,
            TransactionStatus::Keep(ExecutionStatus::OutOfGas),
            TransactionAuxiliaryData::None,
        );
        let txns = TransactionsWithOutput {
            transactions: vec![txn.clone(), txn],
            transaction_outputs: vec![failed, output],
            persisted_auxiliary_infos: vec![],
        };
        let selected = successful_ids(&txns, Some(&filter));
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].transaction_index, 1);
    }
}

#[cfg(test)]
mod subscription_tests;
