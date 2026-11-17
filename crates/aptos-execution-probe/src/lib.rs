// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

pub mod compare;
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
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
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
    events: broadcast::Sender<ProbeEvent>,
    dropped: AtomicU64,
}
#[derive(Clone)]
pub struct Publisher {
    sender: mpsc::Sender<Observation>,
    hub: Arc<Hub>,
    sequence: Arc<AtomicU64>,
}
impl Publisher {
    pub fn interested(&self) -> bool {
        self.hub.events.receiver_count() > 0
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
    let listener = std::net::TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    aptos_logger::info!("Execution probe listening on {}", listener.local_addr()?);
    start_listener(listener, chain_id)
}

fn start_listener(listener: std::net::TcpListener, chain_id: u32) -> Result<Publisher> {
    let (events, _) = broadcast::channel(256);
    let hub = Arc::new(Hub {
        session: uuid::Uuid::new_v4().to_string(),
        chain: chain_id,
        events,
        dropped: AtomicU64::new(0),
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
                    let ids = successful_ids(&o.output.to_commit);
                    COUNTERS.with_label_values(&["transactions"]).inc_by(ids.len() as u64);
                    let chunks = ids.len().div_ceil(1000).max(1);
                    let mut ids = ids.into_iter();
                    for index in 0..chunks {
                        let event = ProbeEvent { ready: false, session_id: worker_hub.session.clone(), chain_id, sequence: o.sequence,
                            block_id: o.block.to_vec(), parent_block_id: o.parent.to_vec(), execution_done_unix_nanos: o.unix_nanos,
                            chunk_index: index as u32, last_chunk: index + 1 == chunks, transactions: ids.by_ref().take(1000).collect(),
                            dropped_blocks: worker_hub.dropped.load(Ordering::Relaxed) };
                        let _ = worker_hub.events.send(event);
                    }
                    COUNTERS.with_label_values(&["blocks"]).inc();
                    DURATIONS.with_label_values(&["prepare_publish"]).observe(start.elapsed().as_secs_f64());
                    DURATIONS.with_label_values(&["execution_to_publish"]).observe(o.at.elapsed().as_secs_f64());
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
) -> Vec<TransactionId> {
    output
        .transactions
        .iter()
        .zip(&output.transaction_outputs)
        .enumerate()
        .filter_map(|(index, (txn, result))| {
            if matches!(txn, Transaction::UserTransaction(_))
                && matches!(result.status(), TransactionStatus::Keep(status) if status.is_success())
            {
                Some(TransactionId {
                    hash: txn.committed_hash().to_vec(),
                    transaction_index: index as u32,
                })
            } else {
                None
            }
        })
        .collect()
}

struct Service(Arc<Hub>);
#[tonic::async_trait]
impl ExecutionProbe for Service {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<ProbeEvent, Status>> + Send>>;

    async fn subscribe(
        &self,
        _: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut source = self.0.events.subscribe();
        let hub = self.0.clone();
        let (sender, receiver) = mpsc::channel(8);
        sender
            .send(Ok(ProbeEvent {
                ready: true,
                session_id: hub.session.clone(),
                chain_id: hub.chain,
                dropped_blocks: hub.dropped.load(Ordering::Relaxed),
                ..Default::default()
            }))
            .await
            .map_err(|_| Status::cancelled("subscriber closed"))?;
        tokio::spawn(async move {
            loop {
                let item = match source.recv().await {
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

    fn user() -> Transaction {
        let key = Ed25519PrivateKey::try_from(&[7u8; 32][..]).unwrap();
        let raw = RawTransaction::new(
            AccountAddress::ONE,
            1,
            TransactionPayload::Script(Script::new(vec![], vec![], vec![])),
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
        let ids = successful_ids(&output);
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].hash, tx.committed_hash().to_vec());
        assert_eq!(ids[0].transaction_index, 0);
    }
    #[test]
    fn full_queue_drops_without_waiting() {
        let (events, _) = broadcast::channel(256);
        let hub = Arc::new(Hub {
            session: "test".into(),
            chain: 4,
            events,
            dropped: AtomicU64::new(0),
        });
        let _sub = hub.events.subscribe();
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
        let (events, _) = broadcast::channel(256);
        let hub = Arc::new(Hub {
            session: "test".into(),
            chain: 4,
            events,
            dropped: AtomicU64::new(0),
        });
        let mut stream = Service(hub.clone())
            .subscribe(Request::new(SubscribeRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(stream.next().await.unwrap().unwrap().ready);
        for _ in 0..300 {
            hub.events.send(ProbeEvent::default()).unwrap();
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
            .subscribe(SubscribeRequest {})
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
}
