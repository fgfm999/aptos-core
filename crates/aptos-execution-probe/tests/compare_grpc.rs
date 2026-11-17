// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use aptos_execution_probe::wire::{
    execution_probe_server::{ExecutionProbe, ExecutionProbeServer},
    ProbeEvent, SubscribeRequest, TransactionId,
};
use aptos_protos::{
    indexer::v1::{
        raw_data_server::{RawData, RawDataServer},
        GetTransactionsRequest, TransactionsResponse,
    },
    transaction::v1::{Transaction, TransactionInfo},
};
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, Notify},
};
use tokio_stream::{
    wrappers::{ReceiverStream, TcpListenerStream},
    Stream,
};
use tonic::{Request, Response, Status};
type Events<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
struct MockProbe(Arc<Notify>, bool);
#[tonic::async_trait]
impl ExecutionProbe for MockProbe {
    type SubscribeStream = Events<ProbeEvent>;

    async fn subscribe(
        &self,
        _: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let notify = self.0.clone();
        let bcs = self.1;
        tx.send(Ok(ProbeEvent {
            ready: true,
            include_data: true,
            data_format: i32::from(bcs),
            bcs_schema_version: u32::from(bcs),
            transaction_filter_json: if bcs {
                r#"{"type":"sender","address":"0x1"}"#.into()
            } else {
                String::new()
            },
            session_id: "fixture".into(),
            chain_id: 4,
            ..Default::default()
        }))
        .await
        .unwrap();
        tokio::spawn(async move {
            notify.notified().await;
            let _ = tx
                .send(Ok(ProbeEvent {
                    session_id: "fixture".into(),
                    chain_id: 4,
                    sequence: 1,
                    include_data: true,
                    data_format: i32::from(bcs),
                    bcs_schema_version: u32::from(bcs),
                    transaction_data: vec![aptos_execution_probe::wire::ExecutedTransactionData {
                        hash: vec![9; 32],
                        transaction_index: 0,
                        data_complete: true,
                        execution_output_bcs: if bcs { vec![1, 2, 3] } else { vec![] },
                        transaction_bcs: if bcs { vec![4, 5] } else { vec![] },
                        events: vec![aptos_protos::transaction::v1::Event::default()],
                        write_set_changes: vec![
                            aptos_protos::transaction::v1::WriteSetChange::default(),
                        ],
                        ..Default::default()
                    }],
                    last_chunk: true,
                    block_id: vec![8; 32],
                    transactions: vec![TransactionId {
                        hash: vec![9; 32],
                        transaction_index: 0,
                    }],
                    ..Default::default()
                }))
                .await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
struct MockIndexer(
    Arc<Notify>,
    Option<aptos_protos::util::timestamp::Timestamp>,
);
#[tonic::async_trait]
impl RawData for MockIndexer {
    type GetTransactionsStream = Events<TransactionsResponse>;

    async fn get_transactions(
        &self,
        r: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        assert_eq!(r.into_inner().starting_version, Some(101));
        let (tx, rx) = mpsc::channel(4);
        self.0.notify_one();
        let timestamp = self.1;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx
                .send(Ok(TransactionsResponse {
                    chain_id: Some(4),
                    processed_range: None,
                    transactions: vec![Transaction {
                        version: 101,
                        timestamp,
                        r#type: 4,
                        info: Some(TransactionInfo {
                            hash: vec![9; 32],
                            success: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                }))
                .await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn comparison_binary_consumes_both_grpc_streams() {
    compare_mode(false, Some(timestamp(-1))).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn comparison_binary_consumes_bcs_data() {
    compare_mode(true, Some(timestamp(-1))).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn comparison_keeps_delta_when_timestamp_is_missing() {
    compare_mode(false, None).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn comparison_rejects_invalid_timestamp_without_fabricating_age() {
    let mut ts = timestamp(-1);
    ts.nanos = 1_000_000_000;
    compare_mode(false, Some(ts)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn comparison_reports_negative_ages_as_clock_anomalies() {
    compare_mode(false, Some(timestamp(86_400))).await;
}
fn timestamp(offset_seconds: i64) -> aptos_protos::util::timestamp::Timestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    aptos_protos::util::timestamp::Timestamp {
        seconds: now.as_secs() as i64 + offset_seconds,
        nanos: now.subsec_nanos() as i32,
    }
}
async fn compare_mode(bcs: bool, timestamp: Option<aptos_protos::util::timestamp::Timestamp>) {
    let p = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p_addr = p.local_addr().unwrap();
    let i = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let i_addr = i.local_addr().unwrap();
    let rest = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_addr = rest.local_addr().unwrap();
    let notify = Arc::new(Notify::new());
    let p_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ExecutionProbeServer::new(MockProbe(notify.clone(), bcs)))
            .serve_with_incoming(TcpListenerStream::new(p)),
    );
    let i_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RawDataServer::new(MockIndexer(notify, timestamp)))
            .serve_with_incoming(TcpListenerStream::new(i)),
    );
    let rest_task = tokio::spawn(async move {
        let (mut socket, _) = rest.accept().await.unwrap();
        let mut buffer = [0u8; 2048];
        let _ = socket.read(&mut buffer).await.unwrap();
        let body = r#"{"chain_id":4,"ledger_version":"100","git_hash":"fixture"}"#;
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
    });
    let out = std::env::temp_dir().join(format!("probe-test-{}", uuid::Uuid::new_v4()));
    let result = tokio::process::Command::new(env!("CARGO_BIN_EXE_execution-probe-compare"))
        .args([
            "--probe",
            &format!("http://{p_addr}"),
            "--indexer",
            &format!("http://{i_addr}"),
            "--rest",
            &format!("http://{rest_addr}"),
            "--out",
            out.to_str().unwrap(),
            "--seconds",
            "1",
            "--warmup-seconds",
            "0",
            "--drain-seconds",
            "0",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("summary.json")).unwrap()).unwrap();
    assert_eq!(report["samples"], 1);
    assert_eq!(report["include_data"], true);
    assert_eq!(
        report["indexer_only_may_include_filtered_transactions"],
        bcs
    );
    if bcs {
        assert_eq!(report["probe_transaction_filter"]["type"], "sender");
    } else {
        assert!(report["probe_transaction_filter"].is_null());
    }
    assert_eq!(report["data_format"], if bcs { "bcs" } else { "json" });
    assert_eq!(report["bcs_bytes_received"], if bcs { 5 } else { 0 });
    assert_eq!(report["data_transactions"], 1);
    assert_eq!(report["events_received"], 1);
    assert_eq!(report["write_set_changes_received"], 1);
    assert_eq!(report["data_incomplete"], false);
    assert_eq!(report["measurement_impaired"], false);
    let csv = std::fs::read_to_string(out.join("matches.csv")).unwrap();
    assert!(csv.contains(&hex::encode([9u8; 32])));
    let mut lines = csv.lines();
    let headers: Vec<_> = lines.next().unwrap().split(',').collect();
    let row: std::collections::HashMap<_, _> = headers
        .into_iter()
        .zip(lines.next().unwrap().split(','))
        .collect();
    let receipt = |key| row[key].parse::<i64>().unwrap();
    let valid = timestamp
        .and_then(|t| aptos_execution_probe::compare::timestamp_unix_micros(t.seconds, t.nanos));
    assert_eq!(
        report["transaction_time"]["samples"],
        u64::from(valid.is_some())
    );
    assert_eq!(
        report["transaction_time"]["missing_or_invalid_timestamp_samples"],
        u64::from(valid.is_none())
    );
    assert_eq!(
        report["transaction_time"]["clock_synchronization_required"],
        true
    );
    if let Some(ts) = valid {
        assert_eq!(receipt("txn_timestamp_unix_micros"), ts);
        let mut negative = false;
        for (age, wall) in [
            ("probe_age_ms", "probe_received_unix_micros"),
            ("indexer_age_ms", "indexer_received_unix_micros"),
        ] {
            let actual = row[age].parse::<f64>().unwrap();
            let expected = (receipt(wall) - ts) as f64 / 1000.0;
            assert!((actual - expected).abs() < 0.000001);
            assert!(
                (report["transaction_time"][age]["p50_ms"].as_f64().unwrap() - actual).abs()
                    < 0.000001
            );
            assert_eq!(
                report["transaction_time"][age]["negative_samples"],
                u64::from(actual < 0.0)
            );
            negative |= actual < 0.0;
        }
        assert_eq!(report["transaction_time"]["measurement_impaired"], negative);
    } else {
        assert_eq!(row["txn_timestamp_unix_micros"], "");
        for age in ["probe_age_ms", "indexer_age_ms"] {
            assert_eq!(row[age], "");
            assert_eq!(report["transaction_time"][age]["samples"], 0);
            assert!(report["transaction_time"][age]["p95_ms"].is_null());
        }
        assert_eq!(report["transaction_time"]["measurement_impaired"], true);
    }
    assert!(report["p50_ms"].as_f64().unwrap().is_finite());
    std::fs::remove_dir_all(out).unwrap();
    p_task.abort();
    i_task.abort();
    rest_task.abort();
}
