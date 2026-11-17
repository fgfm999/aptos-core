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
struct MockProbe(Arc<Notify>);
#[tonic::async_trait]
impl ExecutionProbe for MockProbe {
    type SubscribeStream = Events<ProbeEvent>;

    async fn subscribe(
        &self,
        _: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let notify = self.0.clone();
        tx.send(Ok(ProbeEvent {
            ready: true,
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
struct MockIndexer(Arc<Notify>);
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
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx
                .send(Ok(TransactionsResponse {
                    chain_id: Some(4),
                    processed_range: None,
                    transactions: vec![Transaction {
                        version: 101,
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
    let p = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p_addr = p.local_addr().unwrap();
    let i = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let i_addr = i.local_addr().unwrap();
    let rest = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_addr = rest.local_addr().unwrap();
    let notify = Arc::new(Notify::new());
    let p_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ExecutionProbeServer::new(MockProbe(notify.clone())))
            .serve_with_incoming(TcpListenerStream::new(p)),
    );
    let i_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(RawDataServer::new(MockIndexer(notify)))
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
    assert_eq!(report["measurement_impaired"], false);
    let csv = std::fs::read_to_string(out.join("matches.csv")).unwrap();
    assert!(csv.contains(&hex::encode([9u8; 32])));
    assert!(report["p50_ms"].as_f64().unwrap().is_finite());
    std::fs::remove_dir_all(out).unwrap();
    p_task.abort();
    i_task.abort();
    rest_task.abort();
}
