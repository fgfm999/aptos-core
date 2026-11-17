// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use aptos_execution_probe::{
    decibel::{decode_order_event, decode_output, entry_function, is_place_order},
    wire::{
        execution_probe_server::{ExecutionProbe, ExecutionProbeServer},
        DataFormat, ExecutedTransactionData, ProbeEvent, SubscribeRequest, TransactionId,
        BCS_SCHEMA_VERSION,
    },
};
use aptos_types::{
    account_config::{ObjectCoreResource, ObjectGroupResource},
    state_store::state_key::StateKey,
    transaction::{Transaction, TransactionAuxiliaryData, TransactionOutput, TransactionStatus},
    write_set::{WriteOp, WriteSetMut},
};
use move_core_types::{account_address::AccountAddress, move_resource::MoveStructType};
use serde_json::Value;
use std::{pin::Pin, time::Duration};
use tokio_stream::{
    wrappers::{ReceiverStream, TcpListenerStream},
    Stream,
};
use tonic::{Request, Response, Status};

fn fixture() -> (Transaction, TransactionOutput, Value) {
    let aptos_api_types::TransactionData::OnChain(data) =
        bcs::from_bytes(include_bytes!("data/place-order-8094725931.bcs")).unwrap()
    else {
        panic!("committed fixture")
    };
    let expected: Value =
        serde_json::from_str(include_str!("data/place-order-8094725931.expected.json")).unwrap();
    assert_eq!(data.version.to_string(), expected["version"]);
    assert_eq!(
        data.transaction.committed_hash().to_hex_literal(),
        expected["hash"]
    );
    let output = TransactionOutput::new(
        data.changes,
        data.events,
        data.info.gas_used(),
        TransactionStatus::Keep(data.info.status().clone()),
        TransactionAuxiliaryData::None,
    );
    (data.transaction, output, expected)
}
fn assert_decoded(decoded: &Value, expected: &Value) {
    let events = decoded["events"].as_array().unwrap();
    assert!(!events.is_empty());
    assert_eq!(events.len(), expected["events"].as_array().unwrap().len());
    for (actual, expected) in events.iter().zip(expected["events"].as_array().unwrap()) {
        assert_eq!(actual["type"], expected["type"]);
        assert_eq!(actual["data"], expected["data"]);
    }
    let actual = decoded["write_resource"].as_array().unwrap();
    assert!(!actual.is_empty());
    assert_eq!(
        actual.len(),
        expected["write_resource"].as_array().unwrap().len()
    );
    for expected in expected["write_resource"].as_array().unwrap() {
        let actual = actual
            .iter()
            .find(|v| v["address"] == expected["address"])
            .unwrap();
        assert_eq!(actual["data"], expected["data"]);
    }
}
#[test]
fn real_bcs_event_and_resource_group_match_rest_json() {
    let (transaction, output, expected) = fixture();
    assert!(is_place_order(&transaction));
    assert_eq!(entry_function(&transaction).unwrap(), expected["function"]);
    assert_decoded(&decode_output(&output).unwrap(), &expected);
    // Also exercise the direct-resource representation, not just grouped writes.
    let core = ObjectCoreResource::struct_tag();
    let mut direct_tested = false;
    for (_, op) in output.write_set().write_op_iter() {
        let Some(bytes) = op.bytes() else {
            continue;
        };
        if let Ok(group) = bcs::from_bytes::<ObjectGroupResource>(bytes)
            && let Some(bytes) = group.group.get(&core)
        {
            let writes = WriteSetMut::new(vec![(
                StateKey::resource(&AccountAddress::ONE, &core).unwrap(),
                WriteOp::legacy_modification(bytes.clone().into()),
            )])
            .freeze()
            .unwrap();
            let direct = TransactionOutput::new_success_with_write_set(writes);
            assert_eq!(
                decode_output(&direct).unwrap()["write_resource"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            direct_tested = true;
            break;
        }
    }
    assert!(direct_tested);
}
#[test]
fn unknown_or_truncated_event_layout_fails() {
    assert!(decode_order_event(&[1]).is_err());
    let (_, output, _) = fixture();
    let event = output
        .events()
        .iter()
        .find(|e| {
            matches!(e.type_tag(), move_core_types::language_storage::TypeTag::Struct(tag) if tag.module.as_str() == "market_types" && tag.name.as_str() == "OrderEvent")
        })
        .unwrap();
    let bytes = event.event_data();
    assert!(decode_order_event(&bytes[..bytes.len() - 1]).is_err());
}

struct BcsProbe;
#[tonic::async_trait]
impl ExecutionProbe for BcsProbe {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<ProbeEvent, Status>> + Send>>;

    async fn subscribe(
        &self,
        _: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let (transaction, output, _) = fixture();
        let hash = transaction.committed_hash().to_vec();
        let ready = ProbeEvent {
            ready: true,
            include_data: true,
            session_id: "bcs-fixture".into(),
            chain_id: 1,
            data_format: DataFormat::Bcs as i32,
            bcs_schema_version: BCS_SCHEMA_VERSION,
            ..Default::default()
        };
        tx.send(Ok(ready.clone())).await.unwrap();
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ProbeEvent {
                    ready: false,
                    sequence: 1,
                    last_chunk: true,
                    transactions: vec![TransactionId {
                        hash: hash.clone(),
                        transaction_index: 0,
                    }],
                    transaction_data: vec![ExecutedTransactionData {
                        hash,
                        transaction_index: 0,
                        data_complete: true,
                        execution_output_bcs: bcs::to_bytes(&output).unwrap(),
                        transaction_bcs: bcs::to_bytes(&transaction).unwrap(),
                        ..Default::default()
                    }],
                    ..ready
                }))
                .await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_decoder_consumes_real_bcs_over_grpc() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ExecutionProbeServer::new(BcsProbe))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let result = tokio::process::Command::new(env!("CARGO_BIN_EXE_execution-probe-decode"))
        .args(["--probe", &format!("http://{addr}"), "--seconds", "1"])
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["committed"], false);
    assert_decoded(&report["decoded"], &fixture().2);
    server.abort();
}
