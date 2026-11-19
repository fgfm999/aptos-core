use super::*;
use aptos_config::config::{ExecutionProbeDataFormat, ExecutionProbeFilter};
use aptos_crypto::{
    ed25519::{Ed25519PrivateKey, Ed25519PublicKey},
    SigningKey,
};
use aptos_types::{
    account_address::AccountAddress,
    chain_id::ChainId,
    transaction::{EntryFunction, RawTransaction, SignedTransaction, TransactionPayload},
};
use move_core_types::{identifier::Identifier, language_storage::ModuleId};
use serde_json::{json, Value};

fn entry(address: &str, nonce: u64, bytes: usize) -> Transaction {
    let key = Ed25519PrivateKey::try_from(&[7u8; 32][..]).unwrap();
    let raw = RawTransaction::new(
        AccountAddress::ONE,
        nonce,
        TransactionPayload::EntryFunction(EntryFunction::new(
            ModuleId::new(address.parse().unwrap(), Identifier::new("pool").unwrap()),
            Identifier::new("swap").unwrap(),
            vec![],
            vec![vec![0; bytes]],
        )),
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
fn exclude_decibel() -> Value {
    json!({"and":[{"type":"TransactionRootFilter","success":true,"txn_type":4},
        {"not":{"type":"UserTransactionFilter","payload":{"function":{"address":crate::decibel::DECIBEL}}}}]})
}
fn server(startup: Option<ExecutionProbeFilter>) -> (Publisher, String) {
    struct NoReadDb;
    impl aptos_storage_interface::DbReader for NoReadDb {}
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let publisher = start_listener_with_data(
        listener,
        4,
        Some(payload::DataContext {
            db: Arc::new(NoReadDb),
            indexer_reader: None,
            format: ExecutionProbeDataFormat::Bcs,
        }),
        startup,
    )
    .unwrap();
    (publisher, format!("http://{address}"))
}
async fn subscribe(url: &str, filter: Option<&Value>) -> tonic::Streaming<ProbeEvent> {
    let mut client = wire::execution_probe_client::ExecutionProbeClient::connect(url.to_owned())
        .await
        .unwrap();
    let mut stream = client
        .subscribe(SubscribeRequest {
            transaction_filter_json: filter.map(ToString::to_string).unwrap_or_default(),
        })
        .await
        .unwrap()
        .into_inner();
    let ready = stream.message().await.unwrap().unwrap();
    assert!(ready.ready);
    let acknowledged = if ready.subscription_filter_json.is_empty() {
        None
    } else {
        Some(serde_json::from_str::<Value>(&ready.subscription_filter_json).unwrap())
    };
    assert_eq!(acknowledged.as_ref(), filter);
    stream
}
async fn next(stream: &mut tonic::Streaming<ProbeEvent>) -> ProbeEvent {
    tokio::time::timeout(std::time::Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
fn publish(publisher: &Publisher, sequence: u8, transactions: Vec<Transaction>) {
    publisher.publish(
        HashValue::new([sequence; 32]),
        HashValue::new([sequence - 1; 32]),
        ExecutionOutput::new_dummy_with_input_txns(transactions),
    );
}
fn indices(event: &ProbeEvent) -> Vec<u32> {
    event
        .transactions
        .iter()
        .map(|t| t.transaction_index)
        .collect()
}

#[tokio::test]
async fn grpc_subscription_filters_before_bcs_and_isolates_other_feed_failures() {
    let (publisher, url) = server(None);
    let filter = exclude_decibel();
    let mut selected = subscribe(&url, Some(&filter)).await;
    let mut duplicate = subscribe(&url, Some(&filter)).await;
    let decibel = json!({"type":"UserTransactionFilter","payload":{"function":{"address":crate::decibel::DECIBEL}}});
    let mut other = subscribe(&url, Some(&decibel)).await;
    let kept = entry("0x1", 1, 0);
    // This transaction exceeds the wire limit. Its native fields must reject it
    // before BCS serialization on the selected feed; only the other feed loses it.
    publish(&publisher, 1, vec![
        entry(crate::decibel::DECIBEL, 2, 3 * 1024 * 1024),
        kept.clone(),
    ]);
    for stream in [&mut selected, &mut duplicate] {
        let event = next(stream).await;
        assert_eq!(event.sequence, 1);
        assert_eq!(indices(&event), vec![1]);
        assert_eq!(event.dropped_blocks, 0);
        assert_eq!(event.transaction_data.len(), 1);
        assert!(event.last_chunk);
        let decoded: Transaction =
            bcs::from_bytes(&event.transaction_data[0].transaction_bcs).unwrap();
        assert_eq!(decoded.committed_hash(), kept.committed_hash());
    }
    publish(&publisher, 2, vec![
        entry(crate::decibel::DECIBEL, 3, 0),
        entry("0x1", 4, 0),
    ]);
    for stream in [&mut selected, &mut duplicate] {
        let event = next(stream).await;
        assert_eq!(event.sequence, 2);
        assert_eq!(indices(&event), vec![1]);
        assert_eq!(event.dropped_blocks, 0);
    }
    let event = next(&mut other).await;
    assert_eq!(event.sequence, 2);
    assert_eq!(event.dropped_blocks, 1);
    assert_eq!(indices(&event), vec![0]);
    publish(&publisher, 3, vec![entry(crate::decibel::DECIBEL, 5, 0)]);
    let empty = next(&mut selected).await;
    assert_eq!(empty.sequence, 3);
    assert!(empty.last_chunk && empty.transactions.is_empty() && empty.transaction_data.is_empty());
    assert_eq!(empty.block_id, vec![3; 32]);
    assert_eq!(empty.parent_block_id, vec![2; 32]);
}

#[tokio::test]
async fn subscription_cannot_widen_the_node_startup_filter_and_empty_requests_work() {
    let (publisher, url) = server(Some(ExecutionProbeFilter::EntryFunction {
        address: Some("0x1".into()),
        module: None,
        function: None,
    }));
    let mut legacy = subscribe(&url, None).await;
    let filter = json!({"type":"UserTransactionFilter","payload":{"function":{"address":"0x2"}}});
    let mut selected = subscribe(&url, Some(&filter)).await;
    publish(&publisher, 1, vec![entry("0x1", 1, 0), entry("0x2", 2, 0)]);
    assert_eq!(indices(&next(&mut legacy).await), vec![0]);
    let empty = next(&mut selected).await;
    assert!(empty.last_chunk && empty.transactions.is_empty());
    assert_eq!(empty.sequence, 1);
}

#[tokio::test]
async fn invalid_or_unsupported_filters_are_rejected_before_ready() {
    let (_publisher, url) = server(None);
    let mut client = wire::execution_probe_client::ExecutionProbeClient::connect(url)
        .await
        .unwrap();
    for filter in [
        "{".into(),
        json!({"type":"EventFilter","data_substring_filter":"anything"}).to_string(),
        json!({"type":"UserTransactionFilter","sender":"invalid"}).to_string(),
        " ".repeat(16 * 1024 + 1),
    ] {
        let error = client
            .subscribe(SubscribeRequest {
                transaction_filter_json: filter,
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}

#[tokio::test]
async fn distinct_filter_limit_shares_identical_requests_and_releases_disconnected_feeds() {
    let (publisher, url) = server(None);
    let filters: Vec<_> = (1..=9)
        .map(|n| json!({"type":"UserTransactionFilter","sender":format!("0x{n:x}")}))
        .collect();
    let mut streams = Vec::new();
    for filter in &filters[..8] {
        streams.push(subscribe(&url, Some(filter)).await);
    }
    streams.push(subscribe(&url, Some(&filters[0])).await);
    let mut client = wire::execution_probe_client::ExecutionProbeClient::connect(url.clone())
        .await
        .unwrap();
    let error = client
        .subscribe(SubscribeRequest {
            transaction_filter_json: filters[8].to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    drop(streams);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while publisher.interested() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let _new = subscribe(&url, Some(&filters[8])).await;
    assert!(publisher.interested());
}
