//! Execution boundaries bypass the bounded observation queue and remain visible in Ready.
use super::*;
use aptos_types::ledger_info::LedgerInfo;
use wire::{BoundaryReason, CommittedAnchor, StreamBoundary};

#[derive(Clone)]
pub(crate) struct StreamState {
    pub generation: u64,
    pub boundary: Option<StreamBoundary>,
}
pub(crate) fn initial_state() -> tokio::sync::watch::Sender<StreamState> {
    tokio::sync::watch::channel(StreamState {
        generation: 1,
        boundary: None,
    })
    .0
}
impl Publisher {
    pub fn executor_finish(&self) {
        self.boundary(StreamBoundary {
            reason: BoundaryReason::ExecutorFinish as i32,
            anchor: None,
        });
    }
    pub fn executor_reset(&self, ledger: &LedgerInfo, root: HashValue) {
        self.anchored_boundary(BoundaryReason::ExecutorReset, ledger, root);
    }
    pub fn epoch_root(&self, ledger: &LedgerInfo, root: HashValue) {
        self.anchored_boundary(BoundaryReason::EpochRoot, ledger, root);
    }
    fn anchored_boundary(&self, reason: BoundaryReason, ledger: &LedgerInfo, root: HashValue) {
        self.boundary(StreamBoundary {
            reason: reason as i32,
            anchor: Some(CommittedAnchor {
                canonical_block_id: ledger.consensus_block_id().to_vec(),
                execution_root_block_id: root.to_vec(),
                version: ledger.version(),
                epoch: ledger.epoch(),
                round: ledger.round(),
            }),
        });
    }
    fn boundary(&self, boundary: StreamBoundary) {
        self.hub.boundaries.send_modify(|state| {
            state.generation = state
                .generation
                .checked_add(1)
                .expect("probe generation exhausted");
            state.boundary = Some(boundary);
            self.hub
                .generation
                .store(state.generation, Ordering::Release);
            let boundary=state.boundary.as_ref().expect("boundary installed");
            let reason=match BoundaryReason::try_from(boundary.reason).expect("internal boundary reason") {
                BoundaryReason::ExecutorReset=>"executor_reset",BoundaryReason::ExecutorFinish=>"executor_finish",
                BoundaryReason::EpochRoot=>"epoch_root",BoundaryReason::Unspecified=>"unspecified",
            };
            let anchor=boundary.anchor.as_ref().map(|a|serde_json::json!({
                "canonical_block_id":hex::encode(&a.canonical_block_id),
                "execution_root_block_id":hex::encode(&a.execution_root_block_id),
                "version":a.version,"epoch":a.epoch,"round":a.round,
            }));
            aptos_logger::info!("Execution probe boundary {}",serde_json::json!({"stream_generation":state.generation,"reason":reason,"anchor":anchor}));
        });
        COUNTERS.with_label_values(&["stream_boundaries"]).inc();
    }
}
impl Hub {
    pub(crate) fn boundary_event(&self, state: &StreamState) -> ProbeEvent {
        ProbeEvent {
            session_id: self.session.clone(),
            chain_id: self.chain,
            boundary_protocol_version: wire::BOUNDARY_PROTOCOL_VERSION,
            stream_generation: state.generation,
            boundary: state.boundary.clone(),
            include_data: self.include_data,
            data_format: self.data_format as i32,
            bcs_schema_version: self.bcs_schema_version,
            dropped_blocks: self.dropped.load(Ordering::Acquire),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_types::block_info::BlockInfo;
    use tokio_stream::StreamExt;

    fn fixture() -> (Publisher, Arc<Hub>, mpsc::Receiver<Observation>) {
        let hub = Arc::new(Hub {
            session: "boundaries".into(),
            chain: 4,
            generation: AtomicU64::new(1),
            boundaries: initial_state(),
            feeds: Mutex::new(HashMap::new()),
            active_feeds: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            include_data: false,
            data_format: wire::DataFormat::Json,
            bcs_schema_version: 0,
            filter: None,
            filter_json: String::new(),
        });
        let (sender, receiver) = mpsc::channel(16);
        let publisher = Publisher {
            sender,
            hub: hub.clone(),
            sequence: Arc::new(AtomicU64::new(0)),
        };
        (publisher, hub, receiver)
    }
    async fn next(
        stream: &mut Pin<Box<dyn Stream<Item = Result<ProbeEvent, Status>> + Send>>,
    ) -> Result<ProbeEvent, Box<Status>> {
        tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("probe stream stalled")
            .expect("probe stream closed")
            .map_err(Box::new)
    }
    async fn subscribe(
        hub: Arc<Hub>,
        version: u32,
    ) -> Pin<Box<dyn Stream<Item = Result<ProbeEvent, Status>> + Send>> {
        Service(hub)
            .subscribe(Request::new(SubscribeRequest {
                boundary_protocol_version: version,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
    }
    fn ledger() -> LedgerInfo {
        LedgerInfo::new(
            BlockInfo::new(
                7,
                3,
                HashValue::new([1; 32]),
                HashValue::zero(),
                19,
                0,
                None,
            ),
            HashValue::zero(),
        )
    }
    fn publish(publisher: &Publisher) {
        publisher.publish(
            publisher.capture().unwrap(),
            HashValue::new([3; 32]),
            HashValue::new([9; 32]),
            ExecutionOutput::new_dummy_with_input_txns(vec![]),
        );
    }
    #[tokio::test]
    async fn finish_bypasses_full_observation_queue_and_is_retained_in_ready() {
        let (publisher, hub, receiver) = fixture();
        let mut stream = subscribe(hub.clone(), 1).await;
        assert_eq!(next(&mut stream).await.unwrap().stream_generation, 1);
        for _ in 0..16 {
            publish(&publisher);
        }
        assert_eq!(receiver.len(), 16);
        publisher.executor_finish();
        let boundary = next(&mut stream).await.unwrap();
        assert_eq!(boundary.stream_generation, 2);
        assert_eq!(boundary.sequence, 0);
        assert_eq!(
            boundary.boundary.unwrap().reason,
            BoundaryReason::ExecutorFinish as i32
        );
        assert_eq!(
            receiver.len(),
            16,
            "control must not drain or wait on execution observations"
        );
        let mut joined = subscribe(hub, 1).await;
        let ready = next(&mut joined).await.unwrap();
        assert!(ready.ready);
        assert_eq!(ready.stream_generation, 2);
        assert!(ready.boundary.unwrap().anchor.is_none());
    }
    #[tokio::test]
    async fn boundary_during_conversion_discards_its_error_without_poisoning_the_new_feed() {
        let (publisher, hub, mut queued) = fixture();
        let mut stream = subscribe(hub.clone(), 1).await;
        next(&mut stream).await.unwrap();
        publish(&publisher);
        let observation = queued.recv().await.unwrap();
        publish_observation_with(&observation, &hub, |_| {
            publisher.executor_finish();
            anyhow::bail!("old-generation conversion failed after the boundary")
        });
        assert_eq!(hub.dropped.load(Ordering::Acquire), 0);
        assert!(hub
            .live_feeds()
            .iter()
            .all(|feed| feed.dropped.load(Ordering::Acquire) == 0));
        assert_eq!(next(&mut stream).await.unwrap().stream_generation, 2);
    }

    #[tokio::test]
    async fn legacy_subscription_terminates_at_boundary() {
        let (publisher, hub, _) = fixture();
        let mut stream = subscribe(hub, 0).await;
        next(&mut stream).await.unwrap();
        publisher.executor_finish();
        assert_eq!(
            next(&mut stream).await.unwrap_err().code(),
            tonic::Code::Aborted
        );
    }
    #[tokio::test]
    async fn stale_capture_and_queued_conversion_cannot_enter_new_generation() {
        let (publisher, hub, mut queued) = fixture();
        let mut stream = subscribe(hub.clone(), 1).await;
        next(&mut stream).await.unwrap();
        let old = publisher.capture().unwrap();
        publish(&publisher);
        let observation = queued.recv().await.unwrap();
        publisher.executor_finish();
        publisher.publish(
            old,
            HashValue::zero(),
            HashValue::zero(),
            ExecutionOutput::new_dummy_with_input_txns(vec![]),
        );
        assert!(queued.try_recv().is_err());
        publish_observation(&observation, &hub, None);
        assert_eq!(hub.dropped.load(Ordering::Relaxed), 0);
        next(&mut stream).await.unwrap();
        publisher.executor_reset(&ledger(), HashValue::new([9; 32]));
        publish(&publisher);
        let observation = queued.recv().await.unwrap();
        publish_observation(&observation, &hub, None);
        let control = next(&mut stream).await.unwrap();
        assert_eq!(control.stream_generation, 3);
        let anchor = control.boundary.unwrap().anchor.unwrap();
        assert_eq!(anchor.canonical_block_id, vec![1; 32]);
        assert_eq!(anchor.execution_root_block_id, vec![9; 32]);
        assert_eq!(anchor.version, 19);
        let data = next(&mut stream).await.unwrap();
        assert!(data.boundary.is_none());
        assert_eq!(data.stream_generation, 3);
        assert_eq!(data.block_id, vec![3; 32]);
    }
    #[tokio::test]
    async fn latest_coalesced_boundary_precedes_new_data_for_all_filters() {
        let (publisher, hub, mut queued) = fixture();
        let mut a = subscribe(hub.clone(), 1).await;
        let mut b = Service(hub.clone())
            .subscribe(Request::new(SubscribeRequest {
                boundary_protocol_version: 1,
                transaction_filter_json: r#"{"type":"TransactionRootFilter","success":true}"#
                    .into(),
            }))
            .await
            .unwrap()
            .into_inner();
        next(&mut a).await.unwrap();
        next(&mut b).await.unwrap();
        publisher.executor_finish();
        publisher.executor_reset(&ledger(), HashValue::new([9; 32]));
        publisher.epoch_root(&ledger(), HashValue::new([8; 32]));
        publish(&publisher);
        let observation = queued.recv().await.unwrap();
        publish_observation(&observation, &hub, None);
        for stream in [&mut a, &mut b] {
            let mut control = next(stream).await.unwrap();
            while control.stream_generation < 4 {
                control = next(stream).await.unwrap();
            }
            assert_eq!(
                control.boundary.unwrap().reason,
                BoundaryReason::EpochRoot as i32
            );
            let data = next(stream).await.unwrap();
            assert_eq!(data.stream_generation, 4);
            assert!(data.boundary.is_none());
        }
    }
}
