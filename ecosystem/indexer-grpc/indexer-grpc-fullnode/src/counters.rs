// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use aptos_metrics_core::{
    register_int_counter, register_int_gauge, register_int_gauge_vec, IntCounter, IntGauge,
    IntGaugeVec,
};
use once_cell::sync::Lazy;

/// Number of times the indexer has been unable to fetch a transaction. Ideally zero.
pub static UNABLE_TO_FETCH_TRANSACTION: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "indexer_grpc_unable_to_fetch_transaction_count",
        "Number of times the indexer has been unable to fetch a transaction from storage"
    )
    .unwrap()
});

/// Channel size
pub static CHANNEL_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "indexer_grpc_fullnode_channel_size",
        "Channel size for full node",
        &["step"],
    )
    .unwrap()
});

pub static LATENCY_MS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "indexer_grpc_fullnode_latency_ms",
        "Latency of indexer fullnode (comparing with block timestamp).",
    )
    .unwrap()
});

/// One sample per successfully converted task, using sizes already computed for the response.
pub fn observe_api_conversion_work(kind: &'static str, value: u64) {
    static WORK: Lazy<aptos_metrics_core::HistogramVec> = Lazy::new(|| {
        aptos_metrics_core::register_histogram_vec!(
            "indexer_grpc_api_conversion_work",
            "Work per successful API conversion task; byte fields are component sizes, not wire size",
            &["kind"],
            vec![1.0, 4.0, 16.0, 64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0]
        ).unwrap()
    });
    WORK.with_label_values(&[kind]).observe(value as f64);
}
