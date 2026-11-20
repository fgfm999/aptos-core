// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

// Wire definitions mirror proto/execution_probe.proto. The tonic service module
// is generated using the repository's existing server-streaming service template.
pub const BCS_SCHEMA_VERSION: u32 = 1;
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum DataFormat {
    Json = 0,
    Bcs = 1,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SubscribeRequest {
    #[prost(string, tag = "1")]
    pub transaction_filter_json: String,
    #[prost(uint32, tag = "2")]
    pub boundary_protocol_version: u32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TransactionId {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub transaction_index: u32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct ExecutedTransactionData {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub transaction_index: u32,
    #[prost(message, repeated, tag = "3")]
    pub events: Vec<aptos_protos::transaction::v1::Event>,
    #[prost(message, repeated, tag = "4")]
    pub write_set_changes: Vec<aptos_protos::transaction::v1::WriteSetChange>,
    #[prost(bool, tag = "5")]
    pub data_complete: bool,
    #[prost(string, tag = "6")]
    pub conversion_error: String,
    #[prost(bytes = "vec", tag = "7")]
    pub raw_events_bcs: Vec<u8>,
    #[prost(bytes = "vec", tag = "8")]
    pub raw_write_set_bcs: Vec<u8>,
    #[prost(bytes = "vec", tag = "9")]
    pub execution_output_bcs: Vec<u8>,
    #[prost(bytes = "vec", tag = "10")]
    pub transaction_bcs: Vec<u8>,
}
pub const BOUNDARY_PROTOCOL_VERSION: u32 = 1;
#[derive(Clone, PartialEq, prost::Message)]
pub struct CommittedAnchor {
    #[prost(bytes = "vec", tag = "1")]
    pub canonical_block_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub execution_root_block_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub version: u64,
    #[prost(uint64, tag = "4")]
    pub epoch: u64,
    #[prost(uint64, tag = "5")]
    pub round: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum BoundaryReason {
    Unspecified = 0,
    ExecutorReset = 1,
    ExecutorFinish = 2,
    EpochRoot = 3,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct StreamBoundary {
    #[prost(enumeration = "BoundaryReason", tag = "1")]
    pub reason: i32,
    #[prost(message, optional, tag = "2")]
    pub anchor: Option<CommittedAnchor>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub struct ProbeEvent {
    #[prost(bool, tag = "1")]
    pub ready: bool,
    #[prost(string, tag = "2")]
    pub session_id: String,
    #[prost(uint32, tag = "3")]
    pub chain_id: u32,
    #[prost(uint64, tag = "4")]
    pub sequence: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub block_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    pub parent_block_id: Vec<u8>,
    #[prost(uint64, tag = "7")]
    pub execution_done_unix_nanos: u64,
    #[prost(uint32, tag = "8")]
    pub chunk_index: u32,
    #[prost(bool, tag = "9")]
    pub last_chunk: bool,
    #[prost(message, repeated, tag = "10")]
    pub transactions: Vec<TransactionId>,
    #[prost(uint64, tag = "11")]
    pub dropped_blocks: u64,
    #[prost(bool, tag = "12")]
    pub include_data: bool,
    #[prost(message, repeated, tag = "13")]
    pub transaction_data: Vec<ExecutedTransactionData>,
    #[prost(enumeration = "DataFormat", tag = "14")]
    pub data_format: i32,
    #[prost(uint32, tag = "15")]
    pub bcs_schema_version: u32,
    /// Ready-only startup filter, serialized as config JSON; empty means no filter.
    #[prost(string, tag = "16")]
    pub transaction_filter_json: String,
    #[prost(string, tag = "17")]
    pub subscription_filter_json: String,
    #[prost(uint32, tag = "18")]
    pub boundary_protocol_version: u32,
    #[prost(uint64, tag = "19")]
    pub stream_generation: u64,
    #[prost(message, optional, tag = "20")]
    pub boundary: Option<StreamBoundary>,
}
include!("service.rs");
