// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

// Wire definitions mirror proto/execution_probe.proto. The tonic service module
// is generated using the repository's existing server-streaming service template.
#[derive(Clone, PartialEq, prost::Message)]
pub struct SubscribeRequest {}
#[derive(Clone, PartialEq, prost::Message)]
pub struct TransactionId {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub transaction_index: u32,
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
}
include!("service.rs");
