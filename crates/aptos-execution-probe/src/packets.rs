// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    payload::DataContext,
    successful_ids,
    wire::{ExecutedTransactionData, ProbeEvent, TransactionId},
    Hub, Observation, COUNTERS, DURATIONS,
};
use anyhow::{bail, Result};
use prost::Message;
use std::{sync::atomic::Ordering, time::Instant};

const MAX_BYTES: usize = 3 * 1024 * 1024;
fn nested_size(n: usize) -> usize {
    let mut v = n;
    let mut prefix = 1;
    while v >= 128 {
        prefix += 1;
        v >>= 7;
    }
    1 + prefix + n
}

pub(crate) fn build(
    o: &Observation,
    hub: &Hub,
    context: Option<&DataContext>,
) -> Result<Vec<ProbeEvent>> {
    let ids = successful_ids(&o.output.to_commit);
    let data = context.map(|context| {
        let at = Instant::now();
        let data = context.convert(&o.output, &ids);
        DURATIONS
            .with_label_values(&["data_conversion"])
            .observe(at.elapsed().as_secs_f64());
        COUNTERS
            .with_label_values(&["incomplete_data_transactions"])
            .inc_by(data.iter().filter(|d| !d.data_complete).count() as u64);
        data
    });
    let groups = partition(ids, data)?;
    let count = groups.len();
    let mut result = Vec::with_capacity(count);
    for (i, (ids, data)) in groups.into_iter().enumerate() {
        let event = ProbeEvent {
            ready: false,
            session_id: hub.session.clone(),
            chain_id: hub.chain,
            sequence: o.sequence,
            block_id: o.block.to_vec(),
            parent_block_id: o.parent.to_vec(),
            execution_done_unix_nanos: o.unix_nanos,
            chunk_index: i as u32,
            last_chunk: i + 1 == count,
            transactions: ids,
            transaction_data: data,
            include_data: context.is_some(),
            data_format: hub.data_format as i32,
            bcs_schema_version: hub.bcs_schema_version,
            dropped_blocks: hub.dropped.load(Ordering::Relaxed),
        };
        if event.encoded_len() > MAX_BYTES {
            bail!("probe message exceeds byte limit");
        }
        result.push(event);
    }
    Ok(result)
}

type Group = (Vec<TransactionId>, Vec<ExecutedTransactionData>);
fn partition(
    ids: Vec<TransactionId>,
    data: Option<Vec<ExecutedTransactionData>>,
) -> Result<Vec<Group>> {
    if let Some(data) = &data {
        anyhow::ensure!(ids.len() == data.len(), "data/identity count mismatch");
    }
    let mut data = data.map(|v| v.into_iter());
    let mut groups = Vec::new();
    let mut current: Group = (Vec::new(), Vec::new());
    // Reserve a conservative envelope allowance, including session and hashes.
    let mut bytes = 512;
    for id in ids {
        let datum = data.as_mut().and_then(|it| it.next());
        let size = nested_size(id.encoded_len())
            + datum.as_ref().map_or(0, |d| nested_size(d.encoded_len()));
        if size + 512 > MAX_BYTES {
            bail!("single executed transaction exceeds probe byte limit");
        }
        if !current.0.is_empty() && (current.0.len() == 1000 || bytes + size > MAX_BYTES) {
            groups.push(current);
            current = (Vec::new(), Vec::new());
            bytes = 512;
        }
        bytes += size;
        current.0.push(id);
        if let Some(datum) = datum {
            current.1.push(datum);
        }
    }
    if !current.0.is_empty() || groups.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_splitting_keeps_identity_data_alignment() {
        let ids = (0..3)
            .map(|i| TransactionId {
                hash: vec![i; 32],
                transaction_index: i as u32,
            })
            .collect();
        let data = (0..3)
            .map(|i| ExecutedTransactionData {
                hash: vec![i; 32],
                transaction_index: i as u32,
                raw_write_set_bcs: vec![1; 1_100_000],
                ..Default::default()
            })
            .collect();
        let groups = partition(ids, Some(data)).unwrap();
        assert_eq!(groups.len(), 2);
        for (ids, data) in groups {
            for (id, d) in ids.iter().zip(data) {
                assert_eq!(id.hash, d.hash);
            }
        }
    }
    #[test]
    fn oversized_transaction_fails_before_any_publication() {
        let id = TransactionId {
            hash: vec![0; 32],
            transaction_index: 0,
        };
        let d = ExecutedTransactionData {
            raw_write_set_bcs: vec![1; MAX_BYTES],
            ..Default::default()
        };
        assert!(partition(vec![id], Some(vec![d])).is_err());
        assert_eq!(partition(vec![], Some(vec![])).unwrap().len(), 1);
    }
}
