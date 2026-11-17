// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use serde::Serialize;
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Both clocks are sampled at receipt, before processing the response's transactions.
#[derive(Clone, Copy)]
pub struct ReceivedAt {
    pub monotonic_ms: f64,
    pub unix_micros: i64,
}
impl ReceivedAt {
    pub fn now(elapsed: Duration) -> anyhow::Result<Self> {
        Ok(Self {
            monotonic_ms: elapsed.as_secs_f64() * 1000.0,
            unix_micros: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_micros()
                .try_into()?,
        })
    }
}

/// Missing, zero, pre-epoch, out-of-range and overflowing timestamps are not age samples.
pub fn timestamp_unix_micros(seconds: i64, nanos: i32) -> Option<i64> {
    if seconds <= 0 || seconds > 253_402_300_799 || !(0..1_000_000_000).contains(&nanos) {
        return None;
    }
    seconds
        .checked_mul(1_000_000)?
        .checked_add(i64::from(nanos) / 1000)
}

struct Entry {
    first: f64,
    probe: Option<(ReceivedAt, String)>,
    indexer: Option<(ReceivedAt, u64, Option<i64>)>,
    ambiguous: bool,
}
#[derive(Serialize)]
pub struct Match {
    pub hash: String,
    pub block_id: String,
    pub version: u64,
    pub probe_ms: f64,
    pub indexer_ms: f64,
    pub delta_ms: f64,
    pub txn_timestamp_unix_micros: Option<i64>,
    pub probe_received_unix_micros: i64,
    pub indexer_received_unix_micros: i64,
    pub probe_age_ms: Option<f64>,
    pub indexer_age_ms: Option<f64>,
}
#[derive(Default, Serialize)]
pub struct Counts {
    pub matched: u64,
    pub probe_only: u64,
    pub indexer_only: u64,
    pub ambiguous: u64,
    pub duplicates: u64,
    pub evicted: u64,
}
pub struct Matcher {
    entries: HashMap<String, Entry>,
    order: VecDeque<(String, f64)>,
    capacity: usize,
    ttl: f64,
    pub counts: Counts,
}
impl Matcher {
    pub fn new(capacity: usize, ttl_ms: f64) -> Self {
        assert!(capacity > 0 && ttl_ms > 0.0);
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
            ttl: ttl_ms,
            counts: Counts::default(),
        }
    }

    pub fn contains(&self, hash: &str) -> bool {
        self.entries.contains_key(hash)
    }

    pub fn probe(&mut self, hash: String, block: String, at: ReceivedAt) {
        self.ensure(&hash, at.monotonic_ms);
        let e = self.entries.get_mut(&hash).unwrap();
        if let Some((_, previous)) = &e.probe {
            self.counts.duplicates += 1;
            if previous != &block {
                e.ambiguous = true;
            }
        } else {
            e.probe = Some((at, block));
        }
    }

    pub fn indexer(
        &mut self,
        hash: String,
        version: u64,
        at: ReceivedAt,
        txn_timestamp_unix_micros: Option<i64>,
    ) {
        self.ensure(&hash, at.monotonic_ms);
        let e = self.entries.get_mut(&hash).unwrap();
        if let Some((_, previous, _)) = e.indexer {
            self.counts.duplicates += 1;
            if previous != version {
                e.ambiguous = true;
            }
        } else {
            e.indexer = Some((at, version, txn_timestamp_unix_micros));
        }
    }

    fn ensure(&mut self, hash: &str, at: f64) {
        if self.entries.contains_key(hash) {
            return;
        }
        if self.entries.len() == self.capacity
            && let Some((old, _)) = self.order.pop_front()
        {
            self.entries.remove(&old);
            self.counts.evicted += 1;
        }
        self.order.push_back((hash.to_string(), at));
        self.entries.insert(hash.to_string(), Entry {
            first: at,
            probe: None,
            indexer: None,
            ambiguous: false,
        });
    }

    pub fn expire(&mut self, at: f64, force: bool) -> Vec<Match> {
        let mut matches = Vec::new();
        while self
            .order
            .front()
            .is_some_and(|(_, first)| force || at - first >= self.ttl)
        {
            let (hash, _) = self.order.pop_front().unwrap();
            let e = self.entries.remove(&hash).unwrap();
            debug_assert!(force || at - e.first >= self.ttl);
            if e.ambiguous {
                self.counts.ambiguous += 1;
                continue;
            }
            match (e.probe, e.indexer) {
                (Some((p, block)), Some((i, version, txn_timestamp_unix_micros))) => {
                    self.counts.matched += 1;
                    matches.push(Match {
                        hash,
                        block_id: block,
                        version,
                        probe_ms: p.monotonic_ms,
                        indexer_ms: i.monotonic_ms,
                        delta_ms: i.monotonic_ms - p.monotonic_ms,
                        txn_timestamp_unix_micros,
                        probe_received_unix_micros: p.unix_micros,
                        indexer_received_unix_micros: i.unix_micros,
                        probe_age_ms: txn_timestamp_unix_micros
                            .map(|t| (i128::from(p.unix_micros) - i128::from(t)) as f64 / 1000.0),
                        indexer_age_ms: txn_timestamp_unix_micros
                            .map(|t| (i128::from(i.unix_micros) - i128::from(t)) as f64 / 1000.0),
                    });
                },
                (Some(_), None) => self.counts.probe_only += 1,
                (None, Some(_)) => self.counts.indexer_only += 1,
                (None, None) => unreachable!(),
            }
        }
        matches
    }
}
pub fn quantile(values: &[f64], q: f64) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(
            values[((q * values.len() as f64).ceil() as usize)
                .saturating_sub(1)
                .min(values.len() - 1)],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn at(ms: f64) -> ReceivedAt {
        ReceivedAt {
            monotonic_ms: ms,
            unix_micros: 1_700_000_000_000_000 + (ms * 1000.0) as i64,
        }
    }
    #[test]
    fn ages_use_original_receipts_in_either_arrival_order_and_keep_first_duplicate() {
        for indexer_first in [false, true] {
            let mut m = Matcher::new(10, 100.0);
            let timestamp = 1_700_000_000_000_000;
            if indexer_first {
                m.indexer("a".into(), 9, at(7.0), Some(timestamp));
            }
            m.probe("a".into(), "b".into(), at(1.25));
            m.probe("a".into(), "b".into(), at(3.0));
            if !indexer_first {
                m.indexer("a".into(), 9, at(7.0), Some(timestamp));
            }
            m.indexer("a".into(), 9, at(9.0), Some(timestamp));
            let row = m.expire(200.0, true).pop().unwrap();
            assert_eq!(row.txn_timestamp_unix_micros, Some(timestamp));
            assert_eq!(row.probe_received_unix_micros, timestamp + 1250);
            assert_eq!(row.indexer_received_unix_micros, timestamp + 7000);
            assert_eq!(row.probe_age_ms, Some(1.25));
            assert_eq!(row.indexer_age_ms, Some(7.0));
            assert_eq!(row.delta_ms, 5.75);
        }
    }
    #[test]
    fn missing_timestamp_preserves_delta_without_fabricating_age() {
        let mut m = Matcher::new(10, 100.0);
        m.probe("a".into(), "b".into(), at(1.0));
        m.indexer("a".into(), 9, at(7.0), None);
        let row = m.expire(200.0, true).pop().unwrap();
        assert_eq!(row.delta_ms, 6.0);
        assert_eq!(row.probe_age_ms, None);
        assert_eq!(row.indexer_age_ms, None);
    }
    #[test]
    fn wall_clock_anomalies_do_not_change_monotonic_delta() {
        let mut m = Matcher::new(10, 100.0);
        let timestamp = 1_700_000_000_000_000;
        m.probe("a".into(), "b".into(), ReceivedAt {
            monotonic_ms: 1.0,
            unix_micros: timestamp - 1000,
        });
        m.indexer(
            "a".into(),
            9,
            ReceivedAt {
                monotonic_ms: 7.0,
                unix_micros: timestamp - 2000,
            },
            Some(timestamp),
        );
        let row = m.expire(200.0, true).pop().unwrap();
        assert_eq!(row.delta_ms, 6.0);
        assert_eq!(row.probe_age_ms, Some(-1.0));
        assert_eq!(row.indexer_age_ms, Some(-2.0));
    }
    #[test]
    fn timestamp_validation_and_microsecond_units() {
        assert_eq!(
            timestamp_unix_micros(1_700_000_000, 123_456_000),
            Some(1_700_000_000_123_456)
        );
        for (s, n) in [
            (0, 0),
            (-1, 0),
            (1, -1),
            (1, 1_000_000_000),
            (i64::MAX, 0),
            (253_402_300_800, 0),
        ] {
            assert_eq!(timestamp_unix_micros(s, n), None);
        }
    }

    #[test]
    fn arrival_order_and_negative_delta() {
        let mut m = Matcher::new(10, 100.0);
        m.indexer("a".into(), 9, at(10.0), None);
        m.probe("a".into(), "b".into(), at(15.0));
        assert_eq!(m.expire(120.0, false)[0].delta_ms, -5.0);
    }
    #[test]
    fn duplicates_and_forks_are_not_optimistic_samples() {
        let mut m = Matcher::new(10, 100.0);
        m.probe("a".into(), "b".into(), at(1.0));
        m.indexer("a".into(), 4, at(7.0), None);
        m.probe("a".into(), "b".into(), at(2.0));
        m.probe("a".into(), "c".into(), at(3.0));
        assert!(m.expire(200.0, false).is_empty());
        assert_eq!(m.counts.ambiguous, 1);
        assert_eq!(m.counts.duplicates, 2);
    }
    #[test]
    fn loss_is_counted() {
        let mut m = Matcher::new(1, 100.0);
        m.probe("a".into(), "b".into(), at(1.0));
        m.indexer("c".into(), 9, at(3.0), None);
        assert_eq!(m.counts.evicted, 1);
        m.expire(200.0, false);
        assert_eq!(m.counts.indexer_only, 1);
    }
    #[test]
    fn positive_delta_and_ttl() {
        let mut m = Matcher::new(10, 100.0);
        m.probe("a".into(), "b".into(), at(1.0));
        m.indexer("a".into(), 9, at(7.0), None);
        assert!(m.expire(50.0, false).is_empty());
        assert_eq!(m.expire(101.0, false)[0].delta_ms, 6.0);
    }
}
