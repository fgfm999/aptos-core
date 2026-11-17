// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use serde::Serialize;
use std::collections::{HashMap, VecDeque};

struct Entry {
    first: f64,
    probe: Option<(f64, String)>,
    indexer: Option<(f64, u64)>,
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

    pub fn probe(&mut self, hash: String, block: String, at: f64) {
        self.ensure(&hash, at);
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

    pub fn indexer(&mut self, hash: String, version: u64, at: f64) {
        self.ensure(&hash, at);
        let e = self.entries.get_mut(&hash).unwrap();
        if let Some((_, previous)) = e.indexer {
            self.counts.duplicates += 1;
            if previous != version {
                e.ambiguous = true;
            }
        } else {
            e.indexer = Some((at, version));
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
                (Some((p, block)), Some((i, version))) => {
                    self.counts.matched += 1;
                    matches.push(Match {
                        hash,
                        block_id: block,
                        version,
                        probe_ms: p,
                        indexer_ms: i,
                        delta_ms: i - p,
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
    #[test]
    fn arrival_order_and_negative_delta() {
        let mut m = Matcher::new(10, 100.0);
        m.indexer("a".into(), 9, 10.0);
        m.probe("a".into(), "b".into(), 15.0);
        assert_eq!(m.expire(120.0, false)[0].delta_ms, -5.0);
    }
    #[test]
    fn duplicates_and_forks_are_not_optimistic_samples() {
        let mut m = Matcher::new(10, 100.0);
        m.probe("a".into(), "b".into(), 1.0);
        m.indexer("a".into(), 4, 7.0);
        m.probe("a".into(), "b".into(), 2.0);
        m.probe("a".into(), "c".into(), 3.0);
        assert!(m.expire(200.0, false).is_empty());
        assert_eq!(m.counts.ambiguous, 1);
        assert_eq!(m.counts.duplicates, 2);
    }
    #[test]
    fn loss_is_counted() {
        let mut m = Matcher::new(1, 100.0);
        m.probe("a".into(), "b".into(), 1.0);
        m.indexer("c".into(), 9, 3.0);
        assert_eq!(m.counts.evicted, 1);
        m.expire(200.0, false);
        assert_eq!(m.counts.indexer_only, 1);
    }
    #[test]
    fn positive_delta_and_ttl() {
        let mut m = Matcher::new(10, 100.0);
        m.probe("a".into(), "b".into(), 1.0);
        m.indexer("a".into(), 9, 7.0);
        assert!(m.expire(50.0, false).is_empty());
        assert_eq!(m.expire(101.0, false)[0].delta_ms, 6.0);
    }
}
