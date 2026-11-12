// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Sampled INFO block events. No per-block state is retained in the node.
use aptos_crypto::HashValue;
use once_cell::sync::Lazy;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static ORIGIN: Lazy<(Instant, String)> = Lazy::new(|| {
    (
        Instant::now(),
        format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ),
    )
});

fn relative_us(at: Instant, origin: Instant) -> i64 {
    if at >= origin {
        at.duration_since(origin).as_micros() as i64
    } else {
        -(origin.duration_since(at).as_micros() as i64)
    }
}

// Stable selection by block ID keeps every stage for the same sampled block.
// Boundaries must remain visible even when their synthetic block ID is not selected.
fn selected(stage: &str, id: HashValue) -> bool {
    matches!(stage, "state_reset" | "fallback_enter") || id.as_ref()[0] & 127 == 0
}

/// Saved ingress times may precede lazy initialization; retain their signed offsets.
/// The session separates processes/restarts. Wall clock is never used for intervals.
pub(crate) fn record(stage: &str, id: HashValue, epoch: u64, round: u64, at: Instant) {
    if !selected(stage, id) || !aptos_logger::enabled!(aptos_logger::Level::Info) {
        return;
    }
    let (origin, session) = &*ORIGIN;
    aptos_logger::info!(
        "OBSERVER_TIMELINE {}",
        serde_json::json!({
            "session": session, "block_id": id.to_hex(), "epoch": epoch, "round": round,
            "stage": stage, "mono_us": relative_us(at, *origin)
        })
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn signed_offsets_preserve_saved_ingress_intervals() {
        let origin = Instant::now();
        assert_eq!(relative_us(origin - Duration::from_micros(25), origin), -25);
        assert_eq!(relative_us(origin + Duration::from_micros(40), origin), 40);
    }

    #[test]
    fn sampling_keeps_all_stages_for_selected_blocks_and_all_resets() {
        let selected_id = HashValue::zero();
        let excluded_id = HashValue::new([1; 32]);
        for stage in [
            "payload_ingress",
            "order_ready",
            "execute_start",
            "commit_done",
        ] {
            assert!(selected(stage, selected_id));
            assert!(!selected(stage, excluded_id));
        }
        assert!(selected("state_reset", excluded_id));
        assert!(selected("fallback_enter", excluded_id));
        let count = (0..=255u8)
            .filter(|byte| selected("execute_done", HashValue::new([*byte; 32])))
            .count();
        assert_eq!(count, 2);
    }
}
