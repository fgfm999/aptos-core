// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Opt-in block events. No per-block state is retained in the node.
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

/// Saved ingress times may precede lazy initialization; retain their signed offsets.
/// The session separates processes/restarts. Wall clock is never used for intervals.
pub(crate) fn record(stage: &str, id: HashValue, epoch: u64, round: u64, at: Instant) {
    if !aptos_logger::enabled!(aptos_logger::Level::Debug) {
        return;
    }
    let (origin, session) = &*ORIGIN;
    aptos_logger::debug!(
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
}
