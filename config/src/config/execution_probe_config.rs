// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Experimental live-only execution identity feed, disabled by default.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionProbeConfig {
    pub enabled: bool,
    /// Include Indexer-typed events and changes in the executed envelope.
    pub include_data: bool,
    pub address: SocketAddr,
}
impl Default for ExecutionProbeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            include_data: false,
            address: "127.0.0.1:50052".parse().unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disabled_default_and_config_roundtrip() {
        let config: ExecutionProbeConfig = serde_yaml::from_str("{}").unwrap();
        assert!(!config.enabled);
        assert!(!config.include_data);
        assert_eq!(config.address.to_string(), "127.0.0.1:50052");
        let explicit: ExecutionProbeConfig =
            serde_yaml::from_str("enabled: true\ninclude_data: true\naddress: 127.0.0.1:51234")
                .unwrap();
        assert!(explicit.enabled);
        assert!(explicit.include_data);
        assert_eq!(
            serde_yaml::from_str::<ExecutionProbeConfig>(
                &serde_yaml::to_string(&explicit).unwrap()
            )
            .unwrap(),
            explicit
        );
        assert!(serde_yaml::from_str::<ExecutionProbeConfig>("unknown: true").is_err());
    }
}
