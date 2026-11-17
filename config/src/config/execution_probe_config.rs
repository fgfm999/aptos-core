// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Encoding of optional execution output. JSON preserves the Indexer-shaped feed.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionProbeDataFormat {
    #[default]
    Json,
    Bcs,
}

/// Startup-configured probe predicates. They inspect native fields, never decoded Move bodies.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionProbeFilter {
    And {
        filters: Vec<Self>,
    },
    Or {
        filters: Vec<Self>,
    },
    Not {
        filter: Box<Self>,
    },
    Sender {
        address: String,
    },
    EntryFunction {
        address: Option<String>,
        module: Option<String>,
        function: Option<String>,
    },
    Event {
        address: Option<String>,
        module: Option<String>,
        name: Option<String>,
        /// Full TypeTag including generic arguments; absent means no generic constraint.
        type_tag: Option<String>,
    },
}

/// Experimental live-only execution identity feed, disabled by default.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionProbeConfig {
    pub enabled: bool,
    /// Include execution output in the selected data_format.
    pub include_data: bool,
    pub data_format: ExecutionProbeDataFormat,
    pub address: SocketAddr,
    /// Additional selection within successful user transactions; shared by all subscribers.
    pub transaction_filter: Option<ExecutionProbeFilter>,
}
impl Default for ExecutionProbeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            include_data: false,
            data_format: ExecutionProbeDataFormat::Json,
            transaction_filter: None,
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
        assert!(config.transaction_filter.is_none());
        assert_eq!(config.data_format, ExecutionProbeDataFormat::Json);
        let bcs: ExecutionProbeConfig =
            serde_yaml::from_str("include_data: true\ndata_format: bcs").unwrap();
        assert_eq!(bcs.data_format, ExecutionProbeDataFormat::Bcs);
        assert!(serde_yaml::from_str::<ExecutionProbeConfig>("data_format: unknown").is_err());
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
    #[test]
    fn filter_yaml_is_typed_and_roundtrips() {
        let config: ExecutionProbeConfig = serde_yaml::from_str("transaction_filter:\n  type: and\n  filters:\n    - type: sender\n      address: '0x1'\n    - type: not\n      filter:\n        type: entry_function\n        address: '0x2'\n").unwrap();
        assert_eq!(
            serde_yaml::from_str::<ExecutionProbeConfig>(&serde_yaml::to_string(&config).unwrap())
                .unwrap(),
            config
        );
        assert!(serde_yaml::from_str::<ExecutionProbeConfig>(
            "transaction_filter:\n  type: event\n  data_substring: nope"
        )
        .is_err());
    }
}
