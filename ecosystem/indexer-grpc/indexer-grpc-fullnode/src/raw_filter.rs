//! Evaluates transaction filters before expensive Move/API conversion.
//!
//! A definite rejection skips conversion. A definite match is converted and emitted
//! without running the filter again. An unknown result is converted normally, then
//! checked against the original filter using the complete protobuf transaction.
//! Decisions stay attached to their transactions through conversion. Scanned version
//! ranges are tracked separately, so rejected transactions still advance stream progress.

use aptos_api_types::TransactionOnChainData;
use aptos_protos::{indexer::v1 as filter_pb, transaction::v1 as pb};
use aptos_transaction_filter::{BooleanTransactionFilter, Filterable};
use aptos_types::transaction::Transaction;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    /// The complete filter is known to accept this transaction.
    Match,
    /// The complete filter is known to reject this transaction.
    NoMatch,
    /// At least one required condition needs fields absent from the raw projection.
    Unknown,
}
impl Decision {
    /// Calls the original PB filter lazily, only when raw evaluation was inconclusive.
    pub(crate) fn resolve(self, fallback: impl FnOnce() -> bool) -> bool {
        match self {
            Self::Match => true,
            Self::NoMatch => false,
            Self::Unknown => fallback(),
        }
    }

    fn from_bool(value: bool) -> Self {
        if value {
            Self::Match
        } else {
            Self::NoMatch
        }
    }
    // Preserve uncertainty under boolean composition: false AND unknown is false,
    // true OR unknown is true, and NOT unknown remains unknown.
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::NoMatch, _) | (_, Self::NoMatch) => Self::NoMatch,
            (Self::Match, Self::Match) => Self::Match,
            _ => Self::Unknown,
        }
    }
    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::Match, _) | (_, Self::Match) => Self::Match,
            (Self::NoMatch, Self::NoMatch) => Self::NoMatch,
            _ => Self::Unknown,
        }
    }
    fn not(self) -> Self {
        match self {
            Self::Match => Self::NoMatch,
            Self::NoMatch => Self::Match,
            Self::Unknown => Self::Unknown,
        }
    }
}

/// A compiled copy of the validated filter with unsupported leaf conditions removed
/// and remembered. Supported predicates reuse the existing filter implementations,
/// including their address normalization, against a lightweight PB projection.
pub(crate) enum RawFilter {
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Root(aptos_transaction_filter::TransactionRootFilter),
    /// The boolean marks an unsupported leaf retained for conservative fallback.
    User(aptos_transaction_filter::UserTransactionFilter, bool),
    /// The boolean records a removed JSON-data substring condition.
    Event(aptos_transaction_filter::EventFilter, bool),
    Unknown,
}
impl RawFilter {
    /// Compile once for a coordinator batch; share the result across conversion tasks.
    pub(crate) fn new(filter: &BooleanTransactionFilter) -> Self {
        Self::compile(filter.clone().into())
    }
    fn compile(filter: filter_pb::BooleanTransactionFilter) -> Self {
        use filter_pb::{api_filter::Filter as Api, boolean_transaction_filter::Filter};
        match filter.filter {
            Some(Filter::LogicalAnd(f)) => {
                Self::And(f.filters.into_iter().map(Self::compile).collect())
            },
            Some(Filter::LogicalOr(f)) => {
                Self::Or(f.filters.into_iter().map(Self::compile).collect())
            },
            Some(Filter::LogicalNot(f)) => Self::Not(Box::new(Self::compile(*f))),
            Some(Filter::ApiFilter(f)) => match f.filter {
                Some(Api::TransactionRootFilter(f)) => Self::Root(f.into()),
                Some(Api::UserTransactionFilter(f)) => Self::User(f.into(), false),
                Some(Api::EventFilter(mut f)) => {
                    // Event data is BCS here; the original substring predicate searches
                    // decoded JSON. Searching the raw bytes would change its meaning.
                    let unknown_data = f.data_substring_filter.take().is_some();
                    Self::Event(f.into(), unknown_data)
                },
                _ => Self::Unknown,
            },
            _ => Self::Unknown,
        }
    }
    /// Evaluates only cheap raw fields, without resolving Move layouts or decoding data.
    pub(crate) fn evaluate(&self, raw: &TransactionOnChainData) -> Decision {
        let projection = projection(raw, self.needs_events());
        let payload_known = matches!(&raw.transaction,
            Transaction::UserTransaction(txn) if matches!(txn.payload(), aptos_types::transaction::TransactionPayload::EntryFunction(_)));
        self.evaluate_with_payload(&projection, payload_known)
    }
    // Type/success/sender-only filters do not need event allocations.
    fn needs_events(&self) -> bool {
        match self {
            Self::And(filters) | Self::Or(filters) => filters.iter().any(Self::needs_events),
            Self::Not(filter) => filter.needs_events(),
            Self::Event(_, _) => true,
            _ => false,
        }
    }
    #[cfg(test)]
    fn evaluate_projection(&self, txn: &pb::Transaction) -> Decision {
        self.evaluate_with_payload(txn, true)
    }
    fn evaluate_with_payload(&self, txn: &pb::Transaction, payload_known: bool) -> Decision {
        match self {
            Self::And(filters) => filters.iter().fold(Decision::Match, |a, f| {
                a.and(f.evaluate_with_payload(txn, payload_known))
            }),
            Self::Or(filters) => filters.iter().fold(Decision::NoMatch, |a, f| {
                a.or(f.evaluate_with_payload(txn, payload_known))
            }),
            Self::Not(filter) => filter.evaluate_with_payload(txn, payload_known).not(),
            Self::Root(filter) => Decision::from_bool(filter.matches(txn)),
            Self::User(filter, unknown) => {
                // Direct entry functions have all predicate fields in the projection.
                // Other payload forms (including multisig resolution) retain the PB
                // fallback. Sender and user-type mismatches are still definitive.
                if *unknown || (!payload_known && filter.payload.is_some()) {
                    let mut supported = filter.clone();
                    supported.payload = None;
                    Decision::from_bool(supported.matches(txn)).and(Decision::Unknown)
                } else {
                    Decision::from_bool(filter.matches(txn))
                }
            },
            Self::Event(filter, unknown) => {
                use pb::transaction::TxnData;
                // Match the transaction variants inspected by the existing PB EventFilter;
                // some native variants carry events that the PB predicate does not inspect.
                let events = match txn.txn_data.as_ref() {
                    Some(TxnData::User(t)) => &t.events,
                    Some(TxnData::Genesis(t)) => &t.events,
                    Some(TxnData::BlockMetadata(t)) => &t.events,
                    _ => return Decision::NoMatch,
                };
                let result = Decision::from_bool(events.iter().any(|e| filter.matches(e)));
                if *unknown {
                    result.and(Decision::Unknown)
                } else {
                    result
                }
            },
            Self::Unknown => Decision::Unknown,
        }
    }
}

// This is a predicate input, not a complete transaction for clients. Omitted fields
// must never be inspected by a supported predicate. Event struct generics and data
// are omitted because current struct-type filters only use address/module/name.
// When adding filter fields, extend this projection or keep the new condition Unknown.
fn projection(raw: &TransactionOnChainData, include_events: bool) -> pb::Transaction {
    use pb::transaction::{TransactionType, TxnData};
    let events = || {
        if !include_events {
            return vec![];
        }
        raw.events
            .iter()
            .map(|event| {
                let r#type = match event.type_tag() {
                    move_core_types::language_storage::TypeTag::Struct(tag) => Some(pb::MoveType {
                        content: Some(pb::move_type::Content::Struct(pb::MoveStructTag {
                            address: tag.address.to_hex_literal(),
                            module: tag.module.to_string(),
                            name: tag.name.to_string(),
                            generic_type_params: vec![],
                        })),
                        ..Default::default()
                    }),
                    _ => None,
                };
                pb::Event {
                    r#type,
                    ..Default::default()
                }
            })
            .collect()
    };
    let (kind, data) = match &raw.transaction {
        Transaction::UserTransaction(txn) => (
            TransactionType::User,
            Some(TxnData::User(pb::UserTransaction {
                request: Some(pb::UserTransactionRequest {
                    sender: txn.sender().to_hex_literal(),
                    payload: raw_entry_payload(txn.payload()),
                    ..Default::default()
                }),
                events: events(),
                ..Default::default()
            })),
        ),
        Transaction::GenesisTransaction(_) => (
            TransactionType::Genesis,
            Some(TxnData::Genesis(pb::GenesisTransaction {
                events: events(),
                ..Default::default()
            })),
        ),
        Transaction::BlockMetadata(_) | Transaction::BlockMetadataExt(_) => (
            TransactionType::BlockMetadata,
            Some(TxnData::BlockMetadata(pb::BlockMetadataTransaction {
                events: events(),
                ..Default::default()
            })),
        ),
        Transaction::StateCheckpoint(_) => (TransactionType::StateCheckpoint, None),
        Transaction::ValidatorTransaction(_) => (TransactionType::Validator, None),
        Transaction::BlockEpilogue(_) => (TransactionType::BlockEpilogue, None),
    };
    pb::Transaction {
        r#type: kind as i32,
        info: Some(pb::TransactionInfo {
            success: raw.info.status().is_success(),
            ..Default::default()
        }),
        txn_data: data,
        ..Default::default()
    }
}

// Read the entry-function identity directly; arguments and generic values are not
// needed by EntryFunctionFilter and must not trigger Move decoding here.
fn raw_entry_payload(
    payload: &aptos_types::transaction::TransactionPayload,
) -> Option<pb::TransactionPayload> {
    let aptos_types::transaction::TransactionPayload::EntryFunction(entry) = payload else {
        return None;
    };
    Some(pb::TransactionPayload {
        payload: Some(pb::transaction_payload::Payload::EntryFunctionPayload(
            pb::EntryFunctionPayload {
                function: Some(pb::EntryFunctionId {
                    module: Some(pb::MoveModuleId {
                        address: entry.module().address().to_hex_literal(),
                        name: entry.module().name().to_string(),
                    }),
                    name: entry.function().to_string(),
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_is_never_inverted_into_rejection() {
        assert_eq!(Decision::Unknown.not(), Decision::Unknown);
        assert_eq!(Decision::Unknown.and(Decision::NoMatch), Decision::NoMatch);
        assert_eq!(Decision::Unknown.or(Decision::NoMatch), Decision::Unknown);
        assert_eq!(Decision::Unknown.or(Decision::Match), Decision::Match);
    }
    #[test]
    fn sender_and_root_match_existing_predicates() {
        let txn = pb::Transaction {
            r#type: pb::transaction::TransactionType::User as i32,
            info: Some(pb::TransactionInfo {
                success: true,
                ..Default::default()
            }),
            txn_data: Some(pb::transaction::TxnData::User(pb::UserTransaction {
                request: Some(pb::UserTransactionRequest {
                    sender: "0x1".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        for kind in [
            pb::transaction::TransactionType::User,
            pb::transaction::TransactionType::Genesis,
        ] {
            let filter = aptos_transaction_filter::TransactionRootFilter::from(
                filter_pb::TransactionRootFilter {
                    success: Some(true),
                    transaction_type: Some(kind as i32),
                },
            );
            let expected = filter.matches(&txn);
            assert_eq!(
                RawFilter::Root(filter).evaluate_projection(&txn),
                Decision::from_bool(expected)
            );
        }
        let filter = aptos_transaction_filter::UserTransactionFilter::from(
            filter_pb::UserTransactionFilter {
                sender: Some("0x01".into()),
                payload_filter: None,
            },
        );
        assert_eq!(
            RawFilter::User(filter.clone(), false).evaluate_projection(&txn),
            Decision::Match
        );
        assert_eq!(
            RawFilter::User(filter, true).evaluate_projection(&txn),
            Decision::Unknown
        );
    }
    #[test]
    fn json_substring_remains_unknown_when_event_exists() {
        let filter = RawFilter::compile(filter_pb::BooleanTransactionFilter {
            filter: Some(filter_pb::boolean_transaction_filter::Filter::ApiFilter(
                filter_pb::ApiFilter {
                    filter: Some(filter_pb::api_filter::Filter::EventFilter(
                        filter_pb::EventFilter {
                            struct_type: None,
                            data_substring_filter: Some("pool".into()),
                        },
                    )),
                },
            )),
        });
        let txn = pb::Transaction {
            txn_data: Some(pb::transaction::TxnData::User(pb::UserTransaction {
                events: vec![pb::Event::default()],
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(filter.evaluate_projection(&txn), Decision::Unknown);
        assert_eq!(
            RawFilter::Not(Box::new(filter)).evaluate_projection(&txn),
            Decision::Unknown
        );
    }

    #[test]
    fn raw_non_user_types_preserve_root_filter_results() {
        use aptos_types::transaction::{ExecutionStatus, TransactionInfo};
        let hash = aptos_crypto::HashValue::zero();
        for (native, kind) in [
            (
                Transaction::StateCheckpoint(hash),
                pb::transaction::TransactionType::StateCheckpoint,
            ),
            (
                Transaction::block_epilogue_v0(
                    hash,
                    aptos_types::transaction::block_epilogue::BlockEndInfo::new_empty(),
                ),
                pb::transaction::TransactionType::BlockEpilogue,
            ),
        ] {
            for success in [true, false] {
                let raw = TransactionOnChainData {
                    version: 42,
                    transaction: native.clone(),
                    info: TransactionInfo::new_placeholder(
                        0,
                        None,
                        if success {
                            ExecutionStatus::Success
                        } else {
                            ExecutionStatus::OutOfGas
                        },
                    ),
                    events: vec![],
                    accumulator_root_hash: hash,
                    changes: Default::default(),
                };
                for requested_kind in [kind, pb::transaction::TransactionType::User] {
                    let filter = aptos_transaction_filter::TransactionRootFilter::from(
                        filter_pb::TransactionRootFilter {
                            success: Some(true),
                            transaction_type: Some(requested_kind as i32),
                        },
                    );
                    let expected = success && kind == requested_kind;
                    assert_eq!(
                        RawFilter::Root(filter).evaluate(&raw),
                        Decision::from_bool(expected)
                    );
                }
            }
        }
    }
    #[test]
    fn structural_event_predicate_matches_full_pb_and_preserves_unknown_json() {
        let filter_pb = filter_pb::BooleanTransactionFilter {
            filter: Some(filter_pb::boolean_transaction_filter::Filter::ApiFilter(
                filter_pb::ApiFilter {
                    filter: Some(filter_pb::api_filter::Filter::EventFilter(
                        filter_pb::EventFilter {
                            struct_type: Some(filter_pb::MoveStructTagFilter {
                                address: Some("0x01".into()),
                                module: Some("pool".into()),
                                name: Some("Swap".into()),
                            }),
                            data_substring_filter: None,
                        },
                    )),
                },
            )),
        };
        let full = BooleanTransactionFilter::new_from_proto(filter_pb.clone(), None).unwrap();
        let raw = RawFilter::compile(filter_pb);
        for module in ["pool", "other"] {
            let txn = pb::Transaction {
                txn_data: Some(pb::transaction::TxnData::User(pb::UserTransaction {
                    events: vec![pb::Event {
                        r#type: Some(pb::MoveType {
                            content: Some(pb::move_type::Content::Struct(pb::MoveStructTag {
                                address: "0x1".into(),
                                module: module.into(),
                                name: "Swap".into(),
                                generic_type_params: vec![],
                            })),
                            ..Default::default()
                        }),
                        data: "{\"amount\":\"10\"}".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            };
            assert_eq!(
                raw.evaluate_projection(&txn),
                Decision::from_bool(full.matches(&txn))
            );
        }
    }

    #[test]
    fn confirmed_decisions_skip_final_filter_unknown_runs_it_once() {
        for (decision, expected_calls) in [
            (Decision::Match, 0),
            (Decision::NoMatch, 0),
            (Decision::Unknown, 1),
        ] {
            for fallback_result in [true, false] {
                let mut calls = 0;
                let result = decision.resolve(|| {
                    calls += 1;
                    fallback_result
                });
                assert_eq!(calls, expected_calls);
                assert_eq!(
                    result,
                    match decision {
                        Decision::Match => true,
                        Decision::NoMatch => false,
                        Decision::Unknown => fallback_result,
                    }
                );
            }
        }
    }

    #[test]
    fn decibel_entry_function_is_rejected_before_conversion() {
        use aptos_types::transaction::{EntryFunction, TransactionPayload};
        use move_core_types::{
            account_address::AccountAddress, identifier::Identifier, language_storage::ModuleId,
        };
        let address = "0x50ead22afd6ffd9769e3b3d6e0e64a2a350d68e8b102c4e72e33d0b8cfdfdb06";
        let proto = filter_pb::BooleanTransactionFilter {
            filter: Some(filter_pb::boolean_transaction_filter::Filter::LogicalNot(
                Box::new(filter_pb::BooleanTransactionFilter {
                    filter: Some(filter_pb::boolean_transaction_filter::Filter::ApiFilter(
                        filter_pb::ApiFilter {
                            filter: Some(filter_pb::api_filter::Filter::UserTransactionFilter(
                                filter_pb::UserTransactionFilter {
                                    sender: None,
                                    payload_filter: Some(filter_pb::UserTransactionPayloadFilter {
                                        entry_function_filter: Some(
                                            filter_pb::EntryFunctionFilter {
                                                address: Some(address.into()),
                                                module_name: None,
                                                function: None,
                                            },
                                        ),
                                    }),
                                },
                            )),
                        },
                    )),
                }),
            )),
        };
        let full = BooleanTransactionFilter::new_from_proto(proto.clone(), None).unwrap();
        let raw = RawFilter::compile(proto);
        for (entry_address, expected) in [(address, Decision::NoMatch), ("0x1", Decision::Match)] {
            let entry = TransactionPayload::EntryFunction(EntryFunction::new(
                ModuleId::new(
                    AccountAddress::from_hex_literal(entry_address).unwrap(),
                    Identifier::new("market").unwrap(),
                ),
                Identifier::new("trade").unwrap(),
                vec![],
                vec![],
            ));
            let txn = pb::Transaction {
                txn_data: Some(pb::transaction::TxnData::User(pb::UserTransaction {
                    request: Some(pb::UserTransactionRequest {
                        sender: "0x1".into(),
                        payload: raw_entry_payload(&entry),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            assert_eq!(raw.evaluate_with_payload(&txn, true), expected);
            assert_eq!(expected, Decision::from_bool(full.matches(&txn)));
            assert_eq!(raw.evaluate_with_payload(&txn, false), Decision::Unknown);
        }
    }
}
