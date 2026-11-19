// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Probe predicates over borrowed native values. No PB projection, state reads, layout
//! resolution or Move body decoding occurs here. Configuration is compiled once; matching
//! borrows the transaction and output fields.
use anyhow::{ensure, Context, Result};
use aptos_config::config::ExecutionProbeFilter;
use aptos_types::transaction::{SignedTransaction, TransactionExecutableRef, TransactionOutput};
use move_core_types::{
    account_address::AccountAddress,
    identifier::{IdentStr, Identifier},
    language_storage::TypeTag,
};

#[derive(Clone)]
pub(crate) enum RawFilter {
    Constant(bool),
    /// Indexer UserTransactionFilter treats an absent entry payload as unconstrained.
    UserFunction(NamePattern),
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Sender(AccountAddress),
    EntryFunction(NamePattern),
    Event(NamePattern, Option<TypeTag>),
}
#[derive(Clone)]
pub(crate) struct NamePattern {
    address: Option<AccountAddress>,
    module: Option<Identifier>,
    name: Option<Identifier>,
}
impl NamePattern {
    fn compile(
        address: &Option<String>,
        module: &Option<String>,
        name: &Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            address: address
                .as_deref()
                .map(str::parse)
                .transpose()
                .context("filter address")?,
            module: module
                .as_deref()
                .map(Identifier::new)
                .transpose()
                .context("filter module")?,
            name: name
                .as_deref()
                .map(Identifier::new)
                .transpose()
                .context("filter function/event name")?,
        })
    }

    fn is_empty(&self) -> bool {
        self.address.is_none() && self.module.is_none() && self.name.is_none()
    }

    fn matches(&self, address: &AccountAddress, module: &IdentStr, name: &IdentStr) -> bool {
        self.address.as_ref().is_none_or(|a| a == address)
            && self
                .module
                .as_ref()
                .is_none_or(|m| m.as_ident_str() == module)
            && self.name.as_ref().is_none_or(|n| n.as_ident_str() == name)
    }
}
impl RawFilter {
    pub(crate) fn compile(config: &ExecutionProbeFilter) -> Result<Self> {
        Ok(match config {
            ExecutionProbeFilter::And { filters } => {
                ensure!(!filters.is_empty(), "probe AND filter must not be empty");
                Self::And(filters.iter().map(Self::compile).collect::<Result<_>>()?)
            },
            ExecutionProbeFilter::Or { filters } => {
                ensure!(!filters.is_empty(), "probe OR filter must not be empty");
                Self::Or(filters.iter().map(Self::compile).collect::<Result<_>>()?)
            },
            ExecutionProbeFilter::Not { filter } => Self::Not(Box::new(Self::compile(filter)?)),
            ExecutionProbeFilter::Sender { address } => {
                Self::Sender(address.parse().context("filter sender")?)
            },
            ExecutionProbeFilter::EntryFunction {
                address,
                module,
                function,
            } => {
                let pattern = NamePattern::compile(address, module, function)?;
                ensure!(
                    !pattern.is_empty(),
                    "probe entry_function filter needs at least one field"
                );
                Self::EntryFunction(pattern)
            },
            ExecutionProbeFilter::Event {
                address,
                module,
                name,
                type_tag,
            } => {
                let pattern = NamePattern::compile(address, module, name)?;
                let tag = type_tag
                    .as_deref()
                    .map(str::parse)
                    .transpose()
                    .context("filter event type_tag")?;
                ensure!(
                    !pattern.is_empty() || tag.is_some(),
                    "probe event filter needs at least one field"
                );
                Self::Event(pattern, tag)
            },
        })
    }

    /// Compile the Indexer config dialect once, before any transaction serialization.
    /// Only predicates available from native fields are accepted. JSON event-data
    /// substring predicates require Move layout conversion and are rejected explicitly.
    pub(crate) fn compile_subscription(json: &str) -> Result<(Self, String)> {
        use aptos_transaction_filter::{BooleanTransactionFilter, Filterable};
        ensure!(
            json.len() <= 16 * 1024,
            "subscription transaction_filter exceeds 16 KiB"
        );
        let config: BooleanTransactionFilter =
            serde_json::from_str(json).context("subscription transaction_filter JSON")?;
        config
            .is_valid()
            .map_err(|e| anyhow::anyhow!("invalid subscription transaction_filter: {e}"))?;
        let canonical = serde_json::to_string(&config)?;
        let mut remaining = 256;
        Ok((
            Self::compile_indexer(config.into_proto(), 0, &mut remaining)?,
            canonical,
        ))
    }

    fn compile_indexer(
        filter: aptos_protos::indexer::v1::BooleanTransactionFilter,
        depth: usize,
        remaining: &mut usize,
    ) -> Result<Self> {
        use aptos_protos::indexer::v1::{
            api_filter::Filter as Api, boolean_transaction_filter::Filter,
        };
        ensure!(
            depth <= 32 && *remaining > 0,
            "subscription transaction_filter exceeds depth/node limits"
        );
        *remaining -= 1;
        Ok(match filter.filter.context("missing filter expression")? {
            Filter::LogicalAnd(f) => Self::And(
                f.filters
                    .into_iter()
                    .map(|f| Self::compile_indexer(f, depth + 1, remaining))
                    .collect::<Result<_>>()?,
            ),
            Filter::LogicalOr(f) => Self::Or(
                f.filters
                    .into_iter()
                    .map(|f| Self::compile_indexer(f, depth + 1, remaining))
                    .collect::<Result<_>>()?,
            ),
            Filter::LogicalNot(f) => {
                Self::Not(Box::new(Self::compile_indexer(*f, depth + 1, remaining)?))
            },
            Filter::ApiFilter(f) => match f.filter.context("missing API predicate")? {
                Api::TransactionRootFilter(f) => {
                    // All probe candidates are already successful user transactions.
                    let user =
                        aptos_protos::transaction::v1::transaction::TransactionType::User as i32;
                    Self::Constant(
                        f.success.unwrap_or(true) && f.transaction_type.is_none_or(|t| t == user),
                    )
                },
                Api::UserTransactionFilter(f) => {
                    let mut conditions = Vec::new();
                    if let Some(sender) = f.sender {
                        conditions.push(Self::Sender(sender.parse().context("filter sender")?));
                    }
                    if let Some(payload) = f.payload_filter {
                        let function = payload
                            .entry_function_filter
                            .context("payload filter needs function")?;
                        let pattern = NamePattern::compile(
                            &function.address,
                            &function.module_name,
                            &function.function,
                        )?;
                        ensure!(!pattern.is_empty(), "entry function filter needs a field");
                        conditions.push(Self::UserFunction(pattern));
                    }
                    Self::And(conditions)
                },
                Api::EventFilter(f) => {
                    ensure!(f.data_substring_filter.is_none(), "probe transaction_filter does not support EventFilter.data_substring_filter; use struct_type without body decoding");
                    let tag = f.struct_type.context("event filter needs struct_type")?;
                    let pattern = NamePattern::compile(&tag.address, &tag.module, &tag.name)?;
                    ensure!(!pattern.is_empty(), "event struct_type needs a field");
                    Self::Event(pattern, None)
                },
            },
        })
    }

    /// Entry matching describes the outer executable, not its internal Move calls.
    /// Scripts/unavailable executables do not match EntryFunction; NOT negates that result.
    /// Event leaves require one event to satisfy every supplied field on that leaf.
    pub(crate) fn matches(
        &self,
        transaction: &SignedTransaction,
        output: &TransactionOutput,
    ) -> bool {
        match self {
            Self::Constant(value) => *value,
            Self::UserFunction(pattern) => match transaction.executable_ref() {
                Ok(TransactionExecutableRef::EntryFunction(entry)) => pattern.matches(
                    entry.module().address(),
                    entry.module().name(),
                    entry.function(),
                ),
                _ => true,
            },
            Self::And(filters) => filters.iter().all(|f| f.matches(transaction, output)),
            Self::Or(filters) => filters.iter().any(|f| f.matches(transaction, output)),
            Self::Not(filter) => !filter.matches(transaction, output),
            Self::Sender(address) => transaction.sender() == *address,
            Self::EntryFunction(pattern) => match transaction.executable_ref() {
                Ok(TransactionExecutableRef::EntryFunction(entry)) => pattern.matches(
                    entry.module().address(),
                    entry.module().name(),
                    entry.function(),
                ),
                Ok(
                    TransactionExecutableRef::Script(_)
                    | TransactionExecutableRef::Empty
                    | TransactionExecutableRef::Encrypted,
                )
                | Err(_) => false,
            },
            Self::Event(pattern, exact) => output.events().iter().any(|event| {
                let tag = event.type_tag();
                if exact.as_ref().is_some_and(|expected| expected != tag) {
                    return false;
                }
                if pattern.is_empty() {
                    return true;
                }
                match tag {
                    TypeTag::Struct(tag) => pattern.matches(&tag.address, &tag.module, &tag.name),
                    _ => false,
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_types::{
        contract_event::ContractEvent,
        transaction::{Transaction, TransactionAuxiliaryData, TransactionStatus},
    };
    use serde_json::{json, Value};
    fn fixture() -> (Transaction, TransactionOutput) {
        let aptos_api_types::TransactionData::OnChain(data) =
            bcs::from_bytes(include_bytes!("../tests/data/place-order-8094725931.bcs")).unwrap()
        else {
            panic!("fixture")
        };
        let output = TransactionOutput::new(
            data.changes,
            data.events,
            data.info.gas_used(),
            TransactionStatus::Keep(data.info.status().clone()),
            TransactionAuxiliaryData::None,
        );
        (data.transaction, output)
    }
    fn compile(value: Value) -> Result<RawFilter> {
        RawFilter::compile(&serde_json::from_value(value)?)
    }
    fn matches(value: Value, txn: &Transaction, output: &TransactionOutput) -> bool {
        compile(value)
            .unwrap()
            .matches(txn.try_as_signed_user_txn().unwrap(), output)
    }
    #[test]
    fn real_entry_sender_and_boolean_filters() {
        let (txn, output) = fixture();
        let sender = json!({"type":"sender","address":txn.try_as_signed_user_txn().unwrap().sender().to_standard_string()});
        let entry = json!({"type":"entry_function","address":crate::decibel::DECIBEL,"module":"dex_accounts_entry","function":"place_order_to_subaccount"});
        let event = json!({"type":"event","type_tag":format!("{}::market_types::OrderEvent",crate::decibel::DECIBEL)});
        assert!(matches(
            json!({"type":"and","filters":[sender.clone(),entry.clone(),event.clone()]}),
            &txn,
            &output
        ));
        assert!(!matches(
            json!({"type":"not","filter":entry}),
            &txn,
            &output
        ));
        let wrong = json!({"type":"sender","address":"0x2"});
        assert!(!matches(wrong.clone(), &txn, &output));
        assert!(matches(
            json!({"type":"or","filters":[wrong.clone(),sender]}),
            &txn,
            &output
        ));
        assert!(!matches(
            json!({"type":"and","filters":[wrong,event]}),
            &txn,
            &output
        ));
        assert!(!matches(
            json!({"type":"entry_function","function":"another_function"}),
            &txn,
            &output
        ));
        assert!(matches(
            json!({"type":"entry_function","function":"place_order_to_subaccount"}),
            &txn,
            &output
        ));
    }
    fn event_output(tag: &str) -> TransactionOutput {
        let event = ContractEvent::new_v2(tag.parse().unwrap(), vec![0xFF]).unwrap();
        TransactionOutput::new(
            Default::default(),
            vec![event],
            0,
            TransactionStatus::Keep(aptos_types::transaction::ExecutionStatus::Success),
            TransactionAuxiliaryData::None,
        )
    }
    #[test]
    fn type_matching_does_not_decode_invalid_body_and_normalizes_addresses() {
        let (txn, _) = fixture();
        let output = event_output("0x1::example::E<u64>");
        for address in ["0x1".to_string(), format!("0x{:064x}", 1)] {
            assert!(matches(
                json!({"type":"event","address":address,"module":"example","name":"E"}),
                &txn,
                &output
            ));
        }
        assert!(!matches(
            json!({"type":"event","module":"Example","name":"E"}),
            &txn,
            &output
        ));
        assert!(!matches(
            json!({"type":"event","type_tag":"0x1::example::E<u8>"}),
            &txn,
            &output
        ));
        assert!(matches(
            json!({"type":"event","type_tag":"0x1::example::E<u64>"}),
            &txn,
            &output
        ));
    }
    #[test]
    fn event_fields_must_match_the_same_event() {
        let (txn, _) = fixture();
        let events = vec![
            ContractEvent::new_v2("0x1::a::First".parse().unwrap(), vec![]).unwrap(),
            ContractEvent::new_v2("0x2::b::Second".parse().unwrap(), vec![]).unwrap(),
        ];
        let output = TransactionOutput::new(
            Default::default(),
            events,
            0,
            TransactionStatus::Keep(aptos_types::transaction::ExecutionStatus::Success),
            TransactionAuxiliaryData::None,
        );
        assert!(!matches(
            json!({"type":"event","address":"0x1","name":"Second"}),
            &txn,
            &output
        ));
        assert!(matches(
            json!({"type":"and","filters":[{"type":"event","name":"First"},{"type":"event","name":"Second"}]}),
            &txn,
            &output
        ));
    }
    #[test]
    fn invalid_filters_fail_instead_of_silently_widening_selection() {
        for value in [
            json!({"type":"and","filters":[]}),
            json!({"type":"or","filters":[]}),
            json!({"type":"entry_function"}),
            json!({"type":"event"}),
            json!({"type":"sender","address":"bad address"}),
            json!({"type":"entry_function","module":"bad module"}),
            json!({"type":"event","type_tag":"not a type"}),
            json!({"type":"event","data_substring":"foo"}),
            json!({"type":"unknown"}),
        ] {
            assert!(compile(value.clone()).is_err(), "{value}");
        }
    }
    fn subscription_projection(
        txn: &SignedTransaction,
        output: &TransactionOutput,
    ) -> aptos_protos::transaction::v1::Transaction {
        use aptos_protos::transaction::v1 as pb;
        let payload = match txn.executable_ref() {
            Ok(TransactionExecutableRef::EntryFunction(entry)) => Some(pb::TransactionPayload {
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
            }),
            _ => None,
        };
        let events = output
            .events()
            .iter()
            .map(|event| {
                let kind = match event.type_tag() {
                    TypeTag::Struct(tag) => Some(pb::MoveType {
                        content: Some(pb::move_type::Content::Struct(pb::MoveStructTag {
                            address: tag.address.to_hex_literal(),
                            module: tag.module.to_string(),
                            name: tag.name.to_string(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }),
                    _ => None,
                };
                pb::Event {
                    r#type: kind,
                    ..Default::default()
                }
            })
            .collect();
        pb::Transaction {
            r#type: pb::transaction::TransactionType::User as i32,
            info: Some(pb::TransactionInfo {
                success: true,
                ..Default::default()
            }),
            txn_data: Some(pb::transaction::TxnData::User(pb::UserTransaction {
                request: Some(pb::UserTransactionRequest {
                    sender: txn.sender().to_hex_literal(),
                    payload,
                    ..Default::default()
                }),
                events,
            })),
            ..Default::default()
        }
    }

    #[test]
    fn subscription_dialect_matches_indexer_predicates_including_missing_payload_and_not() {
        use aptos_transaction_filter::{BooleanTransactionFilter, Filterable};
        let (entry, output) = fixture();
        let script = crate::tests::user();
        let event = json!({"type":"EventFilter","struct_type":{"address":crate::decibel::DECIBEL,"module":"market_types","name":"OrderEvent"}});
        let function = json!({"type":"UserTransactionFilter","payload":{"function":{"address":crate::decibel::DECIBEL}}});
        let sender = json!({"type":"UserTransactionFilter","sender":"0x1"});
        let root = json!({"type":"TransactionRootFilter","success":true,"txn_type":4});
        let expressions = vec![
            root.clone(),
            function.clone(),
            event.clone(),
            sender.clone(),
            json!({"not":function.clone()}),
            json!({"and":[root.clone(),{"not":function.clone()}]}),
            json!({"or":[function.clone(),sender]}),
            json!({"not":{"or":[event,function]}}),
            json!({"type":"TransactionRootFilter","success":false}),
            json!({"type":"TransactionRootFilter","txn_type":1}),
            json!({"not":{"type":"TransactionRootFilter","success":false}}),
            json!({"and":[]}),
            json!({"or":[]}),
        ];
        for transaction in [&entry, &script] {
            let user = transaction.try_as_signed_user_txn().unwrap();
            let projection = subscription_projection(user, &output);
            for expression in &expressions {
                let config: BooleanTransactionFilter =
                    serde_json::from_value(expression.clone()).unwrap();
                config.is_valid().unwrap();
                let (filter, _) = RawFilter::compile_subscription(&expression.to_string()).unwrap();
                assert_eq!(
                    filter.matches(user, &output),
                    config.matches(&projection),
                    "{expression}"
                );
            }
        }
    }

    #[test]
    fn subscription_complexity_and_json_data_predicates_fail_explicitly() {
        let mut deep = json!({"type":"TransactionRootFilter","success":true});
        for _ in 0..34 {
            deep = json!({"not":deep});
        }
        assert!(RawFilter::compile_subscription(&deep.to_string()).is_err());
        let wide = json!({"and":vec![json!({"type":"TransactionRootFilter","success":true});260]});
        assert!(RawFilter::compile_subscription(&wide.to_string()).is_err());
        assert!(RawFilter::compile_subscription(
            r#"{"type":"EventFilter","data_substring_filter":"x"}"#
        )
        .is_err());
        assert!(RawFilter::compile_subscription(
            r#"{"type":"UserTransactionFilter","payload":{"function":{"address":"invalid"}}}"#
        )
        .is_err());
    }
}
