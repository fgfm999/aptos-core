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

pub(crate) enum RawFilter {
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Sender(AccountAddress),
    EntryFunction(NamePattern),
    Event(NamePattern, Option<TypeTag>),
}
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

    /// Entry matching describes the outer executable, not its internal Move calls.
    /// Scripts/unavailable executables do not match EntryFunction; NOT negates that result.
    /// Event leaves require one event to satisfy every supplied field on that leaf.
    pub(crate) fn matches(
        &self,
        transaction: &SignedTransaction,
        output: &TransactionOutput,
    ) -> bool {
        match self {
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
}
