// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::wire::{ExecutedTransactionData, TransactionId};
use anyhow::{bail, Result};
use aptos_api_types::{MoveConverter, WriteSetChange};
use aptos_executor_types::execution_output::ExecutionOutput;
use aptos_indexer_grpc_fullnode::convert::{convert_events, convert_write_set_changes};
use aptos_storage_interface::{
    state_store::state_view::{
        cached_state_view::CachedStateView, db_state_view::DbStateViewAtVersion,
    },
    DbReader,
};
use aptos_types::{
    indexer::indexer_db_reader::IndexerReader,
    state_store::{StateView, StateViewId},
    transaction::TransactionOutput,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct DataContext {
    pub db: Arc<dyn DbReader>,
    pub indexer_reader: Option<Arc<dyn IndexerReader>>,
}

impl DataContext {
    pub(crate) fn convert(
        &self,
        output: &ExecutionOutput,
        ids: &[TransactionId],
    ) -> Vec<ExecutedTransactionData> {
        match self.try_convert(output, ids) {
            Ok(data) => data,
            Err(error) => ids
                .iter()
                .map(|id| {
                    failed(
                        id,
                        &output.to_commit.transaction_outputs[id.transaction_index as usize],
                        &error.to_string(),
                    )
                })
                .collect(),
        }
    }

    fn try_convert(
        &self,
        output: &ExecutionOutput,
        ids: &[TransactionId],
    ) -> Result<Vec<ExecutedTransactionData>> {
        let (hot, persisted) = self.db.get_persisted_state()?;
        let state = output.result_state.latest();
        if state.is_descendant_of(&persisted) {
            let view = CachedStateView::new_impl(
                StateViewId::Miscellaneous,
                self.db.clone(),
                hot,
                persisted,
                state.clone(),
            );
            Ok(self.convert_with_view(&view, output, ids))
        } else if persisted.is_descendant_of(state) {
            // The worker can run after this block has persisted. Pin the original
            // version rather than decoding with newer module layouts.
            let view = self.db.state_view_at_version(state.version())?;
            Ok(self.convert_with_view(&view, output, ids))
        } else {
            bail!("executed state is not on the persisted state's branch")
        }
    }

    fn convert_with_view<S: StateView>(
        &self,
        view: &S,
        output: &ExecutionOutput,
        ids: &[TransactionId],
    ) -> Vec<ExecutedTransactionData> {
        let converter = MoveConverter::new(view, self.db.clone(), self.indexer_reader.clone());
        ids.iter()
            .map(|id| {
                convert_one(
                    &converter,
                    id,
                    &output.to_commit.transaction_outputs[id.transaction_index as usize],
                )
            })
            .collect()
    }
}

fn convert_one<S: StateView>(
    converter: &MoveConverter<'_, S>,
    id: &TransactionId,
    output: &TransactionOutput,
) -> ExecutedTransactionData {
    let mut data = ExecutedTransactionData {
        hash: id.hash.clone(),
        transaction_index: id.transaction_index,
        data_complete: true,
        ..Default::default()
    };
    let mut errors = Vec::new();
    match converter.try_into_events(output.events()) {
        Ok(events) => data.events = convert_events(&events),
        Err(e) => errors.push(format!("events: {e}")),
    }
    for (key, op) in output.write_set().write_op_iter() {
        match converter.try_into_write_set_changes(key.clone(), op.clone()) {
            Ok(changes) => {
                for change in &changes {
                    if matches!(change, WriteSetChange::WriteTableItem(item) if item.data.is_none())
                        || matches!(change, WriteSetChange::DeleteTableItem(item) if item.data.is_none())
                    {
                        errors.push("table-info unavailable; decoded table data omitted".into());
                    }
                }
                data.write_set_changes
                    .extend(convert_write_set_changes(&changes));
            },
            Err(e) => errors.push(format!("write-set change: {e}")),
        }
    }
    if !errors.is_empty() {
        mark_incomplete(&mut data, output, &errors.join("; "));
    }
    data
}

fn failed(id: &TransactionId, output: &TransactionOutput, reason: &str) -> ExecutedTransactionData {
    let mut data = ExecutedTransactionData {
        hash: id.hash.clone(),
        transaction_index: id.transaction_index,
        ..Default::default()
    };
    mark_incomplete(&mut data, output, reason);
    data
}

fn mark_incomplete(data: &mut ExecutedTransactionData, output: &TransactionOutput, reason: &str) {
    data.data_complete = false;
    data.conversion_error = reason.chars().take(2048).collect();
    match (
        bcs::to_bytes(output.events()),
        bcs::to_bytes(output.write_set()),
    ) {
        (Ok(events), Ok(changes)) => {
            data.raw_events_bcs = events;
            data.raw_write_set_bcs = changes;
        },
        _ => data
            .conversion_error
            .push_str("; raw BCS serialization failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct MockDbReaderWriter;
    impl DbReader for MockDbReaderWriter {}
    use aptos_protos::transaction::v1::write_set_change::Change;
    use aptos_types::{
        contract_event::ContractEvent,
        state_store::{
            state_key::StateKey, state_value::StateValue, table::TableHandle, MockStateView,
        },
        transaction::{ExecutionStatus, TransactionAuxiliaryData, TransactionStatus},
        write_set::{WriteOp, WriteSetMut},
    };
    use move_core_types::{
        account_address::AccountAddress,
        identifier::Identifier,
        language_storage::{StructTag, TypeTag},
    };
    use std::collections::HashMap;

    #[test]
    fn resource_and_event_json_use_the_same_indexer_types() {
        let module = move_binary_format::file_format::basic_test_module();
        let module_id = module.self_id();
        let mut bytes = Vec::new();
        module.serialize(&mut bytes).unwrap();
        let view = MockStateView::new(HashMap::from([(
            StateKey::module_id(&module_id),
            StateValue::new_legacy(bytes.into()),
        )]));
        let db: Arc<dyn DbReader> = Arc::new(MockDbReaderWriter);
        let converter = MoveConverter::new(&view, db, None);
        let tag = StructTag {
            address: *module_id.address(),
            module: module_id.name().to_owned(),
            name: Identifier::new("Bar").unwrap(),
            type_args: vec![],
        };
        let raw = bcs::to_bytes(&42u64).unwrap();
        let writes = WriteSetMut::new(vec![(
            StateKey::resource(&AccountAddress::ONE, &tag).unwrap(),
            WriteOp::legacy_modification(raw.clone().into()),
        )])
        .freeze()
        .unwrap();
        let event = ContractEvent::new_v2(TypeTag::Struct(Box::new(tag)), raw).unwrap();
        let output = TransactionOutput::new(
            writes,
            vec![event],
            0,
            TransactionStatus::Keep(ExecutionStatus::Success),
            TransactionAuxiliaryData::None,
        );
        let id = TransactionId {
            hash: vec![1; 32],
            transaction_index: 3,
        };
        let data = convert_one(&converter, &id, &output);
        assert!(data.data_complete, "{}", data.conversion_error);
        assert_eq!(data.hash, id.hash);
        assert_eq!(data.transaction_index, 3);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&data.events[0].data).unwrap(),
            serde_json::json!({"x":"42"})
        );
        match data.write_set_changes[0].change.as_ref().unwrap() {
            Change::WriteResource(resource) => assert_eq!(
                serde_json::from_str::<serde_json::Value>(&resource.data).unwrap(),
                serde_json::json!({"x":"42"})
            ),
            other => panic!("unexpected change {other:?}"),
        }
        assert!(data.raw_events_bcs.is_empty());
    }
    #[test]
    fn missing_table_info_is_explicit_and_preserves_raw_output() {
        let view = MockStateView::<StateKey>::empty();
        let converter = MoveConverter::new(&view, Arc::new(MockDbReaderWriter), None);
        let writes = WriteSetMut::new(vec![(
            StateKey::table_item(&TableHandle(AccountAddress::ONE), &[1]),
            WriteOp::legacy_modification(vec![2].into()),
        )])
        .freeze()
        .unwrap();
        let output = TransactionOutput::new_success_with_write_set(writes);
        let data = convert_one(
            &converter,
            &TransactionId {
                hash: vec![2; 32],
                transaction_index: 0,
            },
            &output,
        );
        assert!(!data.data_complete);
        assert!(data.conversion_error.contains("table-info"));
        assert_eq!(
            data.raw_write_set_bcs,
            bcs::to_bytes(output.write_set()).unwrap()
        );
        assert_eq!(data.write_set_changes.len(), 1);
        assert!(
            matches!(&data.write_set_changes[0].change,Some(Change::WriteTableItem(item)) if item.data.is_none())
        );
    }
    #[test]
    fn undecodable_event_never_claims_complete() {
        let view = MockStateView::<StateKey>::empty();
        let converter = MoveConverter::new(&view, Arc::new(MockDbReaderWriter), None);
        let event = ContractEvent::new_v2(TypeTag::U64, vec![1]).unwrap();
        let output = TransactionOutput::new(
            Default::default(),
            vec![event],
            0,
            TransactionStatus::Keep(ExecutionStatus::Success),
            TransactionAuxiliaryData::None,
        );
        let data = convert_one(
            &converter,
            &TransactionId {
                hash: vec![2; 32],
                transaction_index: 0,
            },
            &output,
        );
        assert!(!data.data_complete);
        assert!(!data.raw_events_bcs.is_empty());
        assert!(data.events.is_empty());
    }
}
