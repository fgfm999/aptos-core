// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! Selective decoding of execution output, with no state resolver or JSON conversion on the node.
//! OrderEvent layouts are pinned to the live market_types/order_book_types ABIs captured in tests/data.
use anyhow::{ensure, Context, Result};
use aptos_types::{
    access_path::Path,
    account_config::{ObjectCoreResource, ObjectGroupResource},
    state_store::state_key::inner::StateKeyInner,
    transaction::{Transaction, TransactionExecutableRef, TransactionOutput},
};
use move_core_types::{
    account_address::AccountAddress, language_storage::TypeTag, move_resource::MoveStructType,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const DECIBEL: &str = "0x50ead22afd6ffd9769e3b3d6e0e64a2a350d68e8b102c4e72e33d0b8cfdfdb06";
pub const PLACE_ORDER: &str = "place_order_to_subaccount";

// Move's Option is a struct containing vector<T>. Check its cardinality explicitly.
#[derive(Debug, Deserialize)]
struct MoveOption<T> {
    vec: Vec<T>,
}
impl<T> MoveOption<T> {
    fn json(&self, convert: impl Fn(&T) -> Value) -> Result<Value> {
        ensure!(self.vec.len() <= 1, "invalid Move Option length");
        Ok(json!({"vec": self.vec.iter().map(convert).collect::<Vec<_>>()}))
    }
}

// Declaration order is the BCS enum discriminant. Do not reorder variants or fields.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum OrderStatus {
    Open,
    Filled,
    Cancelled,
    Rejected,
    SizeReduced,
    Acknowledged,
}
#[derive(Debug, Deserialize)]
enum OrderCancellationReason {
    PostOnlyViolation,
    IOCViolation,
    PositionUpdateViolation,
    ReduceOnlyViolation,
    ClearinghouseSettleViolation,
    MaxFillLimitViolation,
    DuplicateClientOrderIdViolation,
    OrderPreCancelled,
    PlaceMakerOrderViolation,
    DeadMansSwitchExpired,
    DisallowedSelfTrading,
    OrderCancelledByUser,
    OrderCancelledBySystem,
    OrderCancelledBySystemDueToError,
    ClearinghouseStoppedMatching,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum TimeInForce {
    Gtc,
    PostOnly,
    Ioc,
}
#[derive(Debug, Deserialize)]
enum TriggerCondition {
    PriceMoveAbove(u64),
    PriceMoveBelow(u64),
    TimeBased(u64),
}
impl TriggerCondition {
    fn json(&self) -> Value {
        let (name, value) = match self {
            Self::PriceMoveAbove(n) => ("PriceMoveAbove", n),
            Self::PriceMoveBelow(n) => ("PriceMoveBelow", n),
            Self::TimeBased(n) => ("TimeBased", n),
        };
        json!({"__variant__": name, "_0": value.to_string()})
    }
}
#[derive(Debug, Deserialize)]
enum OrderEvent {
    V1 {
        parent: AccountAddress,
        market: AccountAddress,
        order_id: u128,
        client_order_id: MoveOption<String>,
        user: AccountAddress,
        orig_size: u64,
        remaining_size: u64,
        size_delta: u64,
        price: u64,
        is_bid: bool,
        is_taker: bool,
        status: OrderStatus,
        details: String,
        metadata_bytes: Vec<u8>,
        time_in_force: TimeInForce,
        trigger_condition: MoveOption<TriggerCondition>,
        cancellation_reason: MoveOption<OrderCancellationReason>,
    },
}

/// Decode just one known event body. Unknown variants/layouts return an error.
pub fn decode_order_event(bytes: &[u8]) -> Result<Value> {
    let event: OrderEvent = bcs::from_bytes(bytes).context("OrderEvent layout mismatch")?;
    match event {
        OrderEvent::V1 {
            parent,
            market,
            order_id,
            client_order_id,
            user,
            orig_size,
            remaining_size,
            size_delta,
            price,
            is_bid,
            is_taker,
            status,
            details,
            metadata_bytes,
            time_in_force,
            trigger_condition,
            cancellation_reason,
        } => Ok(json!({
            "__variant__": "V1", "parent": parent.to_standard_string(), "market": market.to_standard_string(),
            "order_id": order_id.to_string(), "client_order_id": client_order_id.json(|s| json!(s))?,
            "user": user.to_standard_string(), "orig_size": orig_size.to_string(),
            "remaining_size": remaining_size.to_string(), "size_delta": size_delta.to_string(),
            "price": price.to_string(), "is_bid": is_bid, "is_taker": is_taker,
            "status": {"__variant__": status}, "details": details,
            "metadata_bytes": format!("0x{}", hex::encode(metadata_bytes)),
            "time_in_force": {"__variant__": time_in_force},
            "trigger_condition": trigger_condition.json(TriggerCondition::json)?,
            "cancellation_reason": cancellation_reason.json(|r| json!({"__variant__": format!("{r:?}")}))?,
        })),
    }
}

pub fn decode_object_core(bytes: &[u8]) -> Result<Value> {
    let core: ObjectCoreResource = bcs::from_bytes(bytes).context("ObjectCore layout mismatch")?;
    let handle = &core.transfer_events;
    Ok(json!({
        "guid_creation_num": core.guid_creation_num.to_string(),
        "owner": core.owner.to_standard_string(), "allow_ungated_transfer": core.allow_ungated_transfer,
        "transfer_events": {"counter": handle.count().to_string(), "guid": {"id": {
            "addr": handle.key().get_creator_address().to_standard_string(),
            "creation_num": handle.key().get_creation_number().to_string(),
        }}},
    }))
}

pub fn entry_function(transaction: &Transaction) -> Option<String> {
    let transaction = transaction.try_as_signed_user_txn()?;
    match transaction.executable_ref().ok()? {
        TransactionExecutableRef::EntryFunction(entry) => Some(format!(
            "{}::{}::{}",
            entry.module().address().to_standard_string(),
            entry.module().name(),
            entry.function()
        )),
        TransactionExecutableRef::Script(_)
        | TransactionExecutableRef::Empty
        | TransactionExecutableRef::Encrypted => None,
    }
}
pub fn is_place_order(transaction: &Transaction) -> bool {
    entry_function(transaction)
        .is_some_and(|f| f == format!("{DECIBEL}::dex_accounts_entry::{PLACE_ORDER}"))
}

/// Scan type tags before decoding Move bodies; unrelated event/resource layouts are unnecessary.
/// Resource group writes contain a BCS map, while the Indexer flattens that map into WriteResource.
pub fn decode_output(output: &TransactionOutput) -> Result<Value> {
    let mut events = Vec::new();
    let mut writes = Vec::new();
    let decibel = AccountAddress::from_hex_literal(DECIBEL)?;
    for (index, event) in output.events().iter().enumerate() {
        if let TypeTag::Struct(tag) = event.type_tag()
            && tag.address == decibel
            && tag.module.as_str() == "market_types"
            && tag.name.as_str() == "OrderEvent"
            && tag.type_args.is_empty()
        {
            events.push(
                json!({"event_index": index, "type": format!("{DECIBEL}::market_types::OrderEvent"),
                    "data": decode_order_event(event.event_data())?}),
            );
        }
    }
    let core_tag = ObjectCoreResource::struct_tag();
    for (key, op) in output.write_set().write_op_iter() {
        let Some(bytes) = op.bytes() else {
            continue;
        };
        if let StateKeyInner::AccessPath(access) = key.inner() {
            let core = match access.get_path() {
                Path::Resource(tag) if tag == core_tag => Some(decode_object_core(bytes)?),
                Path::ResourceGroup(tag) if tag == ObjectGroupResource::struct_tag() => {
                    let group: ObjectGroupResource =
                        bcs::from_bytes(bytes).context("ObjectGroup layout mismatch")?;
                    group
                        .group
                        .get(&core_tag)
                        .map(|b| decode_object_core(b))
                        .transpose()?
                },
                Path::Resource(_) | Path::ResourceGroup(_) | Path::Code(_) => None,
            };
            if let Some(data) = core {
                writes.push(
                    json!({"type": "write_resource", "address": access.address.to_standard_string(),
                    "data": {"type": "0x1::object::ObjectCore", "data": data}}),
                );
            }
        }
    }
    Ok(json!({"events": events, "write_resource": writes,
        "gas_used": output.gas_used().to_string(), "status": format!("{:?}", output.status())}))
}
