# BCS decoder reference data

`place-order-8094725931.bcs` is the complete REST `TransactionData::OnChain`
envelope for mainnet version 8094725931, fetched from
`http://100.104.174.67:8080/v1/transactions/by_version/8094725931` with
`Accept: application/x-bcs` on 2026-11-18 (Asia/Shanghai).

`place-order-8094725931.expected.json` contains the transaction identity, the two
OrderEvent values, and the two ObjectCore writes from the JSON representation of
that same transaction. Tests compare decoded BCS values against these independent
REST expectations. The original BCS includes grouped writes; it is not a synthetic
TransactionOutput roundtrip fixture.

`decibel-layouts.json` contains the relevant ABI structs/enums fetched from the
REST account module endpoints for Decibel's `market_types` and
`0x5::order_book_types` during the same session. The decoder uses these field and
variant orders. These are captured layouts, not a guarantee that future contract
upgrades preserve them.
