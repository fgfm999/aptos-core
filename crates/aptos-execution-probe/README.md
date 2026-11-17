# Execution probe experiment

This live-only feed publishes identifiers of successful user transactions after
local execution. Observations are **executed**, not committed. It has no replay
or recovery guarantee. The comparison client never drives a trading strategy.

Build the node and client on the host matching the destination architecture:

```sh
make build_node
cargo build --profile performance -p aptos-execution-probe --bin execution-probe-compare
```

Add to the new node's YAML and restart its existing plain-file process:

```yaml
execution_probe:
  enabled: true
  include_data: false
  address: "100.104.174.67:50052"
```

The default is disabled and uses `127.0.0.1:50052`. Only the Observer executor
receives the publisher. Bind to the private interface accessible to the bot;
this experimental listener provides plaintext gRPC without authentication.
Existing RawData, filtering, processed_range and execution settings are unchanged.

Run on the actual bot host, using the same node for all three endpoints:

```sh
./execution-probe-compare \
  --probe http://100.104.174.67:50052 \
  --indexer http://100.104.174.67:50051 \
  --rest http://100.104.174.67:8080 \
  --out probe-run-001
```

The output directory must not already exist. Defaults are 30 seconds warmup,
600 seconds acquisition, and up to 120 seconds drain. The client requests the
Indexer from the head captured after probe Ready, with no server-side filter;
both streams select successful user transactions. `--auth-token` optionally
sets the Indexer Bearer token. Two endpoints must use the same chain and node;
chain IDs are checked, but independent nodes on the same chain cannot be
distinguished automatically.

`matches.csv` records both arrival offsets and signed `delta_ms = indexer - probe`
on the same client monotonic clock. Receive time is captured immediately after
each decoded response, shared by its transactions. `summary.json` reports
nearest-rank quantiles, coverage, unmatched records, duplicates, ambiguities,
queue/sequence loss and writer errors. Cross-block duplicate hashes are excluded
within the 120-second matching window. Reappearance after expiry is a new
observation; it cannot be classified against records no longer retained.

For matched transactions, the CSV also records `txn_timestamp_unix_micros`,
`probe_received_unix_micros`, `indexer_received_unix_micros`, `probe_age_ms`, and
`indexer_age_ms`. Each age is the original client wall-clock receipt time minus
that transaction's block timestamp, obtained from the matching Indexer
`Transaction.timestamp`. The probe receipt is saved immediately; it is not
resampled when the Indexer arrives. This works in identity, JSON, and BCS modes
without changing the node or decoding BCS bodies.

These ages are block-to-receive ages, not submission/mempool latency or latency
from execution_done. They require the client clock to be synchronized with the
chain's timestamp clock. A clock step can affect ages without changing the
monotonic `delta_ms`. A wall-clock age difference is not a substitute for that
delta. Timestamp fields are integers in microseconds; ages are milliseconds.

`summary.json.transaction_time` contains sample counts and P50/P95/P99, mean,
min/max, and negative-sample counts for both ages, using the same matched cohort.
Missing, zero, pre-epoch, or malformed timestamps leave CSV timestamp/age fields
blank and reduce the age sample count; their arrival deltas remain valid.
`transaction_time.measurement_impaired` flags missing/invalid timestamps or
negative ages. The existing top-level `measurement_impaired` continues to describe
arrival-delta quality. Neither flag proves clock synchronization.

Negative differences remain in the distribution. A P95 lead of 5 ms does not
mean that most transactions lead by 5 ms; use `at_least_5ms_fraction` and P05 as
well. The result compares two delivery paths with different message sizes. It
is not an absolute measurement from execution completion, nor a guarantee for
a future feed carrying events or write sets. Server UNIX timestamps are
diagnostic only and are not subtracted across hosts.

The source boundary is after the executor has successfully updated its in-memory
block tree, immediately before returning to the
pipeline's existing `execute_done` marker. Queue admission adds a small amount
of work before that marker; its overhead is measured, not assumed zero. The
worker computes hashes from the retained output, without DB reads or API/PB
transaction conversion. `transaction_index` identifies position in kept output,
including intervening failed kept/system transactions, not an assigned ledger
version. Client pairing uses hashes, never provisional versions.

Ingress retains at most 16 shared block outputs. Full ingress drops observations
instead of waiting; broadcast retains 256 chunks of at most 1,000 identifiers.
A subscriber that falls behind is disconnected with RESOURCE_EXHAUSTED. Metrics
are `execution_probe_events_total{kind=...}` and
`execution_probe_duration_seconds{stage=...}`. No RUST_LOG setting is required.
An enabled listener that cannot bind aborts startup with a clear error. Without
subscribers the executor skips output capture; disabled mode starts no runtime.

Before interpreting results, check `measurement_impaired`, `insufficient_samples`
and both unmatched counts. Startup, window boundaries, speculative results that
never commit and data loss can all produce unmatched observations. The probe
coverage denominator is matched plus probe-only observations; ambiguity and
evictions are separately reported. A disconnect ends the session and saves a
partial summary; reconnect requires a new run. No measurements span restarts.

Run a disabled baseline and enabled experiment under comparable block counts,
gas workload and execution histograms to check observer effects. Then collect
at least ten minutes and 10,000 matches. Deployment and actual bot-host latency
collection require the Linux binary and node restart; local tests do not prove
any latency benefit.

The comparator opens an additional unfiltered Indexer subscription. Keep other
subscriptions constant and record workload/CPU during the experiment: conversion
for this client can itself add Indexer load. First-run numbers reflect that load,
not the production bot's filtered stream in isolation. The source needs a readable
chain-ID resource at startup; enable it on the already-synchronized Observer node.

## Events and write-set changes

Enable the optional background conversion and restart the node:

```yaml
execution_probe:
  enabled: true
  include_data: true
  address: "100.104.174.67:50052"
```

`include_data` defaults to false. Ready and each response advertise the selected
mode. `transactions` remains the original identity list. When enabled,
`transaction_data` has one matching entry per identity, in the same order:

- `hash` and `transaction_index` identify the executed transaction.
- `events` contains `aptos.transaction.v1.Event`, including decoded JSON `data`.
- `write_set_changes` contains `aptos.transaction.v1.WriteSetChange`, including
  decoded resource JSON and table-item data when metadata is available.
- `data_complete` and `conversion_error` report decoding quality.
- On incomplete conversion, `raw_events_bcs` and `raw_write_set_bcs` preserve
  original data for consumers with their own schemas. These fields are absent
  for complete conversions. A raw serialization failure is explicitly reported.

The gRPC API remains `ExecutionProbe/Subscribe`, not `RawData/GetTransactions`.
The bot needs a probe adapter; it can reuse its Event/WriteSetChange parsers.
Never use these messages to advance the committed Indexer cursor. No finalized
ledger version, accumulator root or checkpoint hash is invented.

The worker reuses the same conversion functions as the fullnode Indexer, with
the block's executed state for module layouts and the node's existing shared
IndexerReader for table metadata. If persistence overtakes the queued block,
the resolver uses its exact historical version only after confirming ancestry.
Unrelated branches and decoding failures produce incomplete data, not silently
empty complete results. Newly created table metadata may not yet exist; the
worker does not wait for table-info progress. Preserve this distinction in bot
handling and skip incomplete results unless the adapter can decode the raw data.

Data conversion runs on the probe runtime's blocking worker, never in the
execution thread. Responses are capped at 3 MiB and 1,000 identities; a single
oversized transaction rejects the whole block observation before publication
and increments dropped_blocks/payload_block_errors. Identity-only mode retains
the existing lightweight worker path.

Rebuild the comparison client too. The existing command works unchanged and
measures reception after the larger data response is decoded. Its summary adds
`include_data`, `data_transactions`, `events_received`,
`write_set_changes_received`, `incomplete_data_transactions` and `data_incomplete`.
Incomplete conversions mark the measurement impaired, since those rows cannot
establish delivery latency of complete business data. CSV still contains one row
per paired transaction and does not archive the actual events/changes.

Compare this new run to identity-only mode under comparable block work and
subscription load. The old 34 ms median lead was measured with identifiers;
it does not predict data-mode lead or bot state-application latency.

## BCS execution output

To move Move data decoding to the bot, rebuild the node and both clients from this
branch and select BCS in the node YAML:

```yaml
execution_probe:
  enabled: true
  include_data: true
  data_format: bcs
  address: "127.0.0.1:50052"
```

`data_format` defaults to `json`; `include_data: false` keeps the identity-only
feed. BCS mode uses the same background worker and bounded queues. It serializes
already materialized execution results without reading persisted state, resolving
Move layouts, consulting table-info, or constructing Indexer JSON/PB data.

Ready and each response advertise `data_format = BCS` and
`bcs_schema_version = 1`. For each successful user transaction:

- `execution_output_bcs` is `bcs::to_bytes(&aptos_types::transaction::TransactionOutput)`.
  Decode with `bcs::from_bytes::<TransactionOutput>()`. It contains the write set,
  raw typed events, gas used, and status. The Rust type skips `auxiliary_data` in
  Serde, so that diagnostic field is excluded.
- `transaction_bcs` is `bcs::to_bytes(&aptos_types::transaction::Transaction)`.
  This is the signed input transaction, included so the bot can identify the entry
  function and check the hash. It is separate from the execution output.
- `events` and `write_set_changes` are empty. `data_complete` covers BCS
  serialization; it does not assert that the bot knows every Move layout.
- The existing `raw_events_bcs` / `raw_write_set_bcs` remain JSON-mode failure
  fallbacks. They are not the BCS-mode output fields.

Schema 1 uses the Aptos v1.50.1 Rust types in this branch. Pin the bot to compatible
Rust types; bump the schema before changing their serialized layouts. A Move
module upgrade can independently change an event layout, even when this transport
schema stays at 1. The decoder fails on unknown variants or mismatched bytes.

The existing 1,000-transaction / 3-MiB response limits still apply, including BCS
bytes. A single transaction over the limit rejects that block observation before
publication and increments dropped-block/error counters. Output remains executed,
not committed: it has no final ledger version, proof, or Indexer cursor. This
change does not provide replay or recovery.

### Run the Rust decoder

Build the client in the Aptos source checkout:

```bash
cargo build --profile performance -p aptos-execution-probe --bin execution-probe-decode --bin execution-probe-compare
```

On the bot host, consume the experimental stream:

```bash
./target/performance/execution-probe-decode \
  --probe http://127.0.0.1:50052 --seconds 60
```

The program emits one JSON line per matching executed transaction, for inspecting
client-side decoded data. It defaults to the exact entry function
`0x50ead22afd6ffd9769e3b3d6e0e64a2a350d68e8b102c4e72e33d0b8cfdfdb06::dex_accounts_entry::place_order_to_subaccount`.
Add `--all-transactions` to inspect the same event/resource types in other entries.
Redirect output to a local file if needed. This is a decoder example; it does not
apply bot state or replace committed Indexer reconciliation. A gap, dropped block,
incomplete output, or decode failure ends the program with an error.

The decoder checks event type tags before parsing
`market_types::OrderEvent::V1`, including its nested enums and Move
`Option<T>` vectors. It parses only known bodies. For ObjectCore, it handles direct
resource writes and BCS resource-group maps: a write to
`0x1::object::ObjectGroup` may contain the `0x1::object::ObjectCore` member that
Indexer presents as a `write_resource`. Unrelated Move resources and table values
remain opaque bytes.

To check the decoder against a committed reference transaction without deploying
the new node:

```bash
./target/performance/execution-probe-decode \
  --rest http://100.104.174.67:8080 --transaction-version 8094725931
```

This command requests `application/x-bcs` from REST, decodes the
`aptos_api_types::TransactionData::OnChain` envelope, then uses the same output
parser. It labels the result `committed: true`. The gRPC path labels it false.
The fixture and JSON expectations in `tests/data` cover that real transaction;
the selected ABI layouts are saved in `decibel-layouts.json`. The addresses and
u64/u128 values in the inspection output use strings to preserve precision.

### Compare arrival latency

Use the existing `execution-probe-compare` command. Its summary now includes
`data_format`, `bcs_schema_version`, and `bcs_bytes_received`; it checks consistent
format/schema across responses. `events_received` and
`write_set_changes_received` count the parsed PB fields and are zero in BCS mode.
The comparison client does not decode BCS bodies: its timing still ends when the
gRPC response is received and decoded, before the bot parses or applies data.
Measure bot decode/application separately before claiming usable-data latency.
No latency improvement is inferred from passing functional tests.
