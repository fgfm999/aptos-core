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
