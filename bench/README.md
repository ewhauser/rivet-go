# Rivet runner performance evaluation

This directory compares Go, TypeScript, and Rust actor runners through the
same Rivet Engine `v2.3.10` loopback gateway. `loadgen` is the only benchmark
client; runner-language client SDKs are deliberately excluded.

The strict `persist` variant does not return from `increment` until the state
save has completed. Go's public action adapter already does that after every
successful action, TypeScript calls `saveState({ immediate: true })`, and Rust
calls and awaits `Ctx::save_state`. The Go SDK does not offer a no-persist
successful action, so `no-persist` is measured only for TypeScript and Rust,
using generation-local actor variables rather than persistent state.

The pinned standalone Rust package has an additional configuration wrinkle:
its `sqlite` feature enables the remote implementation, but the registry only
selects that backend when `Actor::HAS_DATABASE` is true. Both Rust benchmark
actors set that marker even though they issue no application SQL; without it,
new stateful actors fail at startup with `SQLite is unavailable`. This is pin
behavior, not a benchmark optimization.

All three S3 echo actors use the pinned default of non-hibernating raw
WebSockets. Go spells this out as `HibernateWebSockets: false`; TypeScript and
Rust leave their corresponding actor options at the same false default. This
keeps private boundary acknowledgement bookkeeping and per-message engine
hibernation acknowledgements out of the echo comparison.

## S5 per-actor SQLite candidates

S5 compares Go-ffi, Go-socket, and TypeScript `rivetkit@2.3.10` `c.db` as an
external raw-SQL reference. Both Go candidates use the same core LocalNative
SQLite backend; only the Go-to-core transport changes. TypeScript calls
`c.db.execute` and its callback `transaction` directly with no ORM. The SQL
statements match, but TypeScript returns object rows and manages the transaction
callback while Go returns column/value matrices and exposes a lease-backed
`Tx`.

Each suite starts one fresh engine data directory and runs two sequential
repetitions. A repetition maps 32 workers one-to-one to 32 actors, excludes a
10-second warmup, and measures a 45-second window. The deterministic mix is
50% point `SELECT`, 40% single-row `INSERT`, and 10% one explicit transaction
containing `INSERT`, `UPDATE`, and `SELECT`. That transaction counts as one
composite operation. Each actor's final row count must equal its post-warmup
baseline plus every successful measured insert.

Run only S5 with:

```sh
./bench/run-s5.sh
```

The script refuses to overwrite a dated archive, builds both runners, sets
`RIVET_GO_SQLITE_TRANSPORT` for the Go cells, samples runner and engine CPU,
appends the labeled table to `RESULTS.md`, and archives JSON, complete process
logs, environment data, and SHA-256 checksums. `report-s5` rejects a missing or
duplicate cell, any non-10/45 timing request, missing CPU samples, load errors,
or failed row reconciliation.

The recorded 2026-08-05 run had no load-generator errors. Its TypeScript
reference log contains 128 teardown-only `transaction_closed` schedule cleanup
records after its two measured repetitions, and its engine log contains two
request-metrics timeouts. Both Go transport logs contain no error-level record.
Those diagnostics are archived for review and are not counted as successful
operations or hidden by the row oracle.

Run the complete, sequential evaluation with:

```sh
./bench/run.sh
```

`run.sh` uses Node without runtime flags and sets `NODE_ENV=production`.
All runners use error-level logging. Raw JSON is written below the ignored
`bench/results/`; a dated provenance copy and Go S1/S3 CPU profiles are kept
under `bench/results-archive/` (gitignored). Historical archives cited by
RESULTS.md and OPTIMIZATION.md were moved out of the repository to keep the
module small; they are preserved as the `rivet-go-bench-archive-*.tar.gz`
release asset, which unpacks to the same `results-archive/<date>` layout.

Engine `v2.3.10` hard-codes a 10,000 request/minute limit per client IP. The
load generator assigns one stable loopback `X-Forwarded-For` identity to each
HTTP worker, matching the engine's documented reverse-proxy path. This keeps
the public abuse-control ceiling from becoming an artificial throughput cap;
the same identity mapping is used for every SDK and correctness still rejects
all non-2xx responses.
