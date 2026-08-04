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
keeps per-message hibernation acknowledgements out of the echo comparison.

Run the complete, sequential evaluation with:

```sh
./bench/run.sh
```

`run.sh` uses Node without runtime flags and sets `NODE_ENV=production`.
All runners use error-level logging. Raw JSON is written below the ignored
`bench/results/`; a dated provenance copy and Go S1/S3 CPU profiles are kept
under `bench/results-archive/`.

Engine `v2.3.10` hard-codes a 10,000 request/minute limit per client IP. The
load generator assigns one stable loopback `X-Forwarded-For` identity to each
HTTP worker, matching the engine's documented reverse-proxy path. This keeps
the public abuse-control ceiling from becoming an artificial throughput cap;
the same identity mapping is used for every SDK and correctness still rejects
all non-2xx responses.
