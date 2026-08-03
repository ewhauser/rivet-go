# rivet-go: Rivet Actor SDK for Go

Host Rivet actors in Go by embedding `rivetkit-core` (Rivet's Rust actor runtime)
as a prebuilt cdylib loaded via [ebitengine/purego](https://github.com/ebitengine/purego) —
no cgo, no C toolchain at `go build` time. The same pattern as
[gomonty](https://github.com/ewhauser/gomonty): a hand-written Rust FFI crate with a
C ABI, msgpack payloads, per-platform embedded dylibs, and zero native→Go callbacks.

## Goal

A production-grade Go SDK where this works end to end against a real Rivet engine:

```go
type Counter struct {
    Count int `json:"count"`
}

func main() {
    registry := rivet.NewRegistry()
    rivet.Register(registry, "counter", rivet.Actor[Counter]{
        Actions: rivet.Actions[Counter]{
            "increment": rivet.Action(func(ctx *rivet.Context[Counter], amount int) (int, error) {
                ctx.State().Count += amount
                ctx.Broadcast("countChanged", ctx.State().Count)
                return ctx.State().Count, nil
            }),
        },
    })
    rivet.Serve(registry) // dials the engine, registers as a runner
}
```

Non-goals for v1: serverless runner mode, the inspector/devtools protocol,
raw SQLite actor storage (KV only), Cloudflare Durable Objects driver.

## Decision record: why embed rivetkit-core instead of implementing the wire protocol

Four options were evaluated (2026-08-02, against rivet-dev/rivet @ main):

| Option | Verdict | Reason |
|---|---|---|
| **purego + C-ABI FFI crate around rivetkit-core** | **Chosen** | Imports the hard semantics (checkpoint/ack, sleep, hibernation, reconnect) from the reference implementation. Pattern productionized in gomonty. No cgo. |
| Pure-Go runner-protocol implementation | Fallback | Wire protocol is small (~440-line BARE schema) but semantics are unspecified — they'd be reverse-engineered from `rivetkit-core` and re-validated on every engine release. Keep as escape hatch if FFI-crate maintenance becomes untenable. |
| wazero + existing `rivetkit-wasm` | Rejected | That build is wasm-bindgen glue targeting a JS host (`js-sys`, externref tables, unstable private ABI). Hosting it in Go means emulating a JS engine's object model. Revisit only if upstream ships a WASI/component build with a defined host interface. |
| cgo around rivetkit-core | Rejected | Loses cross-compilation and toolchain-free builds; no advantage over purego for this callback-free design. |

Key facts underpinning the decision:

- `rivetkit-core` (`rivet-dev/rivet:rivetkit-rust/packages/rivetkit-core`) is the
  single implementation of actor semantics. TypeScript binds it via napi-rs
  (`rivetkit-napi`) and WASM (`rivetkit-wasm`); Python via PyO3 (client only).
  We would be its fourth binding, not a novel architecture.
- The napi crate proves the embedding recipe: `rivetkit-core = { features = ["sqlite"] }`
  behind a cdylib. The wasm crate proves core builds with I/O externalized
  (`default-features = false, features = ["wasm-runtime", "sqlite-remote"]`).
- `@rivetkit/engine-runner` (pure TypeScript, in-tree) is a complete non-Rust
  implementation of the runner protocol — our behavioral reference and the proof
  that the fallback path is viable.
- Runner protocol churn is real: v1–v7 plus an "mk2" variant in roughly a year.
  Embedding core moves us off the wire-churn treadmill; the residual risk is
  core's *crate API* churn (see Risks).

## Architecture

Four layers, callback-free at the native boundary:

```
┌──────────────────────────────────────────────────────────┐
│ L3  rivet (public Go SDK)                                │
│     Registry, Actor[T], Context[T], state serde,         │
│     net/http bridge, connections/events, alarms          │
├──────────────────────────────────────────────────────────┤
│ L2  internal/pump (runtime)                              │
│     one goroutine blocking on runner_poll; dispatcher    │
│     fans events out to per-actor goroutines; submit      │
│     queue batches commands back                          │
├──────────────────────────────────────────────────────────┤
│ L1  internal/ffi (purego loader)                         │
│     go:embed prebuilt dylibs + sha256 checksums,         │
│     extract → dlopen → RegisterLibFunc; owned handles;   │
│     stub build for unsupported platforms                 │
├──────────────────────────────────────────────────────────┤
│ L0  crates/rivetkit-go-ffi (Rust, C ABI)                 │
│     embeds tokio + rivetkit-core; implements core's      │
│     actor traits as proxies that enqueue events;         │
│     event-pump API, msgpack envelopes                    │
└──────────────────────────────────────────────────────────┘
                        │ WebSocket (runner protocol, VBARE)
                        ▼
                  Rivet Engine
```

The FFI surface is an **event pump**, not callbacks — rivetkit-core is an active
runtime (owns tokio, holds the engine WebSocket, generates work spontaneously),
so the boundary inverts it into two queues:

```
runner_new(config)            -> RunnerHandle | Error
runner_poll(h, timeout_ms)    -> msgpack event batch   (blocking, one caller)
runner_submit(h, batch)       -> Ack | Error           (any goroutine, serialized)
runner_shutdown(h, deadline)  -> DrainReport
```

Full boundary contract — envelope format, event/command catalog, ownership and
threading rules, streaming/backpressure — lives in [FFI-BOUNDARY.md](FFI-BOUNDARY.md).

### Division of labor

`rivetkit-core` (behind the boundary) owns everything correctness-critical:
engine WebSocket + reconnect, command/event checkpoints and acks, KV
persistence, alarms/scheduling, sleep/wake, connection hibernation, slot
accounting.

Go owns everything developer-facing: actor registration and action dispatch,
state (de)serialization, the `net/http`-style handler bridge for tunneled
requests, WebSocket connection objects, client-visible errors, logging/metrics.

`crates/rivetkit-go-ffi` owns the inversion: it implements core's actor/handler
traits with proxies that enqueue events and park awaiting the matching command
(correlation IDs), so core believes it is calling a local actor while the real
handler runs in a goroutine.

## Repository layout

```
rivet-go/
├── docs/                      this plan + boundary spec + ADRs
├── crates/rivetkit-go-ffi/    Rust C-ABI crate (cbindgen header)
│   ├── src/lib.rs             handle lifecycle, pump, panic firewall
│   └── src/wire.rs            msgpack envelope types (mirrors Go internal/wire)
├── internal/ffi/              purego loader (gomonty pattern)
│   ├── lib/<platform>/        embedded prebuilt dylibs + checksums.txt
│   └── include/               generated C header (reference only)
├── internal/wire/             msgpack envelope types + fuzz corpus
├── internal/pump/             poll loop, dispatcher, submit batcher
├── rivet/                     public SDK (the only importable surface)
├── client/                    Go client for calling actors (HTTP/WS, JSON)
├── examples/
├── conformance/               harness vs pinned engine binary
└── scripts/build-ffi.sh       Rust build matrix → internal/ffi/lib/
```

Platform matrix (same as gomonty): `darwin/arm64`, `linux/amd64` (gnu+musl),
`linux/arm64` (gnu+musl), `windows/amd64`. Non-matrix platforms get a stub that
returns a clear error at `rivet.Serve`.

## Public API design notes

- **Typed actors via generics**: `rivet.Actor[T]` where `T` is the state struct;
  JSON serde by default, `encoding.BinaryMarshaler` override for hot paths.
- **Action dispatch**: explicit registration map (as in the Goal sketch), not
  reflection over method sets — keeps dispatch table auditable and avoids
  init-order reflection surprises. Revisit codegen (`go generate`) if the map
  gets tedious.
- **Tunneled HTTP**: expose as `http.Handler` per actor (`OnFetch func(ctx, w, r)`)
  with a `ResponseWriter` implementation that chunks through the pump.
- **Context semantics**: one goroutine per live actor; actions on the same actor
  are serialized (matches Rivet's model); `ctx.State()` mutations are persisted
  via core's state save on action completion.
- **Client package** is independent of the runner (talks the client protocol,
  spec'd in `rivetkit-openapi`/`rivetkit-asyncapi`) so callers don't link the dylib.

## Build, release, and version pinning

- CI builds the Rust matrix (`scripts/build-ffi.sh`, cargo + zigbuild for musl),
  writes `internal/ffi/lib/`, updates `checksums.txt`; release-prep workflow
  mirrors gomonty's (`release-prep.yml` / `release.yml`).
- **Every rivet-go release pins one Rivet engine version.** The FFI crate builds
  against that exact `rivet-dev/rivet` tag (git dependency); the conformance
  suite runs against that engine binary. Engine upgrades are deliberate PRs
  bumping the pin + regenerating dylibs, never implicit.
- Module stays `go get`-able: dylibs are committed (≈15 MB × 6 — accepted;
  gomonty precedent). If module-proxy size becomes a problem, fall back to
  first-run download with checksum verification (loader already verifies sha256).

## Testing strategy

1. **Conformance harness** (`conformance/`): starts the pinned engine binary
   (single binary, filesystem storage) in CI, runs the Go SDK through actor
   lifecycle, KV persistence across actor restarts, HTTP/WS tunneling, alarms,
   sleep/wake, engine restart with runner reconnect, graceful drain.
2. **Parity reference**: where behavior is ambiguous, `@rivetkit/engine-runner`
   (pure-TS) and `rivetkit-napi` tests are the spec; port their scenarios.
3. **Boundary fuzzing**: fuzz msgpack envelopes both directions
   (`internal/wire` ↔ `wire.rs`), gomonty `fuzz_test.go` style; malformed
   input must produce Go errors, never UB or process death.
4. **Panic firewall tests**: Rust panics must cross as error events
   (`catch_unwind` at every extern fn); Go-side handler panics must stop the
   actor with `StopCode::ERROR`, not kill the pump.
5. **Soak**: overnight run with chaos (engine restarts, connection drops)
   watching for leaks of handles/buffers (ownership rules in FFI-BOUNDARY.md).

## Milestones

| # | Deliverable | Exit criterion | Est. |
|---|---|---|---|
| M0 | Skeleton: FFI crate builds against pinned rivet tag; purego loader loads it; `runner_version()` round-trip; CI matrix green | `go test ./internal/ffi` passes on all 6 targets | 1 wk |
| M1 | Pump + registration: `runner_new` dials local engine, `ToServerInit` sent, runner visible to engine; poll/submit loop with correlation | Conformance: engine lists the runner | 1 wk |
| M2 | Actor lifecycle: start/stop events → Go handlers; state load/save via core KV; actor survives restart with state intact | Conformance: counter actor persists across engine restart | 1–2 wk |
| M3 | Actions + HTTP tunnel: `http.Handler` bridge, request/response streaming | Conformance: client curl → actor action round-trip | 1–2 wk |
| M4 | WebSockets + events: connection objects, broadcast, message acks | Conformance: two WS clients see each other's broadcasts | 1–2 wk |
| M5 | Scheduling + sleep: alarms, sleep/wake, hibernating WS (`canHibernate`) | Conformance: alarm fires after actor slept; WS survives hibernation | 2 wk |
| M6 | Production hardening: graceful drain, panic firewall, soak/chaos, metrics hooks, docs + examples | 24 h soak clean; README quickstart works from scratch | 2–3 wk |

Roughly 9–13 weeks solo to a production-ready v0. M0–M3 (~4–6 wk) is the
demo-quality milestone and the point of maximum learning — reassess the
approach there.

## Risks and mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| `rivetkit-core` crate API churn (unpublished, no stability contract; in-tree consumers move in lockstep, we chase tags) | High | Pin per release; isolate all core API contact inside the FFI crate; **pitch upstreaming `rivetkit-go-ffi` into rivet-dev/rivet next to `rivetkit-napi`** — this is the single highest-leverage action and should start at M0, not M6 |
| Boundary semantics bugs (event ordering, correlation, ownership) | High | Boundary spec written first (FFI-BOUNDARY.md); fuzzing; conformance suite; keep the surface small and coarse |
| Blocking `runner_poll` pins an OS thread; submit contention | Low | One pinned thread is by design (gomonty precedent); batch submits; measure in M6 soak |
| purego platform gaps (e.g. linux/386, BSDs) | Low | Stub build with clear runtime error; matrix covers the deployment targets that matter |
| Binary size in module / proxy limits | Medium | Accepted for v1; checksummed download fallback designed but not built |
| Rivet pivots the runner model (e.g. ships official polyglot FFI or WASI component) | Medium | That's a win, not a loss — L2–L3 survive a backend swap; keep the pure-Go wire fallback documented |
| tokio inside dlopen'd lib + Go runtime coexistence (signals, TLS) | Medium | tokio confined to its own threads; no signal handlers in FFI crate; validated in M0 on all platforms |

## Upstream engagement (start immediately)

1. Open a discussion in rivet-dev/rivet: intent to build a Go runner via an
   embedded-core FFI crate; ask whether they'd take it in-tree.
2. Ask specifically about: stability intentions for `rivetkit-core`'s API,
   the "mk2" protocol direction, and any existing Go plans (they already ship
   generated Go for the management API in `engine/sdks/go/api-full`).
3. Longer-term pitch: a WASI-component build of core with a defined host
   interface (`wasm-runtime` feature shows the seam exists) — would make every
   future language binding, including a pure-Go wazero host, nearly free.

## References

- Reference implementations: `rivetkit-rust/packages/rivetkit-core`,
  `rivetkit-typescript/packages/engine-runner` (pure TS, behavioral spec),
  `rivetkit-typescript/packages/rivetkit-napi` (embedding recipe),
  `engine/packages/pegboard-runner` (server-side authority)
- Protocol schemas: `engine/sdks/schemas/runner-protocol/*.bare` (v7 current)
- Pattern source: `github.com/ewhauser/gomonty` (purego loader, wire layer,
  release workflows, upstream-refresh skill)

## Implementation notes

### M1 — 2026-08-02

The pinned `rivetkit-core` at `v2.3.10` has completed an internal runner-to-envoy
rename that the original plan did not reflect. `CoreRegistry` now connects
through `/envoys/connect` using `rivet-envoy-protocol` v6 and publishes initial
metadata as `ToRivetMetadata`; it does not use the legacy runner-protocol
`ToServerInit` path. Accordingly, the engine's active registration is asserted
through the management API's `GET /envoys`, while the public Go and FFI names
remain `Runner` to keep this upstream rename out of the SDK contract. The
legacy `GET /runners` endpoint cannot observe a `CoreRegistry` registration at
this pin.

`CoreRegistry::serve_with_config_and_handle_observer` is the supported embedded
registration seam and accepts the runner version and pool/name, but its
`ServeConfig` and the underlying `EnvoyConfig` have no `total_slots` field.
M1 therefore retains and validates `RunnerConfig.total_slots` at the stable FFI
boundary but cannot transmit it at `v2.3.10`. Actor capacity remains zero
because the actor manifest is empty. This field stays in place for the M2
actor-capable adapter rather than changing the boundary or bumping the pin.
