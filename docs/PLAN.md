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

| # | Deliverable | Exit criterion | Est. | Status / review |
|---|---|---|---|---|
| M0 | Skeleton: FFI crate builds against pinned rivet tag; purego loader loads it; `runner_version()` round-trip; CI matrix green | `go test ./internal/ffi` passes on all 6 targets | 1 wk | Complete — [review](reviews/M0-REVIEW.md) |
| M1 | Pump + registration: `runner_new` dials local engine, `ToServerInit` sent, runner visible to engine; poll/submit loop with correlation | Conformance: engine lists the runner | 1 wk | Complete — [review](reviews/M1-REVIEW.md) |
| M2 | Actor lifecycle: start/stop events → Go handlers; explicit state load/save via core; actor survives restart with state intact | Conformance: counter actor persists across engine restart | 1–2 wk | Complete — [review](reviews/M2-REVIEW.md) |
| M3 | Actions + HTTP tunnel: `http.Handler` bridge, request/response streaming | Conformance: client curl → actor action round-trip | 1–2 wk | Complete — [review](reviews/M3-REVIEW.md) |
| M4 | WebSockets + events: connection objects, broadcast, message acks | Conformance: two WS clients see each other's broadcasts | 1–2 wk | Complete — [review](reviews/M4-REVIEW.md) |
| M5 | Scheduling + sleep: alarms, sleep/wake, hibernating WS (`canHibernate`) | Conformance: alarm fires after actor slept; WS survives hibernation | 2 wk | Complete — [review](reviews/M5-REVIEW.md) |
| M6 | Production hardening: graceful drain, panic firewall, soak/chaos, metrics hooks, docs + examples | Configurable strict soak harness; bounded 15-minute run clean; 24-hour release soak documented; README quickstart works from scratch | 2–3 wk | Complete — [review](reviews/M6-REVIEW.md) |
| M7 | Per-actor SQLite through FFI-pump and actor-runtime-socket candidates behind one `Context.DB` API; conformance and S5 comparison | Both transports pass the same durability/isolation/transaction suite; two-repetition S5 archive includes Go-ffi, Go-socket, and TypeScript `c.db` raw SQL | 2–3 wk | Complete — candidate review pending; no default selected |

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

### Remaining production limitations

- Hibernatable messages have a 60-second acknowledgement bound, frames in the
  sleep-intent gap finish on the old generation, and the pin may hide a client
  close that occurs while the actor is fully asleep.
- HTTP headers are limited to 256 names and 1 MiB per name/value; the pinned
  core carries one value per name and buffers complete requests and responses.
- Alarm mutation completion includes the pin-specific four-second transport
  fence, and the engine's 16-second workflow tick prevents low-latency timing
  guarantees.
- Per-actor SQL has no default transport while M7 is under review. Applications
  must select `ffi` or `socket`; the socket candidate is Unix-only. Results are
  fully buffered, bounded to 32 MiB overall, 1 MiB per SQL/text/blob value,
  1,024 parameters and columns, and an active transaction exclusively gates
  other operations for up to its 60-second default lease.
- Each runner uses one blocking, OS-thread-pinned poller. Submissions and
  operability hooks run elsewhere, but event decoding and dispatch are
  intentionally single-poller.

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

### M2 — 2026-08-03

M2 resolves the state-persistence question against the pinned core source.
`ActorContext::save_state(Vec<StateDelta>)` is an explicit, awaited operation,
and `ActorStart.snapshot` supplies the last persisted actor-state bytes.
Accordingly, the Go API exposes `Context.Save(ctx)` rather than an implicit
save-on-hook-return policy. The FFI keeps the planned `SaveState` command and
`StatePersisted` completion event. A missing snapshot and a persisted
zero-length snapshot remain distinct across the boundary, so a custom
`encoding.BinaryUnmarshaler` can handle an intentionally empty state.

Core stores actor state and its public actor KV surface in the actor's internal
SQLite database. The Go adapter selects core's supported `sqlite-remote`
backend so database operations are executed and persisted by the engine/envoy.
The alternative native-local backend requires an atomic-write-enabled SQLite
build and is not part of the Go SDK's prebuilt-library contract. This remains
core-backed persistence and is verified across an actual engine process
restart using the same filesystem data directory.

The restart conformance kills the engine process while the actor is live and
keeps the runner transport reconnecting. With the engine still unavailable it
requires the old actor generation's `OnStop`, proving that the first Go actor
worker and its in-memory typed state are gone. It then launches a distinct
engine process against the same filesystem data directory, waits for the
persisted actor workflow to enter its sleeping/lost state, and wakes it through
the public gateway path. A higher-generation `ActorStart` must deliver the
saved counter to a custom `encoding.BinaryUnmarshaler`; no Go-side cache or
pre-restart observation can satisfy that assertion.

Accepted state saves are fenced ahead of `ActorStopResult` on both sides of the
boundary. Go stops admitting actor-scoped work and waits for accepted saves;
Rust reserves each state operation before dispatch and does not resolve the
stop lifecycle reply until those operations finish. Core state saves time out
after 30 seconds with a structured completion, while the Go acknowledgement
backstop is 35 seconds. A caller cancellation leaves the save correlation
poisoned only until its late acknowledgement is consumed, preventing ID/result
misassociation without permanently disabling later saves.

The remaining pin-specific lifecycle mappings are documented in
[FFI-BOUNDARY.md](FFI-BOUNDARY.md). Actions, HTTP, WebSockets, alarms, and sleep
remain outside M2.

## M3 pin-specific deviation notes — 2026-08-03

M3 adds the explicit action manifest, typed and raw action adapters, and the
`OnFetch` HTTP bridge. Successful actions persist the complete typed actor
state before their result is released to the engine. Handler errors use
client-visible structured actor errors. A panic returns `handler_panic`, stops
only that actor generation through core's run-handler failure path, and leaves
the shared pump and peer actors running.

The v2.3.10 gateway accepts JSON action requests but normalizes the arguments
to a CBOR array before `ActorEvent::Action` reaches an embedder. The typed Go
adapter therefore decodes and encodes CBOR while honoring `json` struct tags;
`RawAction` receives the exact CBOR argument array and returns one CBOR value.
HTTP action dispatch also produces ephemeral connection preflight and open
hooks. The FFI acknowledges those hooks so the action can run, but does not
expose the M4 connection or WebSocket APIs.

At this pin, `ActorEvent::HttpRequest` contains a buffered request and its
reply type is `Response<Vec<u8>>`. The M3 boundary still divides request and
response bodies into chunks no larger than 1 MiB, and Go submits response
chunks from the handler goroutine with bounded-queue retry. Rust must assemble
the response chunks once before satisfying core's buffered reply. Core does
not expose a client-disconnect notification at this embedder seam, so
`HttpRequestAbort` is emitted for boundary deadline expiry; runner and actor
shutdown cancel the request context directly. These constraints are pinned-core
deviations, not public streaming or abort guarantees for later engine versions.

The action deadline is part of each M3 `ActionCall` and comes from the same
60-second Rust duration configured on the core actor. Context-aware Go action
adapters receive that deadline. Cancellation is cooperative; core returns a
structured timeout even if a handler ignores it, and a late result cannot
resolve a different call. The actor resumes serialized work after the handler
returns.

The M3 response writer does not advertise `http.Flusher` because the pinned
core buffers the reply. It serializes concurrent writes, rejects writes after
the handler returns, enforces declared `Content-Length`, and bounds native
backpressure retry to 30 seconds. Header maps remain limited to 256 names; the
public gateway rejects an over-limit request with HTTP 431 before dispatch.
Repeated request fields arrive as the last value at this pin; multiple
`Set-Cookie` response values are rejected structurally rather than joined into
an invalid field. Header names and values over the boundary's 1 MiB blob cap
also fail structurally.

The real-engine conformance test uses `net/http` against
`/gateway/{actor_id}/action/{action}` and `/gateway/{actor_id}/request/...`.
It asserts action results and persisted state, cross-actor isolation, a raw
HTTP response spanning multiple boundary chunks, structured missing-action
errors, and actor-local action/fetch panic handling exclusively from public
HTTP and engine-observed results. A separate restart case mutates state only
inside an action and proves the implicit successful-action save rehydrates in
a higher actor generation after a real engine process replacement.

## M4 pin-specific deviation notes — 2026-08-03

M4 adds raw gateway WebSockets to the typed actor definition through
`OnConnect`, `OnMessage`, and `OnDisconnect`. One `Connection` object is kept
for the complete open/message/close lifecycle and exposes text or binary send,
actor-initiated close, path/header metadata, the core connection ID, and the
pin's hibernation capability. `Context.Broadcast(event, payload)` encodes one
CBOR argument and sends the named event to every live raw connection on the
actor; `BroadcastExcept` excludes the supplied connection. Actions use the
same actor context and can broadcast without a separate integration path.

The v2.3.10 native actor-event surface and raw WebSocket surface are distinct.
`ActorContext::broadcast` reaches actor-connect clients that subscribed to the
event, but it does not write raw WebSocket frames. The M4 FFI invokes that
native method with the CBOR argument-array bytes and additionally frames raw
client broadcasts with the pin's CBOR actor-connect event envelope:
`{body: {tag: "Event", val: {name, args}}}`. This is the shape emitted by
`rivetkit-core/src/registry/actor_connect.rs` and decoded by
`rivetkit-client/src/protocol/codec.rs`, rather than an SDK-specific raw frame.
The public call remains `ctx.Broadcast("countChanged", value)`; targeted
text/binary sends are unchanged.

Raw WebSocket messages are complete frames capped at 1 MiB. They are not split
because multiple frames are not equivalent to one WebSocket message. An
incoming oversized frame closes that connection with code 1009 and reason
`message.incoming_too_long`; an oversized targeted Go send returns an error
without emitting a partial frame or closing a healthy connection. Native
command submission uses the M3 backpressure retry discipline on the handler
goroutine, leaving polling available. Each connection also has a 64-command
native outbound admission queue; overflow closes only that connection with
WebSocket code 1013 and reason `outbound_backpressure`. Concurrent Go callers
are batched up to the boundary's 1024-command envelope ceiling so a burst is
presented atomically enough for that per-connection bound to be load-bearing.
At this pin core accepts admitted sends into an internal unbounded envoy queue
and exposes no peer buffered-byte metric. Conformance therefore stalls a real
gateway client, overflows the admission queue through concurrent sends,
observes the 1013 close at that client, and proves a peer remains usable; the
Rust unit retains deterministic queue isolation coverage.

Connection ownership is recorded on both sides of the boundary. Client close,
actor-issued close, actor stop, and runner shutdown remove the Rust registry
entry deterministically and deliver `OnDisconnect` once in Go. A panic in any
WebSocket hook is contained by the existing handler firewall, submits the
cataloged `StopIntent`, and stops that actor generation without ending the
pump or peer actors.

Broadcast with no live connection, including from `OnStart`, is a successful
no-op. During a graceful actor stop, Go delivers each `OnDisconnect` exactly
once before `OnStop`; a broadcast initiated by `OnStop` is admitted before the
stop acknowledgement. Delivery is best-effort after shutdown begins because
the pinned engine may already be closing the gateway transport. A delivered
event precedes the code-1001 `actor stopped` close; otherwise the client sees
that close directly. Graceful runner shutdown uses the same rule; the shutdown
fallback closes any remaining connection with code 1001 and reason `runner
shutting down`.

The real-engine conformance tests use the public gateway with the same Rivet
WebSocket subprotocol metadata as the pinned official raw client. They prove
two-client handler-mediated broadcast, `BroadcastExcept`, targeted send,
text/binary and empty frame fidelity, exact 1 MiB acceptance and two-sided
oversize behavior, ordered message handling and broadcast delivery,
client/actor close races, disconnect during `OnConnect`, rejection, actor and
runner shutdown, every hook panic, action-originated broadcast, state save and
restart rehydration, exactly-once delivery to 50 simultaneous connections
under two concurrent action calls, and the real 1013 stalled-client policy.

Hibernation is deliberately deferred to M5. M4 keeps `no_sleep = true` and
configures core with `can_hibernate_websocket = false`, while still carrying
`can_hibernate`, `msg_index` acknowledgements, and `WsCloseCmd.hibernate`
through ABI 4. The command takes the ordinary close path and the acknowledgement
is process-local bookkeeping only. M5 must add durable replay and reconcile
core's current post-callback acknowledgement timing before enabling either
sleep or hibernating WebSockets.

## M5 pin-specific deviation notes — 2026-08-03

M5 enables core sleep (`no_sleep = false`) and raw WebSocket hibernation
(`can_hibernate_websocket = true`) and bumps the single-sourced FFI ABI to 5.
`Context.Schedule`, `ScheduleAfter`, and `ClearSchedule` map the actor's one Go
alarm to a reserved durable one-shot core schedule. `OnAlarm` runs as a normal
serialized actor callback after core and the engine wake a sleeping actor, and
its resulting state is saved before `AlarmHandled`. Rapid replacement is
latest-wins and clear removes only the reserved Go alarm.

Alarm and sleep commands carry an operation ID plus the originating actor
generation. Schedule and clear return only after the native schedule mutation
has completed and the pin's separately delivered workflow signal has had two
1.5-second poll intervals plus a 1-second margin to settle. Without that
ordering, the engine can process the later sleep checkpoint first and reject
the lower alarm checkpoint as stale. Sleep returns after the exact generation
admits the intent;
the later `ActorStop` is the proof that core drained and evicted it. This split
avoids making a handler wait for an eviction that cannot begin until that
handler returns. An absent `OnAlarm` produces a structured
`callback_not_found` `AlarmHandled` result, and alarms share the pinned core's
60-second ordinary-action deadline.

`Context.Sleep` is an intent, not an immediate goroutine cancellation. Work
already admitted to the actor's serial worker completes first: the initiating
action/HTTP/WebSocket handler returns, successful action or alarm state is
persisted, the action/HTTP result or WebSocket acknowledgement is submitted,
and then `OnStop` runs. Explicit state and alarm mutations already accepted by
Rust are also drained before `ActorStopResult`. Handler deadlines, engine HTTP
aborts, and forced runner-shutdown cancellation retain their earlier behavior.
Graceful runner shutdown reaches core actors as sleep: eligible WebSockets are
hibernated, `OnStop` completes, and neither a transport close nor
`OnDisconnect` is exposed. This supersedes M4's runner-close expectation; the
shutdown fallback still closes any transport left after the grace deadline.
The real-engine mid-flight cases observe the action result, HTTP response, and
WebSocket replies before the stop hook and actor eviction.

Core hides hibernating gateway/request IDs, persists both message indexes and
request metadata, restores `CommandStartActor.hibernatingRequests`, and drops
already-acknowledged client replay. It expects the embedder's raw-message
callback not to return before handler acceptance is durable. The FFI now holds
that callback until Go submits the matching FIFO `WsMessageAck`; only then can
core persist the index and acknowledge the engine. On sleep the old generation
is detached without a transport close, and the restored `WsOpen` is marked
`resumed` so Go rebuilds its connection table without calling user
`OnConnect`. Hibernation does not call `OnDisconnect`; a real awake close does.
If a client disappears while fully asleep, v2.3.10 may prune it during core
startup settlement before Go can observe a disconnect.

Sleep admitted from inside a WebSocket handler is applied after that handler's
FIFO acknowledgement. Same-socket frames accepted in this boundary window run
in order on the old generation; frames sent after engine-visible sleep wake
and run on the new generation. The conformance case fixes this pin-specific
outcome and rejects loss or duplicate delivery on either side of eviction.

The M5 real-engine suite covers alarm wake with pre-sleep state, clear,
latest-wins replacement, engine-restart alarm identity and durability,
mid-flight action/HTTP/WebSocket drain ordering, unsaved-state loss, repeated
sleep/wake generations, and a same-socket hibernation cycle with ordered
messages on both sides of eviction and post-wake actor-to-client traffic. The
pinned workflow worker polls every 16 seconds, so negative alarm observations
cover one complete tick plus a 5-second margin. Positive schedules are 20,
35, 45, or 60 seconds according to the scenario; canceled/superseded alarms
use 12 seconds so their local deadline remains beyond the 4-second transport
settlement. A 90-second bound includes the schedule, one tick, delivery margin,
and `-race` scheduling. Restart durability uses an original 60-second schedule,
waits through the pin's 22-second envoy liveness
window, demand-rehydrates once to make the pin reconcile its persisted core
schedule after abrupt engine replacement, and resleeps without rescheduling
before the alarm wake.

## M6 production-hardening notes — 2026-08-03

M6 keeps ABI 5 and the v2.3.10 engine pin. The native change is lifecycle-only:
when process-level drain begins, core actor proxies mark the runner as draining
and close raw gateway WebSockets with code 1001 and reason `runner shutting
down`. Ordinary actor sleep still hibernates eligible sockets. Go stops
accepting new work, lets already-admitted actor-serial handlers and their
accepted persistence or scheduling operations finish within the configured
deadline, waits for `RunnerStopped`, and then closes the native handle.

`rivet.Serve` owns `SIGINT` and `SIGTERM`; context cancellation follows the
same path. `Config.ShutdownTimeout` defaults to 10 seconds. `Config.Hooks`
provides dependency-free counters, gauges, and duration observations, while
`Config.Logger` accepts `log/slog` and defaults to discard. The FFI wrappers
also expose process-local native runner, error, and buffer handle counts for
the strict soak drain oracle; those counters do not change the wire contract.

The M6 chaos harness sends all actor work through the real gateway. It runs
counter actions, chat broadcasts, and alarm-driven sleep/wake while replacing
the engine against the same data directory, dropping clients, stalling a live
WebSocket, and panicking sacrificial actors. Field-by-field Go truth models
advance from the workload generator's intents rather than actor callbacks.
Per-client ordered receipt ledgers, monotonic sequence checks, nonzero workload
and chaos guards, and final goroutine/native-handle baselines make a vacuous or
partially observed run fail. Seeds and temporary data/log paths are printed
before work; failed runs preserve data while successful default runs remove it.
The command defaults to a two-minute CI smoke;
[OPERATIONS.md](OPERATIONS.md) owns the 24-hour release runbook.

The public README quickstart, `cmd/rivet-go-dev`, `examples/counter`, and
`examples/chat` all use the exact pinned engine acquisition path shared by
conformance. Real-subprocess conformance sends `SIGTERM` while an action and a
WebSocket are live, proving action completion, code-1001 socket closure,
runner disappearance from the engine, and exit status zero.

M6 introduces no SDK or FFI MessagePack shape and therefore no new ABI decode
surface. Its test/tooling-only decoders are bounded: the soak management and
action clients decode local-engine JSON with capped response bodies, and its
WebSocket clients decode the existing pinned CBOR actor-connect event envelope
under a 1 MiB read limit. No fuzz case, deliberately malformed-input test, or
raw binary-payload test was added.

## WebSocket hibernation opt-in deviation note — 2026-08-04

The original M5 implementation registered every Go actor with
`can_hibernate_websocket = true`. That differed from rivetkit TypeScript,
rivetkit Rust, and `ActorConfig::default()` at v2.3.10, all of which default
raw WebSocket hibernation to false. ABI 6 corrects the public default and adds
`Actor.HibernateWebSockets`; the registry carries one boolean per actor in
`RunnerConfig.actor_hibernate_websockets`, and the FFI maps it directly to
core's `ActorConfig.can_hibernate_websocket` before registering that actor
factory. Core continues to derive the per-open `can_hibernate` value and owns
all persisted hibernation metadata.

The false default closes a raw WebSocket when its actor sleeps, invoking
`OnDisconnect`; an actor that opts in retains the M5 sleep/wake behavior and
suppresses `OnDisconnect` for hibernation itself. The hibernation conformance
actor, the chat example, and the hibernating soak chat actor now opt in
explicitly. The soak also opens a default non-hibernating socket and requires
the code-1001 `actor sleeping` close, so both paths remain active coverage.

The configuration mismatch explained the earlier Go S3 latency gap. An
interleaved loopback A/B changing only this flag moved Go client p50 from
8.243 ms to 6.459 ms; the hibernating path emitted an engine acknowledgement
for every message. This is about 1.8 ms of observed p50 overhead, not evidence
that the callback-free FFI event pump is intrinsically that far behind.
Default sockets also skip the private Go-to-Rust `WsMessageAck` command and
Rust FIFO/index bookkeeping; only opt-in sockets retain that M5 machinery.

New decode surfaces: ABI 6 adds one bounded string-to-boolean map to the
existing MessagePack `RunnerConfig`. Go registration and Rust startup enforce
the 1,024 actor-name limit, the Go shape scanner accepts the exact-bound map,
and Rust rejects keys that are not present in `actor_names`. It adds no event,
command, blob, or unbounded allocation surface. No fuzz or deliberately
malformed-input test was added.

## M7 per-actor SQLite notes — 2026-08-05

M7 resolves the pin's `LocalNative` terminology from core source. It means
SQLite statement execution and transaction coordination run in the embedded
runner process. It does not mean actor data is local-only: core's native SQLite
worker uses the Depot VFS, whose page reads and commits travel through the
envoy to engine Depot and FoundationDB/filesystem storage. A successful commit
is durable at the engine commit boundary. `RemoteEnvoy`, used by the M2 state
mapping, instead executes SQL on the engine/envoy side.

The embedding compiles both core features. With no SQLite transport selected,
the existing state and actor-KV path remains `RemoteEnvoy` and `Context.DB()`
returns `sqlite_transport_not_configured`. Selecting either `ffi` or `socket`
sets `ActorConfig.remote_sqlite = false`, enables the actor database, and uses
the same `LocalNative` database. This makes S5 a transport comparison against
one backend mode. Core's FFI-visible `SqliteDb` API can operate against either
backend, but the Actor Runtime Socket rejects every database other than an
enabled `LocalNative` one, so comparing FFI/remote with socket/local would
confound transport and execution placement.

`Config.SQLiteTransport` accepts only `rivet.SQLiteTransportFFI` (`"ffi"`) or
`rivet.SQLiteTransportSocket` (`"socket"`).
`RIVET_GO_SQLITE_TRANSPORT` overrides it for a complete runner invocation.
There is intentionally no fallback or recommendation while the M7 result is
reviewed. The socket candidate also enables
`ActorConfig.enable_actor_runtime_socket`, provisions the Unix endpoint before
`ActorStart`, and passes its generation-scoped path to Go. The endpoint is
closed and recreated on sleep/wake; an open socket transaction is rolled back
when Go closes that connection before requesting sleep.

Both candidates expose one generation-bound `DB` with `Exec`, `Query`, and
`Begin`; `Tx` has `Exec`, `Query`, `Commit`, and `Rollback`. Values map exactly
between SQL NULL/integer/real/text/blob and Go
`nil`/`int64`/`float64`/`string`/`[]byte`. The 60-second core transaction lease
is shortened to the remaining `Begin` context deadline when one exists. Lease
expiry rolls back and returns structured code `transaction_expired`; actor
stop, socket disconnect, and runner shutdown also make the lease terminal.
Core's 128-entry transaction coordinator admission queue allows concurrent
regular operations under a shared gate, serializes transaction operations,
and gives one active transaction an exclusive gate. The Go actor dispatcher
does not add a second SQL lock, so both transports inherit those same rules.

ABI 7 adds the transport config, optional `ActorStart.sqlite_socket_path`, five
request-ID-correlated SQLite commands, and chunked `SqliteResult` events. FFI
responses are split into at most 1 MiB or 1,024-value chunks and reassembled by
Go, with a 32 MiB content ceiling. The socket v1 BARE response is one negotiated
frame and therefore has a 32 MiB frame ceiling including encoding overhead.
The shared API restricts SQL, text and blob arguments to 1 MiB, arguments and
columns to 1,024, and results to the transport's 32 MiB ceiling. Results are
fully buffered. Context cancellation abandons the Go correlation ID while a
late completion is consumed safely; neither core operation is forcibly
preempted after submission.

The socket client is pure Go. It vendors
`engine/sdks/rust/actor-runtime-socket-protocol/schemas/v1.bare` at commit
`957d4e482f404913ca1955d8ecc357533f6fd081`, implements the two-byte vbare
embedded-version prefix, BARE types, big-endian u32 frame length, hello
exchange, server `maxFrameBytes`, u32 request IDs, and connection-owned lease
keys. The upstream `SqliteExec` request accepts an unparameterized script and
returns no mutation metadata, so public parameterized `Exec` uses the protocol's
`SqliteQuery` request and discards rows while retaining `changes` and
`lastInsertRowid`.

Two pin/build deviations were required for faithful LocalNative behavior.
Pinned Depot SQLite requires `SQLITE_ENABLE_BATCH_ATOMIC_WRITE`, now set in the
workspace Cargo configuration just as it is upstream. The exact v2.3.10 Depot
query decoder also treated a valid zero-length `SQLITE_BLOB` as NULL when
`sqlite3_column_blob` returned a null data pointer. Seven source files are
vendored from the same pinned checkout and selected with a Cargo patch; the
only behavior change preserves `Blob(Vec::new())` after
`sqlite3_column_type == SQLITE_BLOB`. The dependency version and Rivet commit
do not change.

Real-engine conformance runs the same CRUD, five-type values, commit/rollback,
lease expiry, structured SQL errors, concurrency, actor isolation, KV/state
coexistence, sleep/wake, and same-data-directory engine replacement cases for
both candidates. The FFI-specific case proves a result larger than one batch
is reconstructed. The socket-specific case sleeps mid-lease and proves the
old connection and lease are dead. At this pin, an actor left live across an
abrupt standalone engine replacement remains bound to its old envoy session;
the durable SQL fixture therefore sleeps the actor, stops the runner, replaces
the engine against the same data directory, starts a new runner, and
demand-wakes the actor. This proves persisted storage recovery, not seamless
rescheduling of a live generation across a crash.
