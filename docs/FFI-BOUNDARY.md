# FFI boundary specification

The contract between `crates/rivetkit-go-ffi` (Rust, embeds rivetkit-core) and
`internal/ffi` + `internal/pump` (Go, purego). Companion to [PLAN.md](PLAN.md).

## Design principles

1. **No native→Go callbacks.** purego callbacks from foreign (tokio) threads are
   the one genuinely fragile FFI mechanism; gomonty ships without a single
   `purego.NewCallback` and so do we. All spontaneous work from rivetkit-core
   surfaces as *events* pulled by Go.
2. **Coarse-grained calls, serialized payloads.** Few extern functions; all
   structured data crosses as msgpack (`vmihailenco/msgpack` ↔ `rmp-serde`).
   The per-call FFI overhead is dwarfed by payload work, and the surface stays
   evolvable without ABI breaks.
3. **Handles, never shared structs.** Rust state is behind opaque handles;
   buffers crossing the boundary have explicit ownership rules (below).
4. **The boundary is versioned.** `runner_abi_version()` returns a monotonic
   integer; Go refuses to load a dylib with an unexpected version. The msgpack
   envelope carries no per-message version — the ABI version covers the whole
   contract, and dylib + Go code ship together in one module.

## C ABI surface

```c
// identity / lifecycle
uint32_t rk_abi_version(void);
void     rk_string_free(RkBytes s);
void     rk_bytes_free(RkBytes b);

// runner lifecycle
// config: msgpack RunnerConfig (engine endpoint, namespace, runner name,
//         version, total_slots, actor name manifest, log level)
RkRunnerResult rk_runner_new(const uint8_t* config, uintptr_t config_len);
void           rk_runner_free(RkRunner* r);

// the pump
// Blocks up to timeout_ms; returns msgpack EventBatch (possibly empty).
// AT MOST ONE concurrent caller per runner (enforced: second caller gets error).
RkPollResult   rk_runner_poll(RkRunner* r, uint32_t timeout_ms);

// Enqueue msgpack CommandBatch. Thread-safe; callable from any goroutine.
// Returns immediately after enqueue (bounded queue; Backpressure error when full).
RkSubmitResult rk_runner_submit(RkRunner* r, const uint8_t* batch, uintptr_t len);

// Graceful drain: sends ToServerStopping, waits for actors to stop or deadline.
// Poll continues delivering events during drain; final event is RunnerStopped.
RkSubmitResult rk_runner_shutdown(RkRunner* r, uint32_t deadline_ms);
```

Result structs follow the gomonty convention — `{ payload: RkBytes, err: RkError* }`
with `rk_error_json()` / `rk_error_free()` accessors. Every extern fn body is
wrapped in `catch_unwind`; a Rust panic becomes an `internal_panic` error, never
an abort across the boundary.

## Ownership rules

- **Rust→Go buffers** (poll results, error JSON): owned by Go after return; Go
  copies into GC memory immediately, then calls `rk_bytes_free`. The pump does
  this in one place so leaks are structurally impossible outside it.
- **Go→Rust buffers** (config, submit batches): borrowed for the duration of the
  call only; Rust copies before return. Go keeps the slice alive across the call
  (`runtime.KeepAlive`), nothing more.
- **Handles**: freed exactly once by the owning Go object's `Close`; Go wrappers
  use a `sync.Once` + finalizer-as-backstop (finalizer logs — it firing is a bug).

## Threading model

- Rust: `rk_runner_new` spawns a dedicated tokio multi-thread runtime owned by
  the runner handle; all rivetkit-core activity lives there. No signal handlers,
  no thread-local assumptions visible to Go.
- Go: exactly one *pump goroutine* calls `rk_runner_poll` in a loop
  (`LockOSThread`; the blocking call pins the thread — by design, same cost as
  gomonty's dispatch loop). It decodes batches and dispatches to per-actor
  goroutines. Any goroutine may call `rk_runner_submit`.
- Shutdown ordering: `rk_runner_shutdown` → pump drains until `RunnerStopped`
  event → pump exits → `rk_runner_free`.

## Envelope

```
EventBatch   = { seq: u64, events:   [Event] }    // Rust → Go
CommandBatch = { commands: [Command] }            // Go → Rust
```

`seq` is per-runner monotonic, for debugging/metrics only — delivery is in-order
and exactly-once *within a process* (durability across process death is core's
checkpoint machinery, invisible to Go). Correlation is per-domain via explicit
IDs (below), not envelope-level request/response.

## Event catalog (Rust → Go)

Grouped by the rivetkit-core trait whose proxy emits them. `aid` = actor
instance ID, `gen` = generation; both assigned by core.

| Event | Payload | Go must eventually submit |
|---|---|---|
| `RunnerConnected` | runner_id, engine metadata | — |
| `RunnerDisconnected` | reason; core auto-reconnects | — |
| `RunnerStopped` | drain report | — (pump exits) |
| `ActorStart` | aid, gen, name, key, create_ts, input, persisted_state | `ActorStartResult { aid, gen, ok / error }` |
| `ActorStop` | aid, gen, reason (stop cmd / sleep intent / drain) | `ActorStopResult { aid, gen }` after handler cleanup |
| `ActorAlarm` | aid, gen, alarm_ts | `AlarmHandled { aid, gen }` |
| `ActionCall` | aid, gen, call_id, action name, args (raw bytes: JSON/CBOR per client encoding), conn_id | `ActionResult { call_id, output / error }` |
| `HttpRequest` | aid, req_id, method, path, headers, body?, stream flag | `HttpResponseStart` (+ chunks) |
| `HttpRequestChunk` | req_id, body, finish | — (feeds request body reader) |
| `HttpRequestAbort` | req_id | abort handler ctx |
| `WsOpen` | aid, ws_id, path, headers, can_hibernate | `WsOpenResult { ws_id, accept / reject }` |
| `WsMessage` | ws_id, data, binary, msg_index | `WsMessageAck { ws_id, msg_index }` (hibernation bookkeeping) |
| `WsClose` | ws_id, code?, reason? | — |
| `KvResult` | kv_id, ok payload / error | — (completes pending Go future) |
| `StatePersisted` | aid, gen, state_version | — (completes pending save) |

## Command catalog (Go → Rust)

| Command | Payload | Completed by |
|---|---|---|
| `ActorStartResult` / `ActorStopResult` / `AlarmHandled` / `ActionResult` | see above | — |
| `HttpResponseStart` | req_id, status, headers, body?, stream | — |
| `HttpResponseChunk` | req_id, body, finish | — |
| `WsSend` | ws_id, data, binary | — |
| `WsCloseCmd` | ws_id, code?, reason?, hibernate | — |
| `Broadcast` | aid, event name, payload, exclude_conn? | — |
| `SaveState` | aid, gen, state bytes | `StatePersisted` |
| `KvGet` / `KvList` / `KvPut` / `KvDelete` | kv_id, aid, op payload | `KvResult` |
| `SetAlarm` | aid, alarm_ts? (null clears) | — |
| `SleepIntent` / `StopIntent` | aid | eventual `ActorStop` |

The catalogs deliberately mirror runner-protocol concepts
(`engine/sdks/schemas/runner-protocol/v7.bare`) at one level higher — actions
and state are first-class here, whereas the wire protocol only knows KV and
tunnels. Where a name matches the wire protocol, semantics must match
`@rivetkit/engine-runner`'s handling of the same message.

## Streaming and backpressure

- HTTP/WS bodies cross as chunks (the tunnel protocol is already chunked;
  proxies map 1:1). Max chunk size fixed at 1 MiB — larger writes are split.
- `rk_runner_submit` queue is bounded (default 1024 commands); a full queue
  returns a `Backpressure` error and the Go writer retries with jitter — this
  propagates engine-side tunnel backpressure to `http.ResponseWriter.Write`.
- Poll side is naturally bounded: core's internal queues fill and it stops
  reading from the socket, which is the same backpressure story the napi
  binding has.

## Correlation and in-flight bookkeeping (Rust side)

Each proxy call from core (action invocation, state load, etc.) allocates an ID,
enqueues the event, and parks on a oneshot keyed by that ID; `rk_runner_submit`
resolves it. Timeouts: every parked call carries core's own deadline where one
exists (action timeout, actor stop threshold from `ToClientInit` metadata),
else a configurable default (30 s) — expiry resolves the oneshot with a timeout
error so core's normal failure path runs. A `runner_free` drains all parked
calls with a shutdown error. This table (id → oneshot, deadline) is the single
most bug-prone structure in the crate; it gets dedicated loom/proptest coverage.

## Open questions (resolve during M1–M2)

1. Does core's actor trait let state save be explicit per-action (our
   `SaveState` command), or does it snapshot implicitly? Determines whether
   `StatePersisted` exists or `ActionResult` implies persistence.
2. Action args encoding: pass through raw client encoding (JSON/CBOR) vs
   normalize to msgpack in the FFI crate. Leaning pass-through — Go decodes
   per-request, avoiding a double transcode.
3. Hibernating WS resume: how much of the `msg_index` replay bookkeeping does
   core hide from its embedder vs expect the embedder to persist? Check what
   rivetkit-napi exposes to JS.
4. Whether `rk_runner_poll` should also carry a wakeup fd/eventfd alternative
   for integration with Go netpoller — only if the pinned thread ever shows up
   as a real cost.
