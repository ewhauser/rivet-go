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
| `ActorStart` | aid, gen, name, key, create_ts, input, persisted_state (optional; absent differs from present-empty) | `ActorStartResult { aid, gen, ok / error }` |
| `ActorStop` | aid, gen, reason (stop cmd / sleep intent / drain) | `ActorStopResult { aid, gen }` after handler cleanup |
| `ActorAlarm` | aid, gen, alarm_ts | `AlarmHandled { aid, gen }` |
| `ActionCall` | aid, gen, call_id, action name, timeout_ms, args (raw bytes: JSON/CBOR per client encoding), conn_id | `ActionResult { call_id, output / error }` |
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

1. **Resolved in the M2 notes below.** Core supports explicit state save, so
   `SaveState` and `StatePersisted` remain in the boundary.
2. **Resolved in the M3 notes below.** The pinned core normalizes action args
   to CBOR before the embedder callback, so the boundary passes those bytes
   through unchanged.
3. Hibernating WS resume: how much of the `msg_index` replay bookkeeping does
   core hide from its embedder vs expect the embedder to persist? Check what
   rivetkit-napi exposes to JS.
4. Whether `rk_runner_poll` should also carry a wakeup fd/eventfd alternative
   for integration with Go netpoller — only if the pinned thread ever shows up
   as a real cost.

## M1 pin-specific notes — 2026-08-02

Rivet `v2.3.10` names the `rivetkit-core` registration transport “envoy.” The
FFI contract deliberately keeps its language-neutral `Runner*` vocabulary, but
the concrete mappings at this pin are:

- core transport: `/envoys/connect`, `rivet-envoy-protocol` v6, and
  `ToRivetMetadata`/`ToEnvoyInit`;
- management assertion: active `GET /envoys` entry rather than the legacy
  `GET /runners` resource;
- `RunnerConnected.runner_id`: the engine-visible `envoy_key` from that
  management entry;
- `RunnerConnected.metadata`: string metadata that records the management
  resource, concrete protocol, runner name, configured log level, and the
  engine metadata JSON when present.

The supported `CoreRegistry` embedding API exposes version and pool/name but
does not expose `total_slots`. `RunnerConfig.total_slots` is still required and
validated so the boundary does not churn before M2, but it is not transmitted
by the v2.3.10 core adapter. With an empty actor manifest the M1 registration
advertises no actors.

The M1 `RunnerStopped` drain report is
`{ graceful, elapsed_ms, actors_stopped, actors_remaining }`. The M1 command
catalog is empty: an empty `CommandBatch` is accepted, while every non-empty
batch is rejected as `unknown_command` before enqueue.

## M2 pin-specific notes — 2026-08-03

M2 expands the event and command unions and therefore bumps the boundary ABI
to 2. `RK_ABI_VERSION` in Rust is the source of truth; cbindgen writes it to the
committed header, and `scripts/build-ffi.sh` derives Go's generated
`ExpectedABIVersion` from that header. The loader rejects every other value.

Open question 1 is resolved: core supports explicit state persistence. The
adapter maps `SaveState { aid, gen, state }` to
`ActorContext::save_state(vec![StateDelta::ActorState(state)])` and emits
`StatePersisted` only after that future completes. `StatePersisted` carries an
optional structured error in addition to `state_version`, so a failed save
completes the Go future without stopping the pump. There is no automatic
save-on-hook-return behavior in M2.

Actor factories use `ActorFactory::new_with_manual_startup_ready` and are
created from the sorted Go registry manifest. The concrete lifecycle mapping
at `v2.3.10` is:

- `aid` comes from `ActorContext::actor_id()`;
- `gen` comes from the actor SQLite runtime configuration and is widened from
  core's `u32` to the boundary's `u64`;
- `name`, formatted `key`, `input`, and `persisted_state` are available from
  `ActorContext`/`ActorStart`;
- `create_ts` is not exposed by `ActorStart` or `ActorContext`, so the cataloged
  field remains present and uses `0` as a documented unknown sentinel;
- core reduces envoy stop causes to `ShutdownKind::Sleep` or
  `ShutdownKind::Destroy` before invoking the factory, so `ActorStop.reason`
  is `sleep` or `destroy`; the original going-away/lost distinction is not
  recoverable at this trait boundary.

`ActorStopResult` has an optional structured error arm. A Go `OnStop` error or
panic is returned through core's lifecycle reply and the actor factory's
structured failure path; a successful result retains the cataloged no-payload
shape. At `v2.3.10`, a destroy already requested through the management API
still completes as destroyed even when the graceful-cleanup hook fails, so the
engine-visible assertion is the stopped/destroyed actor plus continued runner
health. The precise `handler_panic` arm is asserted at the FFI command boundary.
The same structured-error convention applies to `ActorStartResult`, whose
startup failure is retained by the engine as the actor error.

State-save completion is bounded and ordered. Rust reserves a save before
spawning its core future, rejects new saves once stop acknowledgement begins,
times the core future out after 30 seconds, and waits for every reserved save
before resolving `ActorStopResult`. Go waits up to 35 seconds for
`StatePersisted`, returns context cancellation/deadline as a structured actor
error, and consumes a late completion before allowing another save for that
actor generation. Pending KV callers are woken on actor stop and runner free.

`ActorStart.persisted_state` is encoded as an optional byte string. `None`
means the actor has no prior state; `Some([])` means a zero-length state was
persisted and must be passed to a custom decoder. Go's copy helpers preserve
that distinction.

The public actor KV methods at this pin are deprecated in name but still
supported and backed by the actor SQLite database. `KvGet`, `KvList`, `KvPut`,
and `KvDelete` therefore map directly to `ActorContext::kv()`. KV list requests
and results are capped at 1024 entries. The Go MessagePack shape scanner uses
the same 1024-entry array ceiling; its remaining-bytes rule remains the primary
allocation guard. KV correlation IDs start at 1, skip zero and every live ID on
wrap, and may be reused only after completion.

State and KV use core's `sqlite-remote` feature with
`ActorConfig.remote_sqlite = true`, which executes storage through the
engine/envoy and persists it in the engine data directory. The native-local
backend requires an atomic-write-enabled SQLite build at this pin and is not
used by the prebuilt Go library.

## M3 pin-specific notes — 2026-08-03

M3 expands both envelope unions and bumps the boundary ABI to 3. The runner
configuration now carries `actor_actions`, keyed by registered actor name, and
the core `ActorConfig` advertises each `ActionDefinition`. Action names are
non-empty, unique within an actor, and capped at 1024. HTTP request and
response header maps are capped at 256 entries. The Go shape scanner keeps its
existing 1024-entry map ceiling because it must contain the outer envelope as
well as the schema-capped header map; its existing 1 MiB blob ceiling exactly
matches one HTTP boundary chunk, so no scanner cap changes were needed.

The action mapping at v2.3.10 is:

- a gateway JSON or CBOR request is normalized by core to a CBOR argument
  array, which `ActionCall.args` carries without transcoding;
- `conn_id` carries the ephemeral HTTP action connection ID when core supplies
  it; the required connection preflight and open hooks are acknowledged but
  no public connection surface is introduced in M3;
- `ActionResult.output` is one CBOR value, while the error arm becomes a
  dynamic Rivet error in group `actor`; `action_not_found` uses core's native
  404 error constructor;
- action arguments and results are single, non-streamed boundary blobs and
  retain the existing 1 MiB envelope limit;
- the action correlation uses the pinned core default 60-second action
  deadline. Expiry and runner shutdown resolve the same correlation table as
  lifecycle and state operations.

The public typed adapter uses `fxamacker/cbor` for arguments and results and
honors Go `json` field tags. `RawAction` is the byte-level escape hatch for the
unchanged CBOR argument array and result value. A successful typed or raw
action saves the complete actor state before releasing its output. Ordinary
handler errors remain actor-local; `handler_panic` is returned to the client
and then ends that actor factory future without ending the runner pump.

`ActionCall.timeout_ms` is sourced from the same Rust duration installed in
`ActorConfig.action_timeout` (60 seconds at this pin). Go applies it to the
context passed to `ActionWithContext` and `RawActionWithContext`. Cancellation
is cooperative because Go cannot preempt a handler that ignores its context.
Core still returns the structured `actor/action_timed_out` response at the
deadline; a late `ActionResult` is ignored without consuming another call's
correlation, and the actor can serve later work after the handler exits.

The HTTP mapping has two limitations imposed by the pinned core trait. Core
buffers the incoming body before creating `ActorEvent::HttpRequest`, and the
reply channel accepts only `Response<Vec<u8>>`. The FFI divides the buffered
request into `HttpRequest` plus `HttpRequestChunk` events of at most 1 MiB and
accepts `HttpResponseStart` plus equally bounded response chunks, but it must
assemble all response chunks before replying to core. A 30-second boundary
deadline applies because this callback exposes no separate core deadline.
Deadline expiry emits `HttpRequestAbort`; actor or runner shutdown cancels the
Go request context directly. Client socket aborts are not visible through this
v2.3.10 embedder callback and therefore cannot produce their own abort event.
Core serializes an actor stop behind its active request callback. Runner
shutdown remains bounded by the runner drain deadline; when `RunnerStopped`
arrives, Go cancels every actor worker, wakes request-body reads, and removes
the corresponding request entries. Expired Rust request correlations are
removed before `HttpRequestAbort` is emitted.

Go's `ResponseWriter.Write` splits large writes and calls `rk_runner_submit`
from the handler goroutine. Native `backpressure` responses are retried with
jitter there for at most 30 seconds, leaving the sole poll goroutine free to
dispatch other actors. The writer locks status and headers at the first write,
serializes concurrent `Write` calls, rejects writes after `OnFetch` returns,
and checks an explicit `Content-Length` against the bytes written. It does not
implement `http.Flusher`: core cannot expose incremental response arrival at
this pin because the complete body is assembled before reply.

The pinned core request and response types use `HashMap<String, String>`, so
the boundary preserves one value per header name. Repeated request fields are
reduced by v2.3.10 to the last value before Go receives them. Go joins repeated
response values with a comma and space when that representation is valid, but
rejects multiple `Set-Cookie` values with the structured
`http_response_repeated_header_unsupported` error instead of corrupting them.
Maps that reach the boundary with over 256 names fail structurally. The public
v2.3.10 gateway rejects an over-limit incoming request earlier with HTTP 431,
before an actor event exists. Header values are not subject to that entry-count
limit; a large Cookie value remains one entry. Header names and values retain
the envelope's 1 MiB per-blob ceiling and fail structurally before crossing it.

WebSockets, event broadcast, alarm delivery, sleep scheduling, and the
standalone client package remain outside M3.

## M4 pin-specific notes — 2026-08-03

M4 adds `WsOpen`, `WsMessage`, and `WsClose` events plus `WsOpenResult`,
`WsMessageAck`, `WsSend`, `WsCloseCmd`, `Broadcast`, and `StopIntent` commands,
and bumps the boundary ABI to 4. The existing MessagePack shape scanner limits
do not change: its 1 MiB blob limit is the WebSocket message maximum, its 1024
entry map limit contains the schema-capped 256-header open map, and every new
identifier and close reason is smaller than an existing string limit. Rust
goldens cover every M4 event and command kind, including the fields that are
carried now for M5.

The raw WebSocket mapping at v2.3.10 is:

- `ActorEvent::WebSocketOpen` supplies the `ConnHandle`, `WebSocket`, and
  optional request used to emit `WsOpen`; its reply waits up to 30 seconds for
  `WsOpenResult`, and a rejected Go `OnConnect` follows core's structured raw
  WebSocket failure path;
- the core connection ID is the stable `ws_id` for one actor generation;
  request path and the pinned core's single-value header map cross unchanged;
- core supplies a `u16` client message index for every raw message. Rust
  records it before `WsMessage`, and Go submits `WsMessageAck` after the
  serialized handler returns;
- the raw close callback supplies the peer close code and reason. Actor stop,
  runner shutdown, and an actor-issued close remove the registry entry first,
  emit one `WsClose`, and make later core close notifications harmless;
- all messages are complete text or binary WebSocket frames. A frame may be at
  most 1 MiB and is not split, because splitting would change WebSocket message
  semantics. Text must be valid UTF-8.

Each raw connection has a 64-command FFI-owned outbound queue. `WsSend` and
each raw recipient of `Broadcast` use non-blocking `try_send`; if that queue is
full, only that connection is removed and closed with code 1013 and reason
`outbound_backpressure`. Other connections continue. The pinned core's
`WebSocketSender` accepts into its own unbounded envoy tunnel queue and exposes
no write-completion or buffered-byte signal, so the FFI cannot measure a
gateway peer's socket backlog after core accepts a send. The real-engine test
therefore proves that an unread peer does not delay a reading peer, while the
Rust queue test deterministically proves the close-on-overflow policy. Go uses
the same bounded native-submit retry and 30-second backpressure ceiling as the
M3 HTTP writer, from the handler goroutine rather than the poll goroutine.

`Broadcast.payload` is one CBOR argument array. Core's native
`ActorContext::broadcast` receives those bytes directly for actor-connect
subscriptions. Raw WebSocket clients are not part of that native subscription
surface at this pin, so the FFI also sends them a binary CBOR map shaped as
`{event, args}`. `exclude_conn` filters the matching raw `ws_id`; actor-connect
delivery remains governed by core's subscription registry. Direct
`Connection.Send` uses `WsSend` and does not involve subscriptions.

Hibernation remains an M5 marker, not an M4 behavior. M4 configures
`can_hibernate_websocket = false` and `no_sleep = true`, but faithfully carries
`WsOpen.can_hibernate`, records and acknowledges `WsMessage.msg_index`, and
decodes `WsCloseCmd.hibernate`. The close command always takes the normal
non-hibernating path in M4. Core currently persists and acknowledges a
hibernating raw message after its embedder callback returns, which happens
before the asynchronous Go handler completes; M5 must reconcile that pin
behavior with durable replay before enabling hibernation.
