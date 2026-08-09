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
   contract. The dylib ships as a release asset whose tag and SHA-256 are both
   pinned in the Go source (`internal/ffi/acquire.go`,
   `internal/ffi/checksums.txt`), so a given commit still loads exactly one
   library even though the bytes live outside the module.

## C ABI surface

```c
// identity / lifecycle
uint32_t rk_abi_version(void);
void     rk_string_free(RkBytes s);
void     rk_bytes_free(RkBytes b);

// runner lifecycle
// config: msgpack RunnerConfig (engine endpoint, namespace, runner name,
//         version, total_slots, actor manifest and options, log level)
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
| `ActorStart` | aid, gen, name, key, create_ts, input, persisted_state (optional; absent differs from present-empty), sqlite_socket_path? | `ActorStartResult { aid, gen, ok / error }` |
| `ActorStop` | aid, gen, reason (stop cmd / sleep intent / drain) | `ActorStopResult { aid, gen }` after handler cleanup |
| `ActorAlarm` | aid, gen, alarm_ts | `AlarmHandled { aid, gen }` |
| `ActorIntentResult` | op_id, error? | completes the matching `SetAlarm` or `SleepIntent` admission |
| `ActorScheduleResult` | op_id, operation, schedule_id?, cancelled?, schedules[], error? | completes the matching one-shot schedule create, cancel, get, or list operation |
| `ActionCall` | aid, gen, call_id, action name, timeout_ms, args (raw bytes: JSON/CBOR per client encoding), conn_id | `ActionResult { call_id, output / error }` |
| `HttpRequest` | aid, req_id, method, path, headers, body?, stream flag | `HttpResponseStart` (+ chunks) |
| `HttpRequestChunk` | req_id, body, finish | — (feeds request body reader) |
| `HttpRequestAbort` | req_id | abort handler ctx |
| `WsOpen` | aid, ws_id, path, headers, can_hibernate, resumed | `WsOpenResult { ws_id, accept / reject }` |
| `WsMessage` | ws_id, data, binary, msg_index | `WsMessageAck { ws_id, msg_index }` only when the matching `WsOpen.can_hibernate` is true |
| `WsClose` | ws_id, code?, reason? | — |
| `KvResult` | kv_id, ok payload / error | — (completes pending Go future) |
| `StatePersisted` | aid, gen, state_version | — (completes pending save) |
| `SqliteResult` | request_id, chunk_index, done, first-chunk columns, row-major typed values, rows_affected, last_insert_id?, structured error? | — (completes pending Go SQL future after ordered reassembly) |

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
| `SetAlarm` | op_id, aid, gen, alarm_ts? (null clears) | `ActorIntentResult` after the core schedule operation succeeds or fails |
| `SleepIntent` | op_id, aid, gen | `ActorIntentResult` after exact-generation admission; eventual `ActorStop` reports eviction |
| `ScheduleAfter` / `ScheduleAt` | op_id, aid, gen, action, CBOR argument array, duration or absolute timestamp | `ActorScheduleResult` with a stable schedule ID |
| `ScheduleCancel` / `ScheduleGet` | op_id, aid, gen, schedule ID | `ActorScheduleResult` with a boolean or zero/one pending record |
| `ScheduleList` | op_id, aid, gen | `ActorScheduleResult` with pending records in run order |
| `StopIntent` | aid | eventual `ActorStop` |
| `SqliteExec` / `SqliteQuery` | request_id, aid, gen, deadline_ms, SQL, typed args, lease_key? | one or more ordered `SqliteResult` events |
| `SqliteBegin` | request_id, aid, gen, deadline_ms, lease_key, timeout_ms | `SqliteResult` |
| `SqliteCommit` / `SqliteRollback` | request_id, aid, gen, deadline_ms, lease_key | `SqliteResult` |

The catalogs deliberately mirror runner-protocol concepts
(`engine/sdks/schemas/runner-protocol/v7.bare`) at one level higher — actions
and state are first-class here, whereas the wire protocol only knows KV and
tunnels. Where a name matches the wire protocol, semantics must match
`@rivetkit/engine-runner`'s handling of the same message.

## Streaming and backpressure

- HTTP/WS bodies cross as chunks (the tunnel protocol is already chunked;
  proxies map 1:1). Max chunk size fixed at 1 MiB — larger writes are split.
- FFI SQLite results are flattened row-major and split at 1 MiB of value
  content or 1,024 values, whichever comes first. Only chunk zero carries the
  columns and mutation metadata. The complete content limit is 32 MiB; Go
  buffers and reassembles it before returning `Rows`.
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
3. **Resolved in the M5 notes below.** Core persists the transport metadata and
   message indexes; the embedder must not return from a hibernatable raw-message
   callback until its handler has durably accepted the message.
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
`outbound_backpressure`. Other connections continue. Go batches concurrent
submissions up to the boundary's 1024-entry container ceiling, making the
per-connection admission bound observable under a real burst instead of
letting the native worker drain between one-command FFI calls. The pinned
core's `WebSocketSender` accepts admitted messages into its own unbounded envoy
tunnel queue and exposes no peer buffered-byte signal. The real-engine test
therefore stalls one gateway client, drives its admission queue to overflow,
requires the documented 1013 close at that client, and proves a reading peer
remains usable; the Rust queue test independently proves isolation. Go uses
the same bounded native-submit retry and 30-second backpressure ceiling as the
M3 HTTP writer, from the handler goroutine rather than the poll goroutine.

`Broadcast.payload` is one CBOR argument array. Core's native
`ActorContext::broadcast` receives those bytes directly for actor-connect
subscriptions. Raw WebSocket clients are not part of that native subscription
surface at this pin, so the FFI also sends them the binary CBOR actor-connect
event envelope `{body: {tag: "Event", val: {name, args}}}` used by the pinned
core and client. `exclude_conn` filters the matching raw `ws_id`; actor-connect
delivery remains governed by core's subscription registry. The complete
encoded frame, not only the argument bytes, is capped at 1 MiB. Direct
`Connection.Send` uses `WsSend` and does not involve subscriptions.

Incoming raw frames over 1 MiB close only their connection with code 1009 and
reason `message.incoming_too_long`. Oversized outgoing targeted sends are
rejected synchronously in Go and again by command validation; no partial frame
is emitted. Empty text and binary frames are valid. Received message indexes
and pending Go acknowledgements establish their per-connection origin from the
first frame and then require wrapping-u16 monotonicity; a receive gap closes with
`ws.message_index_skip`, and an
out-of-order acknowledgement closes with `ws.ack_out_of_order`, both using
code 1008. This bookkeeping remains process-local until M5.

Broadcast with zero live connections, including during `OnStart`, succeeds as
a no-op. Graceful actor or runner stop delivers one `WsClose`/`OnDisconnect`
per connection before `OnStop`. An `OnStop` broadcast is still accepted and
submitted before `ActorStopResult`, but delivery is best-effort because the
pinned engine can begin closing the gateway transport before graceful cleanup
finishes. If delivered, it precedes the code-1001 `actor stopped` close; the
client may instead observe that close directly. Shutdown fallback uses code
1001 with reason `runner shutting down`.

Hibernation remains an M5 marker, not an M4 behavior. M4 configures
`can_hibernate_websocket = false` and `no_sleep = true`, but faithfully carries
`WsOpen.can_hibernate`, records and acknowledges `WsMessage.msg_index`, and
decodes `WsCloseCmd.hibernate`. The close command always takes the normal
non-hibernating path in M4. Core currently persists and acknowledges a
hibernating raw message after its embedder callback returns, which happens
before the asynchronous Go handler completes; M5 must reconcile that pin
behavior with durable replay before enabling hibernation.

## M5 pin-specific notes — 2026-08-03

M5 adds `ActorAlarm`, `AlarmHandled`, `ActorIntentResult`, `SetAlarm`, and
`SleepIntent`, adds the `WsOpen.resumed` marker, and bumps the boundary ABI to
5. The shape scanner limits remain unchanged: alarm timestamps and generations
are fixed-width
integers, actor IDs use the existing string cap, and no new blob, map, or
container surface is introduced. Rust produces the M5 command and alarm-event
goldens; Go decodes and re-encodes them byte-for-byte.

The public SDK presents one durable alarm per actor. `SetAlarm` is implemented
with the pinned core's persisted one-shot schedule table using the reserved
action name `__rivet_go_alarm`, rather than with an embedder-owned timer.
Replacement and clear operations are revisioned and serialized; the latest
accepted command cancels any prior reserved schedule before optionally adding
the new timestamp. Each operation is fenced to the caller's actor generation
and reports completion through its operation ID; `Context.Schedule` and
`Context.ClearSchedule` do not report success before the native list, cancel,
and optional set operation has succeeded. At this pin alarm updates and sleep
are separate workflow signals. The engine can otherwise observe the later
sleep checkpoint first, advance its checkpoint, and discard the earlier alarm
update as stale. The FFI therefore holds each serialized alarm completion for
4 seconds: two complete 1.5-second `DatabaseKv` signal polls plus a 1-second
scheduling margin. That settlement also orders rapid replacement and clear
before the caller can submit the next mutation. Other core schedule rows are
untouched. The reserved action
cannot be registered by an actor author. When core fires it, the FFI converts
the scheduled action to `ActorAlarm`, waits for `AlarmHandled`, and replies to
core only after the Go hook and its implicit state save complete. Core owns the
SQLite schedule durability, engine alarm transport, restart resynchronization,
and wake allocation.

`SleepIntent` is fenced to the caller's generation. Its result acknowledges
that the exact generation accepted the intent, not that eviction has already
finished; waiting for eviction inside the initiating handler would deadlock
the drain policy. Rust calls `ActorContext::sleep` after work already admitted
through the proxy becomes idle, and core performs the authoritative work
drain before it emits `ActorStop`. The per-actor Go worker serializes
actions, alarms, HTTP handlers, WebSocket callbacks, and lifecycle hooks. A
handler that requests sleep therefore finishes normally before `OnStop`; a
successful action's implicit save and result, an HTTP response, or a WebSocket
message acknowledgement is submitted before the worker can process
`ActorStop`. Rust additionally reserves explicit state and alarm mutations and
does not resolve `ActorStopResult` until those accepted operations are idle.
Once stop acknowledgement begins, new actor-scoped state work is rejected.
Existing action deadlines, engine HTTP aborts, and runner-shutdown cancellation
remain the only abort paths; sleep does not add a second cancellation policy.
`ActorStop.reason` is `sleep` for engine sleep, `stop` for the actor-local
`StopIntent` panic path, and `destroy` for an engine destroy.

An alarm uses the same 60-second core action deadline and FFI correlation
deadline as an ordinary action. If an actor has no `OnAlarm`, Go returns the
structured `callback_not_found` error in `AlarmHandled`; the pump remains
alive and core receives the failure instead of a panic or silent success.

The hibernatable raw-message callback has a 60-second boundary acknowledgement
limit. If Go has not returned and acknowledged by then, the FFI closes that
connection with code 1011 and reason `ws.handler_ack_timed_out` and returns an
error to core. Go handlers remain cooperatively cancellable; the timeout does
not preempt arbitrary Go code, and the actor worker stays serialized until the
handler returns or runner shutdown cancels the generation.

Open question 3 is resolved at v2.3.10 as follows:

- core persists each hibernatable connection's gateway/request identity,
  request path and headers, client message index, server message index, and
  embedder connection-state bytes in actor SQLite;
- core restores those handles at actor startup, passes them through
  `ActorStart.hibernated`, and privately supplies the corresponding
  `CommandStartActor.hibernatingRequests` metadata to the envoy client. The
  embedder never sees or reconstructs gateway/request IDs;
- the envoy client rebuilds the raw `WebSocket` transport and invokes core's
  WebSocket callback with its restoring flag. The FFI emits
  `WsOpen { resumed: true }`, installs fresh callbacks for the new actor
  generation, and Go rebuilds its internal `Connection` without invoking the
  actor author's `OnConnect`;
- for a hibernatable incoming message, core advances and persists the client
  index and acknowledges the engine only after the embedder callback returns.
  The FFI therefore blocks that callback until the matching FIFO
  `WsMessageAck` arrives after the serialized Go handler. Core suppresses
  already-persisted replay before the callback; the FFI still validates FIFO
  acknowledgements for every message it does receive;
- a sleep intent submitted inside a WebSocket handler is admitted immediately
  but applied only after that handler's acknowledgement. Frames accepted by
  the same gateway socket in that boundary window can consequently run, in
  FIFO order, on the old generation before eviction. Frames sent after
  engine-visible sleep rehydrate the actor and run on the new generation.
  Neither set is lost or delivered twice;
- on sleep, `WsCloseCmd { hibernate: true }` detaches the outgoing queue and
  callbacks belonging to the old generation without calling
  `WebSocket::close`. Core and the engine own the hibernating transport and
  preserve the client connection across the stopped generation.

Hibernation itself does not emit `WsClose` and does not invoke `OnDisconnect`.
A real close observed while the actor is awake still invokes `OnDisconnect`.
Graceful runner cancellation is also a core sleep at this pin, so it follows
the same hibernation rule; if the shutdown grace deadline forces the runtime to
abort, the FFI closes any transport still attached to that runtime with code
1001 and reason `runner shutting down`.
At this pin a client that disappears while the actor is fully asleep can be
removed by core's startup settlement before the foreign run handler starts, so
there is no Go disconnect callback for that already-dead restored transport.
This is the pinned behavior, not a promise that a future core will hide every
sleep-time close.

Real-engine alarm checks use engine-driven timestamps and the existing
`eventually` polling discipline. The pinned engine's workflow worker polls on
a 16-second tick. Negative clear, replacement, and one-shot checks therefore
observe one complete tick plus a 5-second delivery margin. Positive cases use
20-second sleep alarms, 12-second canceled/superseded alarms, a 35-second
hibernation alarm, a 45-second replacement alarm, and a 60-second restart
alarm; they prove that at least 10 seconds remain after engine-visible sleep
where that transition is the assertion. The separate 4-second transport
settlement is derived from the 1.5-second signal poll as described above.
The 90-second alarm bound covers the requested deadline, one poll tick, the
delivery margin, and runner scheduling under `-race`; it is not a latency
claim. The 20-second WebSocket message-wake bound does not depend on the alarm
poller and covers gateway delivery plus actor allocation. No fixed sleep is
used as a success gate.

After an abrupt engine process replacement, the pinned engine does not
reliably resume an already-sleeping workflow timer until the actor is
demand-rehydrated. Restart conformance waits for a post-restart envoy ping and
the 22-second disconnect/liveness interval, rehydrates the actor once, proves
the core schedule and pre-restart state survived with no alarm delivery,
resleeps without rescheduling, and then requires the original 60-second alarm
to wake it. This is the recovery behavior verified at the pin.

## M6 production-hardening notes — 2026-08-03

M6 does not change the C ABI, the MessagePack envelope, or either catalog; ABI
5 remains current. Process-level drain adds one native lifecycle state outside
the serialized boundary. `rk_runner_shutdown` marks each actor proxy as runner
draining and closes every raw gateway WebSocket with code 1001 and reason
`runner shutting down` before asking core to stop. Closing while core's
transport remains live also makes the code-1001 policy hold on the forced
deadline path. An ordinary sleep continues to hibernate an eligible socket.
This distinction prevents a dead Go process from leaving connection state that
it can never resume.

The Go pump continues polling during drain so admitted action, HTTP,
WebSocket, alarm, state, and lifecycle completions can cross normally. New
public work is rejected; commands needed by admitted work remain available.
`RunnerStopped` is the terminal event and is required before the pump frees the
runner handle. A non-graceful drain report is returned as an error.

Native runner, error, and buffer ownership now has process-local accounting in
the Go FFI wrappers. The strict soak compares all three counts with their
pre-run baseline after the pump and engine drain. This is observability over
the existing ownership rules, not another handle or wire format.

There are no new SDK/FFI decode surfaces in M6. The soak and real-subprocess
conformance tooling decode bounded management/action JSON plus the existing
v2.3.10 CBOR actor-connect event envelope; these decoders do not cross the FFI
boundary or change its allocation limits.

## WebSocket hibernation opt-in notes — 2026-08-04

ABI 6 adds `RunnerConfig.actor_hibernate_websockets`, a map from each
registered actor name to the public `Actor.HibernateWebSockets` boolean. The
FFI validates every map key against `RunnerConfig.actor_names` and sets that
actor factory's `ActorConfig.can_hibernate_websocket` to the matching boolean,
defaulting a missing entry to false. This is the level at which pinned core
keys the behavior: core evaluates the actor config when accepting a gateway
WebSocket and supplies the resulting `WsOpen.can_hibernate` marker. The Go
pump then uses that marker when an actor stops for sleep. No per-open override
or new WebSocket event field is needed.

The false public default now matches the v2.3.10 TypeScript option schema,
Rust actor config, and core `CanHibernateWebSocket::default()`. On sleep, core
closes a non-hibernatable transport; the client receives code 1001 and reason
`actor sleeping`, and Go invokes `OnDisconnect`. With
`HibernateWebSockets: true`, the existing ABI 5 hibernation flow remains:
sleep detaches the old generation without a transport close, a later message
wakes the actor through a resumed open, and hibernation itself does not invoke
`OnDisconnect`.

Hibernatable raw messages require core and the engine to persist and
acknowledge the message index before advancing. In the recorded loopback A/B,
changing only this actor option moved Go S3 client p50 from 8.243 ms to
6.459 ms, about 1.8 ms. Applications should opt in when preserving a socket
through sleep matters more than the per-message acknowledgement cost.
For a false `WsOpen.can_hibernate`, Rust allocates no acknowledgement FIFO and
Go submits no `WsMessageAck`; pinned core also omits the engine-wire message
acknowledgement. The message callback remains actor-serialized in Go.

New decode surfaces: the ABI 6 actor-hibernation map is the only new decoder
input. Go registration and Rust startup both enforce the 1,024 actor limit,
the shape scanner accepts a valid map at that exact bound, values are booleans,
and unknown actor keys are rejected before the registry starts. It adds no
event/command variant, binary blob, nested container, or new native allocation
ownership rule. No fuzz or deliberately malformed-input test was added.

## M7 SQLite transports and ABI 7/8 — 2026-08-05

ABI 7 added the SQLite transport and command/event shapes described below. ABI
8 is now single-sourced by Rust and generated into the cbindgen header and Go
loader for all six artifacts. It adds `RunnerConfig.actor_databases`, a bounded
map from registered actor name to `Actor.Database`. Rust validates every key
against `actor_names` and enables `ActorConfig.has_database` only when both the
manifest value and a nonempty wire transport are present. Database-less and
force-disabled actors retain `ActorConfig.remote_sqlite = true`; declaring
actors select `SqliteBackend::LocalNative`, and only declaring socket actors
set `enable_actor_runtime_socket`.

`RunnerConfig.sqlite_transport` still accepts empty, `ffi`, or `socket` at the
wire boundary. Public `Config.SQLiteTransport` resolves empty to FFI before
encoding and maps `disabled` to the empty wire value, so the runner field is a
transport selection rather than an implicit all-actor database switch.

At this pin, LocalNative means the native SQLite worker and transaction
coordinator execute in the embedded runner. Its Depot VFS still obtains pages
and commits through the envoy/engine storage service, so the durability
boundary is the engine's successful Depot commit, not a runner-local file.
RemoteEnvoy sends statement execution through the envoy instead. Core compiles
both modes in this library, and the FFI command proxy could call either, but
`ActorRuntimeSocketEndpoint::provision` explicitly requires an enabled
LocalNative database. M7 therefore compares both transports against
LocalNative and does not mix backend placement into the benchmark.

### FFI-pump contract

The five SQLite commands use a u64 request ID from the pump's existing
correlation allocator. Every command carries actor ID, exact generation, and a
positive deadline in milliseconds. Rust rejects stale generations, reserves
the request before spawning core work, and returns a structured `SqliteResult`
error on timeout, SQL failure, transaction failure, shutdown, or size limit.
Go context cancellation removes the caller but retains a tombstone so a late
native completion cannot be mistaken for a wrapped request ID. Actor stop and
runner free wake all pending callers.

`SqliteExec` maps to `SqliteDb::execute` or
`SqliteTransaction::execute`; `SqliteQuery` uses the same core method and
returns rows. Begin uses `begin_transaction_with_key`, while commit and
rollback use the retained `SqliteTransaction`. Lease keys are unique per Go
process and scoped again by actor ID/generation in the FFI map. A successful
or failed terminal operation removes the retained transaction.

SQL and every text/blob argument are capped at 1 MiB, argument and column
lists at 1,024, and lease keys at 256 bytes. A complete FFI result may contain
32 MiB of columns and values. It is emitted as ordered chunks capped at 1 MiB
of value content or 1,024 values, so the unchanged Go scanner limits remain
valid. Columns, mutation count, and last-insert ID appear only on chunk zero.
Go requires chunk indexes to start at zero, remain contiguous, repeat no
columns or mutation metadata after the first chunk, and finish once. Rows must
be rectangular. The API fully buffers the reconstructed result.

### Actor Runtime Socket contract

The socket endpoint is Unix-only and generation-scoped. Rust provisions it
before `ActorStart` and sends the filesystem path in
`sqlite_socket_path`; Go opens a new connection during each generation's
startup and closes it before sleep or stop. A reconnect is therefore a new
endpoint and all prior connection-owned leases are terminal. Windows builds
support the FFI candidate but reject `SQLiteTransportSocket` during runner
configuration.

`internal/sqlitesocket/schema/v1.bare` is copied from
`engine/sdks/rust/actor-runtime-socket-protocol/schemas/v1.bare` at
`957d4e482f404913ca1955d8ecc357533f6fd081`. The pure-Go client implements only
those v1 types. Each hello and request/response payload begins with the vbare
u16 embedded version in little-endian order. The outer frame is a big-endian
u32 length. BARE fixed-width values are little-endian; variants and collection
lengths use unsigned varints. The 10-second hello must return version 1 and a
positive `maxFrameBytes`; the client honors the smaller of that value and
core's 32 MiB default.

Requests use nonzero u32 IDs with a correlation table, serialized writer, and
reader goroutine. A canceled request leaves a tombstone until its response is
consumed. Disconnect fails every live request and closes the generation's DB
handle; reconnect happens only through the next `ActorStart`. Transaction
begin sends the Go lease key plus its timeout. Other transaction operations
carry that key exactly where the upstream schema requires it, and connection
close makes core expire every active lease. Public parameterized `Exec` uses
the schema's `SqliteQuery` request because `SqliteExec` accepts only a script
and returns neither `changes` nor `lastInsertRowid`. The shared public API is
single-statement for both `Exec` and `Query`; multi-statement input returns
`sqlite_error` with statement index zero before either statement executes.

Socket responses are not chunked by the upstream protocol. Columns and all
rows, including encoding overhead, must fit one negotiated frame. The client
applies the same 1 MiB value and 1,024 parameter/column limits as the public
API, then the negotiated 32 MiB frame ceiling. Every decoder checks declared
length against bytes remaining before allocation, validates rectangular rows,
and rejects trailing bytes.

### Shared concurrency, errors, and build deviations

Both transports end at the same core `TransactionCoordinator`. It admits at
most 128 queued/in-flight entries, permits concurrent regular operations under
a shared gate, gives one active transaction the exclusive write gate, and
serializes operations within that transaction. Its default lease is 60
seconds. `Begin` shortens that to its context deadline; expiry rolls back. Go
reserves one transaction slot before transport submission, so a second Begin
returns `transaction_already_open` without creating a queued, callerless
lease. Non-transaction work waits behind the open transaction. Go adds no
transport-specific lock around ordinary SQL. Its generation lifecycle gate
rejects new calls, rolls back an open lease, waits for admitted calls, and
closes the transport before sleep or stop proceeds. Structured errors preserve
core's stable code and message plus extended SQLite code and statement index
when the failure came from a statement. FFI worker backpressure and worker
close are normalized to the socket codes `sqlite_queue_full` and
`sqlite_endpoint_closed`.

All text entering from Go must be valid UTF-8 and can contain embedded NULs.
Integer bounds, infinities, empty blobs, and NULL remain distinct and exact;
SQLite itself stores a bound NaN as NULL. The pinned Depot decoder uses lossy
UTF-8 replacement when an SQLite TEXT cell contains invalid bytes, before the
result reaches either transport.

SQL and actor state are separate engine commits. Successful SQL mutation calls
return after the Depot commit, explicit state `Save` returns after its own KV
commit, and action completion performs its state save before returning the
action result. There is no atomic transaction spanning SQL and state. Sleep
performs the Go SQL lifecycle fence first, and core does not accept
`ActorStopResult` until the actor stop callback and admitted core work finish.

LocalNative requires bundled SQLite to be compiled with
`SQLITE_ENABLE_BATCH_ATOMIC_WRITE`; `.cargo/config.toml` supplies the same flag
as the pinned upstream workspace. The pinned Depot decoder also collapsed a
zero-length BLOB to NULL when SQLite reported `SQLITE_BLOB` with a null data
pointer. The workspace Cargo patch vendors the exact v2.3.10 seven-file Depot
client and changes only that branch to return an empty blob. This is a local
pin correction, not a dependency or Rivet version change.

### Engine replacement recovery

At this pin, a LocalNative database actor left live across abrupt standalone
engine replacement stays assigned to its old envoy generation. After the
runner reconnects and the 22-second envoy liveness window passes, engine
metadata still reports that generation as connectable, gateway work returns
`503 Actor not found`, and Go receives no new `ActorStart`. The conformance
test asserts that exact negative outcome. The durable recovery fixture sleeps
the database actor first, replaces the engine against the same data directory,
then demand-wakes a higher generation and verifies its SQL rows and actor state.

New decode surfaces: ABI 7 adds the bounded `sqlite_transport` string, optional
ActorStart socket-path string, five SQLite command variants, the chunked
`SqliteResult` event, and the `SqliteValue` union to the existing MessagePack
scanner. The pure-Go socket client additionally decodes the u32 frame length,
vbare hello, `ServerFrame`/`ResponsePayload` variants, structured socket error
metadata, column strings, row/value lists, and optional last-insert ID from the
vendored BARE schema. ABI 8 adds only the bounded string-to-boolean
`actor_databases` manifest map; it adds no command, event, blob, nested
container, or native allocation ownership surface. ABI 9 adds five bounded
schedule commands and `ActorScheduleResult`. A pending record contains an ID,
action, CBOR argument array, and millisecond timestamp. Individual strings and
argument blobs retain the one MiB scanner limit, lists contain at most 1,000
records, and Rust rejects aggregate returned record data above 32 MiB before
encoding an event.

ABI 9 delegates schedule storage and wake delivery to the pinned core's
`ActorContext::after`, `at`, `cancel_schedule`, `get_scheduled_event`, and
`list_scheduled_events` methods. Mutations are exact-generation operations and
remain in the actor work gate until core has committed the SQLite schedule and
the existing four-second signal-settlement window has elapsed. A following
`SleepIntent` therefore cannot overtake an accepted schedule mutation.
Scheduled work arrives from core as the ordinary `ActorEvent::Action` path;
the Go adapter applies the same deadline, panic isolation, structured error,
and automatic state-save rules as a client-dispatched action. The reserved
`__rivet_go_alarm` row is excluded from public get/list results, so the
replaceable `OnAlarm` compatibility API remains independent.
