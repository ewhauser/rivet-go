# Operations

This runbook applies to rivet-go with Rivet Engine `v2.3.10`, commit
`957d4e482f404913ca1955d8ecc357533f6fd081`.

## Process lifecycle and drain

`rivet.Serve` listens for `SIGINT` and `SIGTERM`. Canceling the context passed
to `Registry.Serve` follows the same path. The default graceful deadline is 10
seconds and can be changed with `Config.ShutdownTimeout`.

Once drain begins, new public submissions are rejected while commands needed
by already-admitted handlers remain available. Actions, alarms, and HTTP
handlers that already started finish in actor-serial order when they fit inside
the deadline; their accepted state and alarm operations are fenced before
`ActorStopResult`. Raw gateway WebSockets close immediately with code 1001 and
reason `runner shutting down` while core's transport is still able to deliver
the close frame. An already-admitted WebSocket handler may finish internally,
but the terminating process does not keep the client transport open for a late
reply. Ordinary actor sleep hibernates sockets only when the actor definition
sets `HibernateWebSockets: true`; the default closes them with code 1001 and
reason `actor sleeping`. The native runner then emits `RunnerStopped`, the pump
closes its handle, and a clean drain returns nil so `main` exits with code zero.

If the deadline expires, core aborts the remaining runtime work and reports a
non-graceful drain. In-flight action and HTTP clients must not observe success,
raw WebSockets have already received 1001, `Registry.Serve` returns an error,
and the runnable examples exit with code 1. Treat that forced path as a
production incident even when the process must continue exiting.

## Actor clients and cross-actor calls

`rivet.NewClient` creates an immutable external HTTP client. `Context.Client`
returns the same client boundary scoped to the current actor generation. The
scoped client inherits `Config.Endpoint`, `Namespace`, `RunnerName`, `Token`,
`Headers`, and `HTTPClient`; headers are cloned during construction. Set the
token explicitly in production. The local development launcher uses `dev`.

Creation input is opaque bytes: `CreateOptions.Input` is base64-wrapped for the
Engine management API and delivered unchanged by `Context.Input`. Key segments
use Rivet's stable escaping format. A nil `CreateOptions.Key` creates an
unkeyed actor, while a non-nil empty key is a keyed actor with zero segments.
`GetOrCreate` always uses a key and atomically resolves or creates it in the
Engine.

Every request honors its `context.Context`. A custom `Config.HTTPClient` or
`ClientConfig.HTTPClient` can enforce transport-wide timeouts and connection
pool policy. Non-success Engine responses become `*rivet.ClientError` with
HTTP status, group, code, message, metadata, actor generation details, and ray
ID when present. `errors.Is(err, rivet.ErrActorNotFound)` handles both an empty
resolution and a structured `actor/not_found` response.

An actor keeps its serial action slot while waiting for an actor-to-actor call.
The SDK rejects a direct call to the same actor with `rivet.ErrSelfCall`, but it
cannot detect a longer cycle such as A calling B while B calls A. Design call
graphs without cycles and propagate the action context so deadlines cancel the
whole chain. Each action call is one HTTP request and is not silently replayed
across lifecycle transitions.

Multi-actor work is not transactional. A remote action may commit before the
calling actor saves its own state. Store an idempotency key or durable workflow
state when partial failure must be recovered rather than merely reported.

## Per-actor SQLite

SQLite is an actor capability. Declare it only on actor types that use
`Context.DB`:

```go
rivet.Actor[State]{
    Database: true,
    // ...
}
```

`Config.SQLiteTransport` selects one transport for all declaring actors. Its
empty value defaults to `rivet.SQLiteTransportFFI`. Set
`rivet.SQLiteTransportSocket` to use the experimental Unix socket path, or
`rivet.SQLiteTransportDisabled` to force-disable databases for every actor.
`RIVET_GO_SQLITE_TRANSPORT=ffi|socket|disabled` overrides the config for a
complete runner invocation.

An actor without `Database: true`, or any actor under the disabled transport,
keeps the RemoteEnvoy state/KV backend. Its `ctx.DB()` handle returns the
structured `sqlite_transport_not_configured` error and points to the actor
declaration in its message.

| Build target | FFI SQLite (default) | Socket SQLite |
|---|---|---|
| macOS arm64 | Supported | Supported |
| Linux amd64/arm64, glibc or musl | Supported | Supported |
| Windows amd64 | Supported | Rejected during runner configuration because the endpoint is Unix-only |
| Other targets, including macOS amd64 | Unsupported-platform runner stub | Unsupported-platform runner stub |

Both active choices run core's LocalNative SQLite worker against the same
engine-backed per-actor Depot storage. `ffi` sends correlated commands through
the existing MessagePack pump and reconstructs chunked results. `socket` uses
core's experimental generation-scoped Unix endpoint and is unavailable on
Windows. The endpoint is replaced on every wake; a transaction from an older
generation or connection cannot be reused.

Transactions lease core's exclusive per-actor database gate for 60 seconds by
default. A deadline on `Begin` shortens the lease. Expiry, actor sleep, socket
disconnect, or runner shutdown rolls the transaction back and makes its `Tx`
terminal. Keep transactions short and do not call the outer `DB` while holding
one: regular operations wait behind the active transaction and may consume the
caller deadline. A second `Begin` on the same generation returns
`transaction_already_open` immediately. Sleep and stop reject new SQL, roll
back the open lease, wait for already-admitted calls, and close the transport
before lifecycle completion.

Public `Exec` and `Query` each accept one SQL statement. Both transports reject
a multi-statement string as `sqlite_error` before any statement runs. Valid
UTF-8 text may contain embedded NULs; invalid UTF-8 text arguments are rejected.
SQLite converts bound NaN values to NULL, while infinities remain REAL values.
Invalid bytes already stored with SQLite TEXT affinity are returned with UTF-8
replacement by the shared pinned Depot decoder.

SQL results are fully buffered. SQL text, text values, and blob values are
limited to 1 MiB each; requests accept at most 1,024 arguments and results at
most 1,024 columns. FFI results have a 32 MiB content limit and cross in 1 MiB
chunks. Socket results must fit one negotiated frame capped at 32 MiB including
protocol overhead. Large result APIs should paginate explicitly.

SQL commits and actor-state saves are durable but separate; there is no atomic
transaction spanning them. A SQL mutation returns after its Depot commit, and
`Context.Save` returns after its state commit. Normal action completion also
saves state before releasing the action result. Encoded action results and
ActorConnect connection state are each limited to 1 MiB; the Go adapter rejects
oversized values as actor-local structured errors before they reach the shared
native runner. `ActorStopResult` follows the
actor stop callback, DB close, and core's admitted-operation fence.

## Metrics and logging

`Config.Hooks` accepts a concurrency-safe implementation of:

```go
type Hooks interface {
    Counter(name string, delta int64)
    Gauge(name string, value int64)
    ObserveDuration(name string, value time.Duration)
}
```

Stable metric names are exported as `rivet.Metric*` constants:

| Kind | Name | Meaning |
|---|---|---|
| Counter | `events_polled_total` | decoded events received from native batches |
| Counter | `commands_submitted_total` | commands accepted by the native queue |
| Counter | `backpressure_hits_total` | native submit attempts rejected for retry |
| Counter | `actor_starts_total` | actor generations admitted |
| Counter | `actor_stops_total` | actor stop events admitted |
| Counter | `actor_panics_total` | recovered user-hook panics |
| Gauge | `live_actors` | actor workers currently owned by the pump |
| Gauge | `live_connections` | raw WebSockets currently owned by the pump |
| Duration | `poll_latency` | wall time of event-bearing native polls; intentional empty timeout polls are excluded |

Hooks are serialized on a dedicated dispatcher outside pump state locks and
the poll, submit, and actor goroutines. A hook may call back into the SDK, and a
briefly blocking hook does not stall event or command progress. Hooks should
still return quickly because the dispatcher queue is drained during shutdown.
A hook panic is recovered and logged; it does not stop the runner. The counter
example's `-metrics-address` option is a complete `expvar` implementation.

Set `Config.Logger` to a configured `*slog.Logger` for Go-side lifecycle,
disconnect, drain, actor, panic, and leak-backstop records. Nil discards these
records. `Config.LogLevel` configures the embedded Rust runtime separately.

## Chaos soak

The normal test suite does not start the soak. The command's two-minute
default is the CI smoke profile:

```sh
go run ./cmd/soak
```

Run the release-candidate profile for 24 hours from a clean checkout:

```sh
go run ./cmd/soak \
  -duration=24h \
  -intensity=32 \
  -clients=12
```

Record the printed seed and complete final summary with the release evidence.

Resolved intermittent (observed 2026-08-03/04/05, root-caused and fixed
2026-08-07): rare exit-1 soak failures — one `-duration=2m -intensity=4` run
(2026-08-03) and default-profile runs (2026-08-04/05) — each passing on
immediate rerun, roughly one in ten short runs. Captured with full output on
the default profile (2026-08-05, and 2026-08-07 at seed
`1785863143857976000` plus a same-seed replay): the engine-restart chaos
cycle's demand rehydration (`resleep` on the sleeping alarm actor) failed
spuriously — as plain `503 Actor not found` or as the structured
`actor/stopping` transition against the mid-teardown stale generation. Root
cause: the replacement engine process completes workflow-worker failover
about 30 seconds after start — beyond the 22-second generation liveness
settlement — and a wake-requiring gateway request issued in that gap parks in
route dispatch. It usually completes once failover lands, but at the failover
boundary it can instead resolve to one of those signatures even though the
actor's persisted state is intact (its next-generation SQLite preload
succeeds moments later in the same engine log). This was a harness robustness
gap against pinned-engine recovery behavior, not an SDK defect; the SDK never
observes the failed request. The harness now retries exactly the transient
signatures — plain `503 Actor not found`, `actor/stopping`, and
`actor/not_ready` — for that one rehydration probe within its 30-second
budget (`gatewayActionEventually` in `cmd/soak/gateway.go`); a successful
probe implies failover has completed before the workload resumes, and every
strict oracle still runs unchanged afterward. The 2026-08-03 failure's output
was never captured; it is attributed to the same signature because the
restart cadence is identical at both intensities, but that attribution is
presumed, not proven. Always redirect soak output to a file and preserve the
printed summary and failure data directory from any failing run.
The harness fails if any required chaos knob did not activate, if its Go truth
model differs field-by-field from engine-persisted actor state, if any live
client sees a missing or duplicate broadcast, if sequence values regress, or
if the final goroutine and native-handle counts do not return to baseline.

The mandatory activations are engine replacement using the same data
directory, mid-stream client disconnect, sleep/wake churn, a stalled WebSocket
client, a sacrificial action panic, and database inserts reconciled against an
independent row-count oracle. Other soak actors remain database-less, so the
same run covers both capability classes. A successful duration alone is not a
pass.

The soak gives its final runner drain 60 seconds so actors accumulated across
a long restart history can stop across multiple pinned 16-second workflow
ticks. This is a harness setting, not a change to the SDK's 10-second default;
successful and forced application deadlines are verified separately by process
conformance.

The seed is printed before engine startup and selects deterministic counter
value streams and per-producer intent sequences. Reusing the seed and flags
replays those streams and the fixed chaos cadence. A default temporary data
directory is printed and preserved on failure, with the engine log path, but
removed after a successful run. An explicit `-data-dir` is always left under
the operator's ownership.

For a failed run:

1. Preserve the seed, command line, summary, and engine log path.
2. Re-run the same seed and duration before changing intensity.
3. Classify the first strict-oracle failure; later disconnects are often
   consequences.
4. Keep the data directory until state and engine-ordered lists have been
   compared after sorting both sides in Go.

## Engine pin upgrade

Engine upgrades are deliberate and never happen through a floating Cargo or
download dependency.

1. Choose one stable Rivet tag and record its full commit in
   `docs/PINNED-VERSION.md`, `Cargo.toml` and `Cargo.lock`,
   `internal/devengine/devengine.go`, `README.md`, conformance documentation,
   and user-facing limitations. Update both pinned-engine cache paths and keys
   in `.github/workflows/verify.yml`. A repository-wide search for the old tag
   and full commit must return only intentionally retained historical reviews.
2. Read upstream core, envoy, gateway, storage, alarm, and WebSocket changes.
   Update `docs/FFI-BOUNDARY.md` before changing the boundary. Bump the ABI for
   any event, command, or configuration shape change.
3. Build all six release targets with Rust 1.97, cbindgen 0.29.4, Zig 0.16,
   cargo-zigbuild 0.23, and cargo-xwin 0.23 where applicable:

   ```sh
   scripts/build-ffi.sh aarch64-apple-darwin
   scripts/build-ffi.sh x86_64-unknown-linux-gnu
   scripts/build-ffi.sh x86_64-unknown-linux-musl
   scripts/build-ffi.sh aarch64-unknown-linux-gnu
   scripts/build-ffi.sh aarch64-unknown-linux-musl
   scripts/build-ffi.sh x86_64-pc-windows-msvc
   ```

   The artifacts themselves are not committed; each build updates that
   platform's SHA-256 line in `internal/ffi/checksums.txt` (committed),
   regenerates `THIRD-PARTY-NOTICES.md`, and seeds the local acquisition
   cache so this machine never downloads what it just built.
4. Run `scripts/build-ffi.sh` for the same six targets again and require a
   clean diff. This is the idempotence check.
5. Run `cargo test --workspace`, Clippy with warnings denied, `go vet ./...`,
   `go test -race -count=1 ./...`, the checked-in conformance suite, and the
   24-hour soak. Preserve the existing corpus for the separately owned fuzz
   handoff; the upgrade itself does not add malformed-input cases.
6. Compare behavior against the prior pin, update every deviation below, and
   set `artifactReleaseTag` in `internal/ffi/acquire.go` to the next release
   tag. Pushing that tag runs `.github/workflows/release.yml`, which rebuilds
   every artifact, refuses to publish unless each one hash-matches its
   `checksums.txt` entry and the tag matches `artifactReleaseTag`, and then
   uploads the assets the loader will download and verify.

## Platform support

| Go target | Native artifact | Status |
|---|---|---|
| `darwin/arm64` | Mach-O dylib | Supported and tested |
| `linux/amd64` glibc | ELF shared object | Supported and tested |
| `linux/amd64` musl | ELF shared object | Supported and tested in Alpine |
| `linux/arm64` glibc | ELF shared object | Supported and tested |
| `linux/arm64` musl | ELF shared object | Supported and tested in Alpine |
| `windows/amd64` | PE DLL | Supported and tested |
| all other targets | stub | Builds, but `Serve` returns a clear unsupported-platform error |

## Known v2.3.10 limitations

The following bullets are the grouped, complete deviation summary for the
pin-specific notes in `docs/FFI-BOUNDARY.md`:

- Core calls runner registration an envoy and exposes it through `/envoys`.
  The public SDK deliberately retains runner vocabulary.
- `TotalSlots` is validated and retained at the stable boundary, but this core
  pin exposes no setter and does not transmit it.
- Actor creation time is unavailable to the adapter and crosses as zero.
  Core reduces stop causes to sleep or destroy, with the Go panic-stop path
  adding stop.
- Database-less actors use core's RemoteEnvoy state/KV backend. Setting
  `Database: true` switches that actor to the LocalNative worker while retaining
  engine-backed Depot durability; `SQLiteTransportDisabled` overrides every
  declaration. The build supplies SQLite batch-atomic-write support required
  by LocalNative.
- FFI is the default database transport and is supported on all six targets.
  Socket shares the public API and LocalNative backend but is experimental,
  Unix-only, and generation-scoped.
- SQL results are fully buffered and bounded: 1 MiB SQL/text/blob values,
  1,024 arguments/columns, and 32 MiB results. FFI chunks within that total;
  a socket response must fit one frame including protocol overhead. The exact
  pin's Depot decoder is locally corrected so an empty BLOB remains distinct
  from NULL.
- A database actor left live across abrupt standalone engine replacement stays
  assigned to its old generation at this pin. After runner reconnection and
  the envoy liveness window, gateway work returns `503 Actor not found` and no
  new Go `ActorStart` occurs. Sleep database actors before planned standalone
  replacement; sleep/wake durability is covered separately.
- Gateway JSON action arguments are normalized to CBOR by core. `RawAction`
  therefore exposes the exact CBOR argument array, not original JSON bytes.
  Actions use the pin's 60-second cooperative deadline; Go cannot preempt a
  handler that ignores its context.
- HTTP requests and responses are buffered by core. There is no `Flusher`, a
  peer socket abort is not visible to the handler, headers have one value per
  name, at most 256 names and 1 MiB per name/value cross the boundary, and
  multiple response `Set-Cookie` values cannot be represented.
- Raw WebSocket frames and individual boundary blobs are capped at 1 MiB. The
  SDK's 64-command per-connection admission queue can isolate a stalled peer,
  but core's later envoy queue exposes no buffered-byte gauge.
- Broadcast uses core subscriptions plus the pin's CBOR actor-connect envelope
  for raw WebSockets. Zero-recipient broadcast is a successful no-op.
- One compatibility `OnAlarm` alarm exists per actor. Separately,
  `Context.Schedules` exposes the pinned core's multiple durable one-shot
  action schedules with independent IDs. Action-schedule mutations report
  success after core commits them, while an internal four-second transport
  fence prevents a following sleep from overtaking them. Compatibility alarm
  mutation completion includes that fence. The engine workflow worker polls on
  a 16-second tick, so delivery is not a low-latency timer guarantee. Core caps
  pending schedules at 1,000 per actor, and the Go boundary caps a returned
  list at 32 MiB of record data.
- A hibernatable WebSocket message holds the native callback until Go returns
  its FIFO acknowledgement, with a 60-second bound. Frames accepted in the
  sleep-intent gap finish on the old generation; later frames wake the new
  generation. Hibernation itself suppresses `OnDisconnect`.
- Raw WebSocket hibernation is opt-in per actor through
  `Actor.HibernateWebSockets`. The false default matches the pinned TypeScript,
  Rust, and core defaults and avoids both private boundary acknowledgement
  bookkeeping and the engine acknowledgement on every message. Enabling it
  preserves sockets across sleep but added about 1.8 ms client-observed p50
  per loopback echo in the recorded investigation.
- After abrupt engine replacement, an already-sleeping actor may need one
  demand rehydration to reconcile its persisted alarm. A client that vanished
  while fully asleep may be pruned before Go observes `OnDisconnect`.
- Process-level runner drain intentionally closes raw WebSockets instead of
  hibernating them: no Go process remains to resume their connection state.

One additional architecture constraint is not pin-specific: every runner has a
single blocking poller pinned to one OS thread. Submission and user hooks do
not run on that thread, but event decoding and dispatch remain single-poller by
design.

## Tracked follow-ups

There are no unresolved code TODOs in the current implementation. Future pin
changes must convert any new limitation into an issue reference here before
leaving a TODO in code.
