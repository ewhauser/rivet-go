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
reply. Ordinary actor sleep still hibernates eligible sockets. The native
runner then emits `RunnerStopped`, the pump closes its handle, and a clean drain
returns nil so `main` exits with code zero.

If the deadline expires, core aborts the remaining runtime work and reports a
non-graceful drain. In-flight action and HTTP clients must not observe success,
raw WebSockets have already received 1001, `Registry.Serve` returns an error,
and the runnable examples exit with code 1. Treat that forced path as a
production incident even when the process must continue exiting.

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
The harness fails if any required chaos knob did not activate, if its Go truth
model differs field-by-field from engine-persisted actor state, if any live
client sees a missing or duplicate broadcast, if sequence values regress, or
if the final goroutine and native-handle counts do not return to baseline.

The mandatory activations are engine replacement using the same data
directory, mid-stream client disconnect, sleep/wake churn, a stalled WebSocket
client, and a sacrificial action panic. A successful duration alone is not a
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
3. Build all committed targets with Rust 1.97, cbindgen 0.29.4, Zig 0.16,
   cargo-zigbuild 0.23, and cargo-xwin 0.23 where applicable:

   ```sh
   scripts/build-ffi.sh aarch64-apple-darwin
   scripts/build-ffi.sh x86_64-unknown-linux-gnu
   scripts/build-ffi.sh x86_64-unknown-linux-musl
   scripts/build-ffi.sh aarch64-unknown-linux-gnu
   scripts/build-ffi.sh aarch64-unknown-linux-musl
   scripts/build-ffi.sh x86_64-pc-windows-msvc
   ```

4. Run `scripts/build-ffi.sh` for the same six targets again and require a
   clean diff. This is the idempotence check.
5. Run `cargo test --workspace`, Clippy with warnings denied, `go vet ./...`,
   `go test -race -count=1 ./...`, the checked-in conformance suite, and the
   24-hour soak. Preserve the existing corpus for the separately owned fuzz
   handoff; the upgrade itself does not add malformed-input cases.
6. Compare behavior against the prior pin, update every deviation below, and
   do not publish until all platform artifacts and checksums agree with the
   reviewed source.

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

The following 12 bullets are the grouped, complete deviation summary for the
M1-M6 pin-specific notes in `docs/FFI-BOUNDARY.md`:

- Core calls runner registration an envoy and exposes it through `/envoys`.
  The public SDK deliberately retains runner vocabulary.
- `TotalSlots` is validated and retained at the stable boundary, but this core
  pin exposes no setter and does not transmit it.
- Actor creation time is unavailable to the adapter and crosses as zero.
  Core reduces stop causes to sleep or destroy, with the Go panic-stop path
  adding stop.
- State and actor KV use core's remote SQLite backend. The native-local backend
  needs an atomic-write SQLite build and is not shipped.
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
- One durable Go alarm exists per actor. Alarm mutation completion includes a
  four-second pinned transport settlement; the engine workflow worker polls on
  a 16-second tick, so alarm delivery is not a low-latency timer guarantee.
- A hibernatable WebSocket message holds the native callback until Go returns
  its FIFO acknowledgement, with a 60-second bound. Frames accepted in the
  sleep-intent gap finish on the old generation; later frames wake the new
  generation. Hibernation itself suppresses `OnDisconnect`.
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

There are no unresolved code TODOs in M6. Future pin changes must convert any
new limitation into an issue reference here before leaving a TODO in code.
