# Real-engine conformance

`go test ./conformance` starts Rivet Engine `v2.3.10` with filesystem storage
in a test temporary directory and exercises the complete public SDK through
the real gateway. It covers registration, lifecycle and persistence, actions,
HTTP, WebSockets, scheduling and hibernation, process replacement, runnable
examples, and graceful process drain.

## Engine acquisition

The shared `internal/devengine` acquisition order is:

1. `RIVET_GO_ENGINE_BIN` (must name an executable file).
2. `~/.cache/rivet-go/engine-v2.3.10/rivet-engine`.
3. A source build of exact tag `v2.3.10` / commit
   `957d4e482f404913ca1955d8ecc357533f6fd081`, cached under the directory in
   step 2.

The source-build fallback is equivalent to:

```sh
git clone --depth 1 --branch v2.3.10 https://github.com/rivet-dev/rivet.git \
  ~/.cache/rivet-go/engine-v2.3.10/source
cargo build \
  --manifest-path ~/.cache/rivet-go/engine-v2.3.10/source/Cargo.toml \
  -p rivet-engine --release \
  --target-dir ~/.cache/rivet-go/engine-v2.3.10/target
cp ~/.cache/rivet-go/engine-v2.3.10/target/release/rivet-engine \
  ~/.cache/rivet-go/engine-v2.3.10/rivet-engine
```

Acquisition failure is a conformance failure with a remediation message. Only
`go test -short` skips the real-engine test.

## Pin-specific management resource

At `v2.3.10`, `rivetkit-core` uses the renamed envoy protocol and connects at
`/envoys/connect`; therefore its registration appears in the active-only
`GET /envoys` management resource. The legacy `GET /runners` resource lists
the older runner protocol and cannot observe a core-hosted registry at this
pin. The public Go and FFI vocabulary remains `Runner` so later protocol
renames do not leak into the SDK.

## M5 scheduling, sleep, and hibernation

The M5 cases exercise only public SDK calls through the real engine and
gateway. They require an actor to be absent from the runner
(`connectable_ts == null`) before accepting a sleep assertion, then verify:

- an engine alarm rehydrates the actor and `OnAlarm` sees state persisted
  before sleep;
- clearing an alarm keeps the actor asleep past the old deadline, and two rapid
  schedules do not fire at the earlier timestamp and fire only the later one;
- a persisted alarm survives replacement of the engine process with the exact
  original timestamp and wakes on the reconnected runner;
- an awake alarm runs once, alarms remain serialized behind a long action, an
  alarm can request sleep, and a rehydrated `OnStart` can schedule the next
  alarm;
- sleep requested inside blocked action, HTTP, or WebSocket work cannot
  overtake that work's client-visible completion; and
- one real gateway WebSocket remains connected across actor eviction, buffers
  ordered client messages sent while the actor is asleep, uses those messages
  to rehydrate the actor before its later alarm, receives targeted and
  broadcast traffic on the same socket, and invokes `OnDisconnect` only for
  the later real close. That actor explicitly sets `HibernateWebSockets`; a
  second actor leaves the option at its false default and proves sleep closes
  the socket with code 1001 and reason `actor sleeping`, invokes
  `OnDisconnect`, and stops the actor. The stopped hibernating generation sees
  an unsaved mutation while the rehydrated generation reloads the earlier
  persisted value.

When a WebSocket handler itself requests sleep, the intent is admitted before
that handler returns but applied after its FIFO acknowledgement. Frames sent
in that boundary window are therefore handled in order by the old generation;
frames sent after engine-visible sleep rehydrate and run on the new generation.
The test drains the exact sequence so a loss or duplicate fails the next
expected observation.

Alarm assertions use engine timestamps and `eventually`, never a fixed sleep
as the success condition. The pinned workflow poll tick is 16 seconds.
Negative clear, overwrite, and one-shot observations cover one full tick plus
a 5-second margin. Positive alarms use 20-second sleep, 35-second hibernation,
45-second replacement, or 60-second restart schedules and a 90-second bound;
the bound covers the requested schedule, one poll tick, delivery margin, and
runner scheduling under `-race`. The 20-second message-driven WebSocket wake
bound is independent of alarm polling and covers gateway delivery plus actor
allocation.

The pinned engine delivers an alarm update and a later sleep intent as
separate workflow signals. Its signal fallback polls every 1.5 seconds, and
those signals can otherwise be observed out of checkpoint order. The FFI
holds serialized alarm completion for 4 seconds: two poll intervals plus a
1-second scheduling margin. Clear and overwrite use a 12-second earlier
deadline so that deadline remains safely beyond this settlement window.

The restart case uses one original 60-second schedule. After replacing the
engine process it proves a post-restart envoy ping, waits through the pin's
22-second liveness interval, demand-rehydrates the actor to reconcile the
persisted core schedule, confirms the alarm has not fired, and resleeps without
rescheduling before requiring the engine wake.

## M6 examples and process drain

`TestRunnableExamplesAndSIGTERMDrain` compiles `examples/counter` and
`examples/chat` as real subprocesses. The counter is called through the
gateway before receiving `SIGTERM`. The chat subprocess receives `SIGTERM`
with a raw WebSocket open plus a 1.5-second action and a 1.5-second HTTP request
already running on separate actors. The test requires both requests to finish,
the socket to close with code 1001 and reason `runner shutting down`, the
engine management API to stop listing the runner, and both example processes
to exit with status zero. A second chat subprocess uses a 200-millisecond
deadline and proves the forced path: action and HTTP do not report success,
the socket still receives 1001, the runner disappears, and the process exits
with code 1.

`TestPortedRunnableExamples` compiles and runs the SQLite todo, durable
reminder, raw HTTP counter, keyed per-tenant database, actor KV, raw WebSocket
connection-admin, actor-actions, and cross-actor-actions examples. It exercises
SQLite migration, CRUD, and transactions; explicit HTTP state persistence; an
alarm waking a sleeping actor; keyed state isolation; text and binary KV
operations; live connection administration; external Go actor creation and
resolution; typed and raw action calls; and company-to-employee plus
checkout-to-inventory calls through actor-scoped clients. Every runner must
also leave the engine cleanly after `SIGTERM`.

The soak is intentionally separate from `go test`: `cmd/soak` owns its engine,
runner, gateway clients, chaos schedule, strict truth models, and final leak
oracles. Its two-minute default is suitable for a smoke job; the 24-hour
release procedure is documented in `docs/OPERATIONS.md`.

## M7 per-actor SQLite

`TestPerActorSQLiteConformance` is one real-engine test parameterized over
`ffi` and `socket`. Each candidate runs CRUD, parameter binding for NULL,
integer, real, text, blob, and distinct empty-blob values, commit and rollback,
integer boundaries, REAL non-finite behavior, embedded-NUL text, a blob at the
value cap, lease expiry and rollback read-after, cancellation, rejected second
Begin, outer-DB transaction gating, syntax, constraint, and multi-statement
errors, concurrent access, per-actor isolation, SQL plus explicit public state
save, result-limit recovery, ordinary sleep/wake, dirty-lease sleep, and engine
process replacement against the same data directory. SQL errors must retain
structured codes and the actor must remain healthy.

FFI additionally queries a result larger than one boundary batch and requires
ordered reconstruction, including an empty result. Both candidates sleep the
actor while a transaction lease is open; the shared Go lifecycle fence rejects
new work, rolls back the lease, waits for admitted calls, closes the transport,
and then submits sleep. The stale `Tx` cannot affect the rehydrated actor.

Pinned engine behavior constrains the restart fixture. A generation left live
across abrupt standalone engine replacement remains associated with its old
envoy session rather than being seamlessly assigned to the reconnected runner.
The test therefore first sleeps the actor and observes its generation stop,
stops the runner, kills and restarts the engine with the same data directory,
starts a new runner, and demand-wakes the actor. The recovered SQL rows and Go
state prove storage durability across process replacement; the fixture does
not claim live-generation failover.

`TestDatabaseActorLiveGenerationDoesNotRehydrateAcrossEngineCrash` pins that
negative interaction. A `Database: true` actor is left live across standalone
engine replacement. After runner reconnection and the 22-second envoy
liveness window, engine metadata still assigns the old generation, a gateway
request returns `503 Actor not found`, and no new Go `ActorStart` occurs. The
sleep-first durability case above is the supported recovery fixture at this
pin.

The suite is intentionally part of normal `go test ./conformance` and the full
`go test -race -count=1 ./...` gate. It is not skipped as a long test. No fuzz
or deliberately malformed socket-input cases live here; the supervisor owns
that work. The package raises Go's injected 10-minute test timeout to 20
minutes because the unshortened race-enabled real-engine suite now exceeds the
default; a larger caller-supplied timeout is preserved.
