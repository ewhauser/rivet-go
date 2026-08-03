# Real-engine conformance

`go test ./conformance` starts Rivet Engine `v2.3.10` with filesystem storage
in a test temporary directory, serves a zero-actor registry through the public
`rivet` package, and verifies registration and shutdown independently through
the engine management API.

## Engine acquisition

Resolution order is:

1. `RIVET_GO_ENGINE_BIN` (must name an executable file).
2. `~/.cache/rivet-go/engine-v2.3.10/rivet-engine`.
3. The RivetKit engine-process prebuilt convention.
4. A source build of exact tag `v2.3.10` / commit
   `957d4e482f404913ca1955d8ecc357533f6fd081`, cached under the directory in
   step 2.

The prebuilt resolver in the pinned source derives these platform URLs:

- `https://releases.rivet.dev/rivet/2.3.10/engine/rivet-engine-aarch64-apple-darwin`
- `https://releases.rivet.dev/rivet/2.3.10/engine/rivet-engine-x86_64-unknown-linux-musl`
- `https://releases.rivet.dev/rivet/2.3.10/engine/rivet-engine-aarch64-unknown-linux-musl`
- checksum manifest: `https://releases.rivet.dev/rivet/2.3.10/engine/SHA256SUMS`

The manifest returned HTTP 404 when checked on 2026-08-02, so there are no
published hashes to record and the source-build fallback is expected at this
pin. The fallback is equivalent to:

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
  schedules fire only the later timestamp;
- a persisted alarm survives replacement of the engine process and wakes on
  the reconnected runner;
- a sleep requested inside a blocked action cannot overtake that action's
  completion; and
- one real gateway WebSocket remains connected across actor eviction, buffers
  a client message sent while the actor is asleep, replays it after a scheduled
  alarm wake, receives targeted and broadcast traffic on the same socket, and
  invokes `OnDisconnect` only for the later real close.

Alarm assertions use engine timestamps and `eventually`, never a fixed sleep
as the success condition. Ordinary and restart wake checks are bounded at 45
seconds. That bound is intentionally wider than the observed local alarm
precision because v2.3.10 can spend roughly 22 seconds detecting and replacing
an envoy connection after an engine restart.
