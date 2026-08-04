# M5 review — alarms, sleep, and WebSocket hibernation

## Result

M5 is accepted after fixing one blocker, three major findings, and one minor
finding. The most serious defect was permanent alarm loss: at the pinned
engine, an alarm checkpoint and a following sleep checkpoint travel as
independent workflow signals, and the engine could process sleep first and
then discard the lower-index alarm as stale. The original alarm-after-sleep
test failed twice under focused execution before the fix. It passed twice
after the fix and also passed in the full race-enabled suite.

| Finding | Severity | Disposition | Fixing commit |
| --- | --- | --- | --- |
| M5-1. Scheduling intents were fire-and-forget and not generation-fenced | Major | Fixed | `fa1c36c` |
| M5-2. Sleep could overtake an alarm checkpoint and lose the alarm permanently | Blocker | Fixed | `fa1c36c` |
| M5-3. Hibernation conformance did not prove the actual socket and replay boundary | Major | Fixed | `63ebc61` |
| M5-4. Alarm, drain, identity, and timing assertions left required behavior unproved | Major | Fixed | `fa1c36c`, `63ebc61` |
| M5-5. Public actions erased structured native intent errors | Minor | Fixed | `fa1c36c` |

## Findings

1. **M5-1 — Major — reconstructed from the interrupted review diff:
   scheduling intents were fire-and-forget and not generation-fenced.**

   Provenance: this is the required reconstruction of the prior review pass.
   At M5 HEAD, `SetAlarm` selected the current actor by actor ID, spawned the
   native list/cancel/set work, logged native errors, and returned success to
   Go before that work completed. `SleepIntent` likewise selected the current
   actor by ID and returned without reporting whether the intended generation
   accepted the sleep. A stale handler could therefore affect a replacement
   generation, and `Schedule`, `ClearSchedule`, or `Sleep` could report success
   after native rejection or failure.

   The interrupted work correctly introduced operation IDs, exact generation
   identity, and an `ActorIntentResult` event, but it was not complete or
   releasable: only the Darwin library had been rebuilt, the other five
   artifacts described the old protocol, public action wrapping still erased
   the structured result, late completions could collide after operation-ID
   wrap, and the new path had not passed the required gates.

   Fix: `fa1c36c` completes the operation-ID protocol in Rust and Go, waits for
   native completion, returns named native failures as `HandlerError`, retains
   abandoned operation-ID tombstones until a late result consumes them, and
   fences alarm and sleep commands to the exact actor generation. Rust and Go
   goldens cover the new event and command fields. All six ABI artifacts were
   regenerated twice from the completed source.

2. **M5-2 — Blocker — sleep could overtake an alarm checkpoint and lose the
   alarm permanently.**

   Evidence: the original `TestSchedulingSleepAndMidflightPolicy` failed twice
   after its 90-second positive bound, and the restart alarm case failed once.
   A debug run showed the core producing the alarm checkpoint at index 3 and
   sleep at index 4. The engine received the independent sleep workflow signal
   first, advanced the checkpoint, and logged the later-arriving alarm signal
   as ignored by its generation/index filter. The actor then remained asleep;
   extending the test timeout would only hide a permanent loss.

   Fix: `fa1c36c` keeps alarm mutations serialized and holds their completion
   across two full 1.5-second `DatabaseKv` signal-poll intervals plus a
   1-second scheduling margin before a handler can issue the next alarm or
   sleep checkpoint. This is a pin-specific transport settlement, documented
   in `docs/FFI-BOUNDARY.md`; it is not presented as alarm delivery latency.
   The focused scheduling test then passed twice (162.08s and 161.67s), the
   restart case passed (61.25s), and the first full race suite passed.

3. **M5-3 — Major — hibernation conformance did not prove the actual socket
   and replay boundary.**

   The original test sent a `wake` message after sleep and accepted the result,
   but did not prove that one gateway TCP connection remained open, that
   messages sent while the engine showed the actor asleep replayed in message
   index order, or that a message in the sleep-intent acknowledgement gap could
   neither disappear nor reappear. It also lacked the required unsaved-state
   negative control and could not distinguish an alarm wake from a client
   touch.

   Fix: `63ebc61` holds one Gorilla WebSocket connection throughout the test.
   While the sleep-requesting handler is still admitted, it sends two ordered
   messages and proves that both complete on the old generation before stop.
   After `ActorStop` and an engine record with `connectable_ts = null` and a
   non-null `sleep_ts`, it sends two more ordered messages on that same socket,
   proves message-driven rehydration occurs before the pending alarm, and
   observes both on the new generation. Actor-to-client send and broadcast
   resume afterward. `OnConnect` remains at one and `OnDisconnect` occurs only
   on the final real close. The stop sees an unsaved value of 99, while the
   rehydrated actor reads the persisted value 41. `fa1c36c` also keeps each
   hibernatable native message operation admitted through its matching FIFO
   acknowledgement, with wrap coverage across an actual hibernate/restore
   cycle. The focused real-gateway test passed in 35.91s.

4. **M5-4 — Major — alarm, drain, identity, and timing assertions left
   required behavior unproved.**

   The initial clear check used a 5-second grace even though the pinned
   workflow worker polls on a 16-second tick. Rapid overwrite did not observe a
   full tick after the superseded timestamp or reject early firing of the
   replacement. Restart coverage did not compare the original, post-restart,
   and delivered alarm timestamps. There was no awake one-shot alarm, alarm
   versus long-action serialization check, structured absent-`OnAlarm` check,
   sleep-from-`OnAlarm`, schedule-from-rehydrated-`OnStart`, real HTTP drain
   outcome, or deterministic shutdown/sleep-intent race. The FFI alarm wait was
   also 30 seconds while the pinned core action deadline is 60 seconds.

   Fix: `fa1c36c` aligns alarm correlation to the 60-second core action
   deadline and adds pump-level serialization, absent-callback, structured
   intent, and shutdown-race checks. `63ebc61` adds the missing real-engine
   cases. Negative alarm observations now cover the 16-second worker tick plus
   a 5-second margin. Positive bounds and transition margins are derived in
   `conformance/README.md`. Action, HTTP, and WebSocket handlers each prove
   their client-visible completion precedes `ActorStop`; no fixed sleep is used
   as a success assertion gate.

5. **M5-5 — Minor — public actions erased structured native intent errors.**

   `HandlerError` did not implement the public action-code convention, and the
   action adapter checked generic action errors before preserving pump errors.
   Native codes such as `actor_generation_stale`, `alarm_set_failed`, and
   `actor_intent_timeout` would therefore collapse into `action_failed`.

   Fix: `fa1c36c` gives `HandlerError` an `ActionCode` and preserves an existing
   `HandlerError` before generic action wrapping, with a regression test.

## Required pattern sweep

1. **Sleep theater — checked, clean.** The alarm-after-sleep case observes
   `ActorStop`, then the engine's non-connectable sleeping record, and only then
   waits passively for the engine alarm. No test call touches the actor to wake
   it.
2. **Hibernation truth — checked, clean.** One gateway TCP connection carries
   old-generation gap messages, new-generation sleep-time messages, and
   resumed actor traffic. Real-gateway lifecycle counts prove connect and
   disconnect suppression.
3. **Ack-gap consequences — checked, clean.** Two messages at the intent/handler
   boundary are acknowledged once in FIFO order on the old generation; two
   engine-visible sleep-time messages replay once in FIFO order on the new
   generation. The next expected frame and handler observation make loss or a
   duplicate fail deterministically. Message indices still wrap from 65535 to
   0 across hibernation.
4. **Alarm durability and identity — checked, clean.** Restart preserves the
   exact timestamp; clear remains negative for a full worker tick plus margin;
   overwrite remains negative through the old deadline and cannot fire before
   the new one; an awake alarm runs once; an absent hook returns
   `callback_not_found`.
5. **Deadline and ordering — checked, clean.** Alarm waits behind an active
   action, uses the core's 60-second action deadline, can request sleep itself,
   and can be scheduled by `OnStart` after rehydration.
6. **Drain policy honesty — checked, clean.** Action, HTTP, and WebSocket work
   completes before sleep and exposes that completion to the client. A runner
   shutdown racing a pending sleep result returns `ErrShuttingDown` and closes
   in the documented order.
7. **Timing-test hygiene — checked, clean.** The 1.5-second signal poll,
   16-second worker tick, 5-second delivery margin, 10-second transition
   margin, 20-second message-wake bound, and 90-second positive alarm bound are
   stated and separated by purpose. Tests poll observed state or wait on
   bounded channels; no bare `time.Sleep` is an assertion gate.
8. **State fidelity — checked, clean.** The rehydrated actor reads the actual
   persisted payload (41), not a Go cache; the unsaved mutation (99) is absent;
   generation increases across sleep exactly as it does across M2 restart.
9. **Regression sweep — checked, clean.** The unmodified M4 overflow, fanout,
   and rejection cases, M3 HTTP/actions, M2 restart, and M1 disconnect cases
   all pass in the full race suite. Commit `9822882` correctly prevents native
   code from replacing the intended WebSocket rejection with a generic close;
   its existing rejection assertions remain intact.
10. **Envelope hygiene — checked, clean.** Rust goldens cover `ActorAlarm`,
    `ActorIntentResult`, the M5 commands, and `WsOpen.resumed`; Go re-encodes
    them byte-for-byte. A null `SetAlarm.alarm_ts` remains null. `ActorStop`
    accepts exactly the emitted `sleep`, `stop`, and `destroy` reasons, and a
    resumed WebSocket must be hibernatable. ABI 5 remains single-sourced and
    old-ABI rejection is exercised.

## Decode surface notes

M5 adds bounded scalar fields: fixed-width alarm timestamps, generations,
operation IDs, and the resumed flag. It does not add a blob, map, recursive
container, or unbounded allocation surface. The new event golden and updated
command goldens are Rust-produced and Go byte-identical on re-encode, including
the nullable alarm timestamp. The supervisor's fuzz-seed commit and all fuzz
files were left untouched. Per review constraints, this pass added no fuzz
case, deliberately malformed-input test, or raw binary-payload test.

## Verification

- `go test -race -count=1 ./...` — pass; conformance 521.054s, all five Go
  packages green.
- `go test -race -count=1 ./...` — independent repeat pass; conformance
  521.180s, all five Go packages green.
- `cargo test --workspace` — pass, 29 tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` —
  pass.
- `go vet ./...`, `cargo fmt --all --check`, and `git diff --check` — pass.
- ABI loader/version and old-ABI rejection tests — pass under `-race`.
- `shasum -a 256 -c checksums.txt` — all six artifacts pass.
- `scripts/build-ffi.sh` for all six targets, twice — pass; the second complete
  pass produced zero artifact difference, combined digest
  `7efec81c2c53b9d735819d605ff61417a16e1997ab6acac814d4b35b503858d0`.

No GitHub operation, upstream contact, dependency pin change, or fuzz-file
change was made.
