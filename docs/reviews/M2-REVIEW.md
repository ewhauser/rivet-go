# M2 adversarial review

## Summary

| Severity | Found | Fixed | Outstanding |
| --- | ---: | ---: | ---: |
| Blocker | 1 | 1 | 0 |
| Major | 6 | 6 | 0 |
| **Total** | **7** | **7** | **0** |

Review baseline: the requested M2 commits `692cf08`, `04934a3`, and
`fbf0f21`. Commit `aec0587`, a concurrent corpus-seed change, appeared while
the review was in progress and was preserved; no fuzzing was invoked in this
review.

Fixes are in `fdcd0d6`, regenerated native artifacts are in `440d1f3`, and
the corrected pin-specific contract is in `9b94072`. The final all-package run
also caught a review-helper regression in zero-actor manifest encoding; that
was fixed in `b4d942b` before the successful final run.

## Findings

1. **Blocker — the M2 restart exit criterion did not pass and the test did not
   establish a valid rehydration path.**

   Evidence: the baseline `go test -race -count=1 ./...` failed in
   `TestCounterStatePersistsAcrossEngineRestart` after waiting for a
   post-restart `ActorStart`. The test gracefully drained the original runner,
   restarted the engine, and repeatedly registered a fresh runner, but the
   pinned actor workflow's persisted `GoingAway` state did not wake through
   that sequence. There was therefore no post-restart handler observation from
   which to assert persisted state.

   Fix: `fdcd0d6` kills the live engine process, keeps the runner transport
   reconnecting, and requires the original actor generation's `OnStop` while
   the engine is still unavailable. It starts a distinct engine process on the
   same `runningEngine.storage` directory, waits for the lost actor to become
   non-connectable, and wakes it through the engine's public gateway path. The
   assertion is made by generation 2's `OnStart`, whose public
   `BinaryUnmarshaler` receives the persisted counter value 41. The actor ID,
   input, typed value, and strictly higher generation are all checked. Focused
   runs passed twice, and the final full conformance run passed.

2. **Major — `ActorStopResult` could overtake accepted state writes, and a
   missing `StatePersisted` could wait forever.**

   Evidence: Rust dispatched `SaveState` in detached tasks while resolving an
   `ActorStopResult` immediately, with no per-actor operation fence. Go did not
   close actor-scoped admission and wait for concurrently accepted saves before
   submitting its stop result. Neither side supplied a complete bounded path
   when a save acknowledgement never arrived.

   Fix: `fdcd0d6` reserves each Rust state operation synchronously before it is
   spawned, rejects saves after stop acknowledgement begins, and waits for all
   reserved operations before resolving the stop lifecycle correlation. Core
   saves have a 30-second structured timeout; Go has a 35-second
   acknowledgement backstop and structured cancellation/deadline errors. Go
   closes admission and waits for accepted saves before `ActorStopResult`.
   A canceled save consumes its late acknowledgement before later reuse.
   Regressions cover concurrent save/stop ordering, a missing completion,
   shutdown with a pending save, and reuse after late completion.

3. **Major — absent state and a persisted zero-length state were conflated.**

   Evidence: Rust converted `ActorStart.snapshot` with `unwrap_or_default`, and
   Go treated every zero-length byte slice as a first start. A public state type
   implementing `encoding.BinaryUnmarshaler` therefore could not observe an
   intentionally persisted empty state.

   Fix: `fdcd0d6` carries `persisted_state` as an optional byte string, preserves
   nil versus present-empty through Go copies, and skips decoding only for nil.
   Rust-generated fresh and empty-state goldens prove the wire distinction.
   Unit tests cover binary and JSON encode/decode errors, and the real restart
   conformance now uses the public binary serde path, so its successful
   rehydration also proves `Context.Save` reaches `BinaryMarshaler`.

4. **Major — KV correlation allocation could emit ID zero or overwrite a live
   waiter after integer wrap.**

   Evidence: `nextKVID.Add(1)` was used directly. On wrap it returns zero,
   which `internal/wire.validateEvent` rejects, and a later wrapped value could
   replace an existing entry in `kvPending`.

   Fix: `fdcd0d6` allocates under the pending-table lock, skips zero and every
   live ID, and permits reuse only after completion. Tests force wrap with ID 1
   pending, require ID 2 for the competing request, and then prove ID 1 can be
   reused after completion. Structured KV errors, actor-stop drain, runner-free
   drain, and goroutine cleanup are covered. A real-engine test executes the KV
   error path and lists 65 entries.

5. **Major — M2 command goldens did not prove the bytes Go actually emits.**

   Evidence: the Rust golden serialized the Rust tagged enum, while Go's
   `Command` always encodes its complete field set. The Go M2 test decoded the
   golden but did not re-encode and compare it, so Rust tolerance of Go's extra
   fields and cross-language byte identity were unproven.

   Fix: `fdcd0d6` generates the M2 command golden from the complete Go command
   shape, immediately decodes it with Rust's `CommandBatch`, and requires Go
   re-encoding to be byte-identical. The committed suite covers every M2 event
   and command kind, including the two state-presence variants.

6. **Major — ABI 2 had no downgrade-rejection regression and the committed
   libraries had not been independently checked for the reviewed surface.**

   Evidence: the version was correctly sourced from Rust through the generated
   header and Go constant, but tests only loaded the expected library. An ABI-1
   library exporting all required symbols was not shown to fail at the same
   bind-and-validate step used by the loader.

   Fix: `fdcd0d6` factors the loader's exact bind-and-validate path and tests it
   with a minimal ABI-1 cdylib fixture. `440d1f3` regenerates all six committed
   artifacts. Both full matrix build passes converged without a generated
   diff; checksums validate and symbol inspection found the complete ABI-2
   surface in Mach-O, all four ELF variants, and PE/COFF.

7. **Major — OnStop panic behavior was unproved and the pin-specific contract
   overstated the engine result.**

   Evidence: real-engine coverage exercised `OnStart` panic only. The boundary
   document claimed an `OnStop` failure reached an engine `StopCode::Error`, but
   at v2.3.10 an actor already being destroyed completes as destroyed after a
   graceful-cleanup error. That claim was not supported by the engine's
   management response.

   Fix: `fdcd0d6` adds exact structured `handler_panic` coverage for `OnStop`,
   returns the cleanup failure through the actor factory's structured core
   failure path, and verifies with the real engine that the actor reaches a
   stopped/destroyed state while the runner continues to start and stop a
   healthy peer. `9b94072` records the pin's actual behavior instead of
   claiming an unavailable stop code.

## Checked, clean

1. **Restart theater:** a real OS process is killed and replaced; both launches
   use the same filesystem database directory. The old actor generation stops
   before restart. Only the later, higher-generation `ActorStart` handler can
   satisfy the persisted-state assertion.
2. **Stop-path durability:** Go and Rust both fence stop acknowledgement behind
   accepted saves. Saves from `OnStop` remain admitted until the hook finishes.
   Missing completions and caller cancellation return structured errors rather
   than hanging. Engine destroy, concurrent save, timeout, late completion,
   and shutdown paths are tested.
3. **Per-actor ordering and head-of-line blocking:**
   `TestSlowActorDoesNotBlockFastActorAndPreservesOrder` blocks one actor's
   start, queues its stop, and proves a second actor starts and reports success
   first. The slow actor's start result still precedes its stop result.
4. **Panic containment breadth:** `OnStart` and `OnStop` panics have actor-local
   structured unit coverage. Real-engine coverage verifies startup failure,
   stop/destroy completion, and healthy-peer continuity.
5. **KV correlation:** IDs begin at 1, skip zero and live IDs on wrap, and reuse
   completed IDs. A real error arm is exercised. Actor stop and runner free
   drain pending callers, with `goleak` checks on both paths.
6. **Envelope cardinality:** Rust caps `KvList` at 1024 and Go's shape scanner
   uses the same array ceiling. The native real-engine test returns 65 entries,
   crossing the old poll-sized limit without a decode failure.
7. **Golden coverage:** all seven M2 event kinds and all seven M2 command kinds
   have committed Rust-generated goldens consumed by Go. Go command
   re-encoding is byte-identical and the Rust decoder accepts the full Go
   shape.
8. **ABI bump hygiene:** ABI 2 flows Rust constant to generated header to
   generated Go constant to runtime assertion. The ABI-1 fixture is rejected.
   The M0 checksum/load tests remain active. All six committed libraries export
   the reviewed runner surface.
9. **State serde edges:** binary and JSON errors propagate out of the typed
   adapter and become actor-local lifecycle errors. The public binary override
   is used by real persistence conformance. Absent and present-empty state are
   distinct in Rust goldens and Go decoding.
10. **Manifest correctness:** registration remains sorted and duplicate names
    are rejected. The M1 zero-actor registration passes with an explicit empty
    array. The real KV actor's registered name is observed through the engine
    API. The first final suite run caught a nil-slice helper regression here;
    `b4d942b` fixed it and the exact full suite then passed.

M0 and M1 review fixes were also rechecked: checksum-verified loading, the
loaded-library panic firewall, ABI single-sourcing, error-handle ownership,
poll sequence monotonicity, bounded startup failures, disconnect reporting,
poll exclusivity, zero-actor registration, and graceful/free lifecycle tests
all remain active and green. The Rivet pin remains v2.3.10 at the locked
957d4e48 revision.

## Decode surface notes

No fuzzing command was invoked, and no malformed-input or decoder stress cases
were added. The mandatory all-package command executed the repository's
pre-existing unit corpus; this review did not construct any new decoder input.
The existing shape scanner was inspected as ordinary boundary code. Its M2
array ceiling is 1024, matching Rust's valid KV-list maximum, while native
conformance proves a legitimate 65-entry result crosses the former
poll-batch-sized boundary. Optional state presence and full command shape were
checked only with valid Rust-generated goldens.

## Final verification

The following completed successfully after all fixes:

- `go test -race -count=1 ./...` — all packages passed; real-engine
  conformance completed in 111.376 seconds.
- `cargo test --workspace` — 16 Rust tests passed plus doc tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `go vet ./...`.
- `cargo fmt --all --check` and `git diff --check`.
- `scripts/build-ffi.sh` for all six targets, twice. The second pass changed no
  artifact, header, generated ABI constant, or checksum.
- SHA-256 manifest verification for all six libraries and symbol inspection of
  Mach-O, four ELF variants, and PE/COFF.

The final worktree was clean after committing this review. No upstream contact,
GitHub operation, push, or Rivet pin change was performed.
