# M6 review — final hardening

## Provenance and result

The milestone builder wrote the previous contents of this file. Those contents
were builder claims, not review evidence, and have been replaced in full. This
document is the independent review of the seven M6 commits ending at `64ceea2`
plus the fixes made during that review.

The builder tip was not release-ready. Its default soak failed, its chat and
alarm models were partly circular, blocking hooks could stop the runtime, and
the forced drain path could lose the promised WebSocket close. The reviewed
tip fixes every blocker and major found here. Local release-candidate evidence
is recorded below; the 24-hour run and the first hosted CI execution remain
release gates, not claims.

| Severity | Found | Fixed | Open |
|---|---:|---:|---:|
| Blocker | 2 | 2 | 0 |
| Major | 4 | 4 | 0 |
| Minor | 1 | 1 | 0 |

## Findings

1. **Blocker — the strict soak was not a trustworthy release oracle.**

   At `64ceea2`, the first default run failed while draining
   `client-000003`: the ledger had 455 received receipts but only 94 expected.
   Chat truth advanced inside `OnMessage`; alarm truth advanced inside actor
   lifecycle callbacks. Both reused the events consumed by the system under
   test instead of applying workload-generator intents to an independent
   Go-side model. Client membership also changed from actor callbacks, so a
   disconnect could stop expectations while its socket continued receiving.

   `2155422` makes counter tokens producer-local and deterministic, advances
   chat and alarm truth from generator intents, maintains an ordered ledger per
   client, and pauses the workload while membership changes. Accelerated
   falsification then found a second temporal bug: restart could interrupt an
   alarm after Go observed its callback but before the state was durable.
   `7c4eac9` keeps the alarm intent in the work gate through verified firing;
   it does not waive a lost alarm. Counter and chat state are checked on every
   intent; receipt ledgers converge before membership/restart transitions and
   after stalls; alarm state converges after restart; all models converge
   again at the end.

2. **Blocker — the required 15-minute profile survived every chaos oracle but
   failed its own final drain budget.**

   Seed 6301 completed the full active window, then returned `runner drain
   exceeded its deadline with 5 actors remaining`. The soak reused the SDK's
   10-second application default even though a long restart history leaves the
   pinned workflow engine needing more than one 16-second tick to process the
   five final workload actor stops. Short runs had hidden the mismatch.

   `3fa4868` gives the soak an explicit 60-second shutdown budget within its
   existing 90-second finalization context; `bbd639a` documents why this is a
   harness setting rather than a relaxation of the public default. Successful
   and forced application deadlines remain independently tested at 10 seconds
   and 200 ms. The complete 15-minute profile was rerun after this fix.

3. **Major — failures were not reproducible or diagnosable as documented.**

   The selected seed and data path appeared only in a successful final summary,
   a shared atomic token counter made producer interleaving affect token
   assignment, and automatically created data directories survived successful
   runs as well as failures. Management JSON reads were unbounded and the
   action response cap did not reject an over-limit body.

   `2155422` prints the seed before engine startup, prints the data and log
   paths immediately after allocation, uses deterministic per-producer token
   streams, preserves automatic data only on failure, removes it on success,
   and rejects management/action responses beyond 2 MiB. The existing 1 MiB
   WebSocket surface is enforced at the reader. Native runner, error, and
   buffer counts are compared with the pre-run baseline; the same commit fixes
   two FFI tests that bypassed the error-accounting constructor.

4. **Major — a metrics hook could deadlock the pump, and idle timeout was
   reported as poll latency.**

   Hooks ran synchronously on poll, submit, and actor paths. A
   `commands_submitted_total` hook that called back into `Submit` waited on the
   submit loop that was currently invoking it. A briefly blocking hook also
   stopped runtime progress. Every empty intentional poll timeout was recorded
   as latency, making the metric mostly a configured timeout distribution.

   `ab2544c` adds a serialized dispatcher outside pump locks and critical
   goroutines, drains it at shutdown, contains hook panics, and records poll
   latency only for event-bearing polls. Race tests block and re-enter from a
   hook and prove that submit progress continues; another test proves repeated
   idle polls emit no latency sample.

5. **Major — documented forced drain behavior did not match the transport
   teardown order, and HTTP/forced paths were unproved.**

   The native forced path closed raw WebSockets only after aborting core, when
   no live sender remained to carry the 1001 frame. Process conformance covered
   one successful action but not an in-flight HTTP request or the deadline
   path.

   `fb55935` marks the proxy draining and closes sockets before asking core to
   stop. The runnable chat example now supports a bounded drain-probe HTTP
   handler and a configurable shutdown deadline. Conformance proves successful
   in-flight action and HTTP completion, client-visible 1001, exit 0, and runner
   disappearance; a 200 ms deadline proves action/HTTP do not report success,
   1001 is still delivered, the runner disappears, and the process exits 1.

6. **Major — CI and operating documentation claimed gates that did not exist
   and omitted production constraints.**

   `OPERATIONS.md` called the default command the CI smoke profile, but no CI
   job ran it. The pin-upgrade procedure omitted the workflow cache paths and
   keys. The limitation summary omitted the 60-second action deadline, HTTP
   header limits, hibernation acknowledgement and sleep-gap behavior, and the
   single-poller constraint. Drain policy did not distinguish WebSocket,
   action, HTTP, and forced-deadline outcomes.

   `57e2ef3` adds a dedicated 30-minute `linux-amd64-soak-smoke` job that runs
   `go run ./cmd/soak` without an allow-failure path. It corrects the upgrade
   file list and drain/metric policy, and records all 12 grouped pin-specific
   deviations plus the separate single-poller architecture constraint. The
   documented 24-hour flags all exist in `cmd/soak`; the platform table matches
   the FFI build tags and all six build-script targets.

7. **Minor — the README confused runner exit with the `go run` wrapper's
   interrupt status.**

   The quickstart transcript passed, and Ctrl-C produced a completed runner
   drain, but the Go tool wrapper itself exited 1 for the interrupt. `131394c`
   states the observed distinction and retains the exit-0 promise for a built
   runner after clean SIGTERM.

## Pattern audit — checked clean

1. **Oracle theater:** activation counters increment only after the relevant
   chaos operation completes. Disabling each knob made the real soak fail with
   exactly its missing activation: `engine_restart` (seed 6101),
   `client_disconnect` (6102), `actor_sleep_wake` (6103),
   `stalled_ws_client` (6104), and `action_panic` (6105). A temporary dropped
   receipt failed with `client-000000 receipts=12 expected=13` (6201). A
   temporary skipped first counter update failed immediately on value,
   operation count, token, delta, and checksum (6202). Both sabotages were
   reverted with a clean diff.
2. **Soak reproducibility:** seed and paths print before fallible setup; the
   seed drives deterministic counter streams and producer-local intent
   sequences, while chaos cadence and target selection are fixed by flags.
   Failed automatic data directories were observed present; a successful
   automatic directory was observed absent.
3. **Leak accounting:** the soak uses `goleak` and explicit FFI counts for every
   owned runner, error, and native buffer. The successful 15-minute run moved
   from four runtime goroutines to three while `goleak` and every native count
   returned to baseline, demonstrating why raw goroutine equality is not the
   oracle.
4. **Drain policy:** successful actions and HTTP complete within the deadline;
   raw WebSockets close immediately with 1001; the forced path returns no
   action/HTTP success and exits 1. Manual SIGTERM of a built chat process
   3.74 seconds into a five-second action returned `{"output":2}`, delivered
   1001/`runner shutting down`, exited 0, and left `/envoys` empty.
5. **Docs accuracy:** all pin-upgrade paths, tool versions, build targets,
   soak flags, CI job names, build tags, and 12 grouped deviation bullets were
   checked against the repository. PLAN lists hibernation, header, alarm-fence,
   and single-poller limitations explicitly.
6. **Example quality:** `go build ./...` passes; neither example imports an
   `internal/` package. Real-engine conformance checks response bodies, state,
   close codes/reasons, process status, and engine-visible runner removal, not
   merely exit-zero.
7. **Metrics coherence:** gauge observations are enqueued in state-mutation
   order while user code executes after locks are released. Hooks are
   serialized, panic-contained, re-entrant, and permitted to block briefly.
   Empty blocking poll timeouts do not contribute to `poll_latency`.
8. **PLAN annotations:** M0-M6 are complete and linked to their reviews. The
   M6 acceptance row matches the bounded soak, documented 24-hour gate, and
   from-scratch quickstart evidence.
9. **Regression sweep:** M1-M5 test bodies remain active. M6 centralizes engine
   acquisition in `internal/devengine`; the WebSocket shutdown assertion
   intentionally changes from hibernation to process-drain 1001. ABI remains 5
   in Rust, the C header, and generated Go. All six artifact checksums match.
10. **CI realism:** `linux-amd64-soak-smoke` runs the exact default profile with
    a 30-minute job timeout and no conditional skip or green-washing. Hosted CI
    has not yet executed this new job.

## Verification performed by this reviewer

- `go test -race -count=1 ./...` — pass; real-engine conformance completed in
  533.139 seconds.
- `go test -short -count=1 ./...` — pass.
- `cargo test --workspace` — pass, 29 tests.
- `cargo clippy --workspace --all-targets -- -D warnings` — pass.
- `cargo fmt --all --check` — pass.
- `go vet ./...`, `go build ./...`, and `git diff --check` — pass.
- CI smoke profile, `go run ./cmd/soak` — pass in 2m8.208s with receipts
  400/400; chaos counts panics 3, disconnects 3, restarts 1, sleep/wakes 5,
  stalls 2; goroutines 3/3. Its automatic data directory was removed.
- Default-chaos soak, `go run ./cmd/soak -duration=15m -seed=6301` — pass in
  15m23.193s: 713 counter operations, 325 chat messages, receipts 1300/1300,
  34 alarm fires, and chaos counts panics 17, disconnects 17, restarts 10,
  sleep/wakes 34, stalls 16. The successful automatic data directory was
  removed; the earlier failed run's directory remains preserved.
- README quickstart from a fresh clone — exact readiness and curl transcript
  passed: `{"output":3}` followed by `3`.
- Six `scripts/build-ffi.sh` targets, twice — pass. Both complete passes
  produced digest `f8a3d0691d12c100da4758164eff0f5d8b48225b676eadbdaad186c4e429fb38`;
  `shasum -a 256 -c checksums.txt` reports all six artifacts `OK`.
- Focused successful and forced process-drain conformance — pass.

No fuzz test, deliberately malformed-input test, raw binary-payload test,
dependency pin change, push, or remote write was performed. During README
setup, one mistaken read-only `git clone` contacted GitHub before the local URL
rewrite was installed; that checkout was abandoned and moved to Trash. This is
reported because it did not comply with the requested no-upstream-contact
process rule, although it made no remote mutation.

## Release readiness

The reviewed code has passed every required local gate. A 24-hour
clean-checkout soak still must prove
that convergence, per-client receipt equality, all five chaos counters,
native-handle baselines, goroutine baselines, and final drain remain stable at
release duration. The first push to hosted CI still must prove all six native
matrix jobs, Linux race conformance, cache-key correctness, and the new default
soak-smoke job on the actual runners. Neither result should be inferred from
this local review.
