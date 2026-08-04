# Hibernation opt-in review

Reviewed commits `b71bf56`, `387eba8`, and `9e03f4c` on `main`. The review
started from a clean tree and read `docs/PLAN.md`, `docs/FFI-BOUNDARY.md`, the
post-fix section of `bench/RESULTS.md`, and the M4 and M5 reviews before reading
the commits. The pinned dependency remained Rivet v2.3.10 at `957d4e48`.

## Summary

| Finding | Severity | Result | Fixing commit |
|---|---|---|---|
| 1. Default sockets still paid the private acknowledgement path | Major | Fixed | `87cf954` |
| 2. The public Go registry did not enforce the documented 1,024-actor bound | Major | Fixed | `87cf954` |
| 3. Old-ABI rejection did not exercise the immediately stale ABI 5 | Minor | Fixed | `87cf954` |
| 4. Benchmark provenance and the public latency tradeoff were incomplete | Minor | Fixed | `87cf954`, `d4b46ff` |

No blocker, major, or minor finding remains open.

## Findings

1. **Major — default sockets still crossed the private acknowledgement path.**

   Evidence: the reviewed change correctly set core's per-actor
   `can_hibernate_websocket` option and propagated `WsOpen.can_hibernate`, but
   every `ActiveWebSocket` still allocated a Rust acknowledgement FIFO. Every
   Go `OnMessage` also submitted `WsMessageAck`, including for actors that used
   the new false default. Pinned core conditionally omitted its engine-wire
   acknowledgement for those sockets, so the dominant engine round trip was
   gone, but the Go-to-Rust command, lookup, allocation, and FIFO/index work
   remained. That contradicted the stated default fast-path goal.

   Fix: `87cf954` allocates acknowledgement state only for opt-in sockets,
   emits no `WsMessageAck` from Go for a default socket, and returns directly
   from Rust after delivering its message event. The opt-in FIFO, exact index
   check, wraparound behavior, cancellation, and 60-second handler bound remain
   unchanged. Unit coverage proves that a default message produces no boundary
   command and no Rust acknowledgement state.

   Execution evidence: separate real-engine debug runs of the strengthened
   default close test and the M5 hibernation test counted the pinned envoy
   client's `ack ws msg` log at 0 for the default echo and 6 for the opt-in
   replay case. The default test sends and receives an echo before requesting
   sleep, so the zero count is not vacuous. The temporary debug-log setting was
   reverted before committing.

2. **Major — the public Go registry did not enforce the 1,024-actor bound.**

   Evidence: Rust rejected more than 1,024 actor names at native startup, but
   `rivet.Register` continued accepting them. A caller could therefore build a
   registry that the public API accepted and that failed only when native
   serving began. This also left the documentation's claim that the new map was
   bounded on both sides untrue.

   Fix: `87cf954` rejects the 1,025th distinct registration under the registry
   lock. The exact-bound test registers 1,024 actors, rejects the next actor,
   and proves that a rejected duplicate at the bound cannot change the
   hibernation map. Rust now has an exact 1,024-entry manifest/action/map test
   followed by an over-limit rejection, and the Go shape scanner proves that
   three containers at their shared 1,024-entry limit are accepted together.
   The existing Rust check still rejects hibernation-map keys absent from the
   actor manifest.

3. **Minor — old-ABI rejection did not exercise ABI 5.**

   Evidence: the loader compares the reported version with the generated ABI 6
   constant and therefore rejected ABI 5 by inspection, but its regression
   fixture reported only ABI 1. The requested immediately stale boundary was
   not executed.

   Fix: `87cf954` builds the same valid loader fixture as ABI 1 and ABI 5 and
   requires both to fail with the reported version in the error. This is an ABI
   compatibility test, not a malformed-input test.

4. **Minor — benchmark provenance and the public latency tradeoff were
   incomplete.**

   Evidence: the 8.243 ms versus 6.459 ms flag-only A/B and the approximately
   36 us in-runner measurement were described next to the committed archive
   without saying that they came from the uncommitted investigation. The
   public field documentation said only that hibernation added latency, without
   the measured magnitude or its loopback limitation. The corrected same-run
   table contained the evidence for Go and TypeScript being essentially even,
   but the conclusion was not stated against its variance.

   Fix: `87cf954` documents the boundary work and the approximately 1.8 ms
   single-machine loopback observation in the public API, WebSocket guide,
   operations limitations, plan deviation, and benchmark setup. `d4b46ff`
   labels the flag-only A/B and 36 us measurement as uncommitted investigation
   evidence that is absent from the archive, retains and corrects the old 22%
   conclusion, and states the same-run comparison: Go versus TypeScript differs
   by 0.5% in averaged throughput and 0.5% in averaged p50, below each SDK's
   recorded repetition movement.

## Requested pattern checks

1. **Default-flip completeness — clean after finding 1.** Per-actor
   registration drives core, `WsOpen.can_hibernate`, Go's connection record,
   acknowledgement allocation, and `WsCloseCmd.hibernate`. Default messages
   have neither a private boundary acknowledgement nor an engine-wire
   acknowledgement. Opt-in messages retain both. Pinned envoy replay consumes
   `hibernating_requests` only through its hibernating restore path; the M5
   wake test executes that path.

2. **M5 preservation — clean.** `TestHibernatingWebSocketSurvivesSleep` still
   checks that the same client socket survives sleep, messages accepted during
   sleep replay in order after wake, and hibernation suppresses `OnConnect` and
   `OnDisconnect`. The full race suite passed it against the rebuilt library.
   The default close claim was checked against source at the exact pin rather
   than inferred: TypeScript's option schema defaults
   `canHibernateWebSocket` to false, its native bridge passes the value into
   core, and core's sleep grace path dispatches `DisconnectConn` for a
   non-hibernatable connection while using transport-save only for a
   hibernatable connection. The pinned TypeScript sleep tests also exercise raw
   socket close/disconnect handlers during sleep shutdown. Go's explicit close
   code 1001 and reason `actor sleeping` are its adapter contract and were
   observed through the v2.3.10 engine by the conformance test; they are not
   presented as a separately specified TypeScript close reason.

3. **Configuration plumbing — clean after finding 2.** Unknown map actors are
   rejected. Duplicate re-registration cannot change a manifest entry. Empty
   actor names/actions/maps remain a valid Rust configuration, and
   `TestNativeBoundaryConcurrencyAndLifecycle` still performs a real
   zero-actor registration. Go and Rust both enforce 1,024 actors, and the Go
   scanner accepts the actor-name array, action map, and hibernation map
   together at exactly 1,024 entries.

4. **ABI 6 hygiene — clean after finding 3.** Rust's `RK_ABI_VERSION`, the
   generated header, and the generated Go constant all equal 6. LLVM export
   inspection found the complete loader surface in all six committed Mach-O,
   ELF glibc, ELF musl, and PE artifacts. Disassembly of `rk_abi_version` in
   every artifact showed the immediate value 6. The artifact directory
   contains only the six supported libraries. Loader tests reject ABI 1 and
   ABI 5.

5. **Benchmark honesty — clean after finding 4.** All 51 archived files pass
   `SHA256SUMS`. The old conclusion is retained and corrected rather than
   removed. The same-run S3 averages are Go 3642.2 operations/s at 8.687 ms p50
   and TypeScript 3660.1 operations/s at 8.643 ms p50. Their 0.5% differences
   are smaller than the table's Go repetition movements of 1.8% throughput and
   1.3% p50 and TypeScript movements of 3.1% and 2.5%. The report continues to
   label this as an engine-limited loopback workload.

6. **Documentation coherence — clean after finding 4.** The public field
   documentation explains the sleep-survival tradeoff, engine acknowledgement,
   approximately 1.8 ms loopback observation, and non-generalizability. The
   operations limitations table and WebSocket guide include the default fast
   path. The plan's goal-sketch actor does not expose a raw WebSocket, so it
   does not need the opt-in field.

7. **Soak coverage — clean.** Warmup directly exercises a hibernating chat
   socket through sleep, wake, message delivery, and broadcast convergence,
   then exercises a default socket through message handling, sleep closure,
   `OnDisconnect`, and `OnStop`. `activationCounts.validate` fails the run if
   either `hibernating_ws_wake` or `non_hibernating_ws_close` is zero. The final
   default-profile run recorded one of each.

## Final execution

| Check | Result |
|---|---|
| `go test -race -count=1 ./...` | Pass; conformance 535.319s, all packages green |
| `cargo test --workspace` | Pass; 30 unit tests and doc tests |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Pass |
| `go vet ./...` | Pass |
| `cargo fmt --all --check` and `git diff --check` | Pass |
| `scripts/build-ffi.sh` for all six targets, twice | Pass; second pass byte-for-byte identical for all libraries, header, Go constant, and checksum manifest |
| Native checksum manifest | Pass; manifest SHA-256 `32b23ce0ccf3a7e1af6a16150bcdd22c016384d8db0ec85a27e79e49b29f2981` |
| Archived benchmark checksums | Pass; 51 files |
| `go run ./cmd/soak` | Pass in 2m8.143s; 207 counter operations, 102 chat messages, 408/408 receipts, 5 alarms, both WebSocket activation paths nonzero, goroutines 3 to 3 |

The ABI 6 configuration map is the only new decode surface. It is a
string-to-boolean map whose keys must be present in the bounded actor manifest;
Go's shape scanner and Rust's semantic validation enforce the same cardinality
limit. It adds no event or command variant and no native allocation ownership
rule. This review added no fuzz case, deliberately malformed-input test, or raw
binary-payload test.

No GitHub operation, push, upstream contact, dependency pin change, or pin-file
edit was made.
