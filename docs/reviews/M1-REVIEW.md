# M1 review

## Summary

| Severity | Found | Fixed | Outstanding |
| --- | ---: | ---: | ---: |
| Blocker | 1 | 1 | 0 |
| Major | 5 | 5 | 0 |
| **Total** | **6** | **6** | **0** |

Review baseline: `68e3551` (builder's last M1 commit). Fixes landed in
`435c235`, `ceeaf42`, and `9ebc2a9`.

Process note: this review ran as three passes. The first two reviewer runs were
terminated mid-session by the Codex provider's content filter, which flags
work on malformed-input handling; the interrupted passes' verified results and
in-flight fixes were carried forward, completed, and re-verified by the
session supervisor (Claude), who also owns fuzzing going forward. All findings
below were confirmed by execution regardless of which pass surfaced them.

## Findings

1. **Blocker — `DecodeEventBatch` performed unbounded allocation on hostile
   length prefixes.** Found by the envelope fuzzer in pass 1: an 84-byte input
   whose `events` field is an array32 header claiming ~1.95 billion elements
   drove a multi-GB allocation and hung the process. Regression input
   committed at `internal/wire/testdata/fuzz/FuzzDecodeEventBatch/2bee19d607d2768d`.
   Fix `435c235`: a shape-validation scan before `msgpack.Unmarshal` rejects
   any container/blob whose declared length exceeds remaining input and caps
   nesting depth. Hardened in `ceeaf42` (pass 3): hard caps — 64 array
   entries (max poll batch), 1024 map entries, 1 MiB blobs per
   FFI-BOUNDARY.md — plus tests covering every msgpack type family, cap
   enforcement, and that all encoder outputs pass validation. Verified with a
   clean 60 s fuzz run (33.5M execs) after the fix.

2. **Major — poll-result buffer leaked on the error path.**
   `Runner.Poll` freed the native payload only on the success path; a non-nil
   payload accompanying an error was never freed. Fix in `9ebc2a9`
   (`internal/ffi/ffi_supported.go`): `bytesFree` deferred whenever the
   payload pointer is non-nil.

3. **Major — data race between `Error.JSON()` and `Error.Close()`.**
   Concurrent JSON/Close could use `ptr` after free. Fix in `9ebc2a9`: RWMutex
   around payload access and close.

4. **Major — pump did not enforce batch seq monotonicity.** FFI-BOUNDARY.md
   declares seq monotonic per runner; the pump ignored it, so a native
   sequencing bug would pass silently. Fix in `9ebc2a9`: pump fails fast on
   non-increasing seq; conformance also asserts strictly increasing seq across
   real polls.

5. **Major — conformance depth: failure bounds, boundary lifecycle, and
   disconnect were untested.** Added in `9ebc2a9`, all against the real pinned
   engine: structured bounded errors for a silent endpoint, a refused port,
   and a non-engine HTTP server (each within the 13 s startup bound); poll
   exclusivity (`poll_in_progress` on a second concurrent poll); graceful
   shutdown drain-report assertions; double-`Close` and free-without-shutdown
   soundness; and an engine-kill test asserting `RunnerDisconnected` arrives
   within the 22 s liveness window, followed by clean shutdown.

6. **Major — engine acquisition was not reproducibly pinned.** The builder's
   `downloadPinnedEngine` fetched a SHA256SUMS manifest from the same remote
   as the artifact (trust-on-first-use, nothing pinned in-repo). Fix in
   `9ebc2a9`: download path removed; the engine is built from the pinned tag
   with the source commit asserted equal to `engineCommit`, and
   `verifyEngineBinary` runs `--version` and requires the pinned version and
   commit — this also validates `RIVET_GO_ENGINE_BIN` overrides.

## Checked, clean

- Builder's Rust suite: 14 tests including unreachable-endpoint startup bound,
  submit backpressure, forced-free drain of parked senders (`cargo test`).
- Full suite under race detector including real-engine conformance
  (`go test -race -count=1 ./...`, conformance 56.8 s).
- Cross-language goldens are Rust-generated, Go-consumed, committed; Go
  re-encoding is byte-identical (`TestRust*Golden`).
- M0 review fixes unregressed (loader checksum, panic firewall through the
  loaded dylib, ABI single-sourcing).
- `scripts/build-ffi.sh` idempotency verified for all six targets by the
  builder; darwin artifact re-verified this pass.
- Recommendation (not run in Codex passes): periodic fuzz runs stay with the
  session supervisor; new decode surfaces added in M2+ (KV values) must get
  seeds + a fuzz pass before their milestone closes.
