# M7 review: per-actor SQLite transports

Review date: 2026-08-05

Commits reviewed: `aaaa641`, `54a7b33`, `073589a`, `f6022c7`, and
`b0c7981`. Supervisor fuzz commit `523da8e`, every fuzz file, and `.github/`
were left untouched. The review used only the local repository and the local
pinned Rivet checkout; it performed no remote operation.

## Summary

The builder tip was not ready to choose a transport. Sleep had different
transaction semantics on the two paths, a canceled nested Begin could acquire
an invisible lease later, FFI and socket returned different backpressure
codes, and the result receiver did not enforce the boundary it documented.
The local review commits fix every blocker and major found. No finding changes
the Rivet `v2.3.10` / `957d4e482f404913ca1955d8ecc357533f6fd081`
pin or relaxes a test.

| Finding | Severity | Disposition | Fixing commit |
|---|---|---|---|
| M7-1. Sleep/stop did not provide one transaction lifecycle contract | Blocker | Fixed | `633195b` |
| M7-2. A second canceled Begin could create an invisible lease | Major | Fixed | `633195b` |
| M7-3. FFI chunk metadata and the Go total-result guard were dishonest | Major | Fixed | `633195b`, `d705a1a` |
| M7-4. FFI worker errors did not match socket error codes | Major | Fixed | `d705a1a` |
| M7-5. The socket schema and hello ceiling did not match their documented contract | Minor | Fixed | `d705a1a` |
| M7-6. The old-ABI gate skipped immediately stale ABI 6 | Minor | Fixed | `d705a1a` |

## Findings

1. **Blocker — sleep/stop did not provide one transaction lifecycle contract.**

   `Context.Sleep` called `DB.closeForSleep`, but the FFI backend's close was a
   no-op while the socket backend closed its connection immediately. There was
   no shared admission fence for a query racing sleep. An open FFI `Tx` could
   therefore remain callable until later actor teardown while the equivalent
   socket `Tx` was already terminal. This contradicted the documented
   cross-transport API and made rollback timing transport-dependent.

   Commit `633195b` gives each generation a shared lifecycle gate. Close now
   rejects new SQL, marks and rolls back the open lease, waits for admitted
   operations, closes the transport, and only then lets sleep or stop proceed.
   A race-focused unit test proves close requests rollback but cannot overtake
   an admitted transaction operation. Real-engine conformance now performs
   dirty-lease sleep on both candidates, requires stale handles to return
   `sqlite_endpoint_closed`, and reads after wake to prove the partial insert
   was rolled back.

2. **Major — a second canceled Begin could create an invisible lease.**

   The original API sent every Begin to core. During review, Begin B was issued
   while transaction A held the exclusive gate. B reached its caller deadline,
   A rolled back, and B then acquired a 150 ms lease with no Go `Tx` handle.
   The immediately following ordinary `SELECT 1` failed with
   `transaction_expired` on both FFI and socket. This was a real execution
   failure, not a theoretical queue concern.

   Commit `633195b` reserves the generation's single transaction slot before
   transport submission. A second Begin now returns
   `transaction_already_open` immediately and sends no transport request.
   Conformance asserts that code on both paths and proves the database is
   usable as soon as the first transaction rolls back.

3. **Major — FFI chunk metadata and the Go total-result guard were dishonest.**

   Rust repeated `rows_affected` and `last_insert_id` on every chunk although
   the boundary document assigned them to chunk zero. Go silently ignored
   repeated columns, overwrote mutation metadata on later chunks, and relied
   solely on trusted Rust for the 32 MiB aggregate cap.

   Commit `d705a1a` emits columns and mutation metadata only on chunk zero.
   Commit `633195b` makes the assembler reject later metadata and independently
   accounts columns and values against 32 MiB. Exact unit cases cover an empty
   result, 1,025 values, a result exactly at a chunk boundary, one byte over a
   chunk, and exact/over-total-limit results. Real-engine cases reconstruct a
   multi-chunk FFI result and make both transports reject an oversized result
   as `sqlite_result_too_large`, then succeed on the same DB handle.

4. **Major — FFI worker errors did not match socket error codes.**

   The socket protocol maps queue saturation to `sqlite_queue_full` and endpoint
   loss to `sqlite_endpoint_closed`. FFI passed Depot worker errors through
   `RivetError::extract`; a direct executable test returned `internal_error`
   for `SqliteWorkerOverloadedError`. This broke the public error taxonomy even
   though SQL statement errors happened to match.

   Commit `d705a1a` recognizes the typed Depot overload, closing, and dead
   errors before generic extraction. The test that originally observed
   `internal_error` now requires the socket-equivalent codes. SQL syntax,
   constraint, lease-expiry, result-limit, and stale-generation cases remain
   structured and identical in parameterized real-engine coverage.

5. **Minor — the socket schema and hello ceiling did not match their documented contract.**

   The checked-in BARE schema had been reformatted and stripped of upstream
   comments, so it was not the claimed byte-for-byte copy. `decodeHello` also
   rejected a server ceiling above 32 MiB instead of selecting the documented
   smaller local ceiling. Commit `d705a1a` restores the exact pinned schema and
   clamps a larger server value. The local and upstream schema now share SHA-256
   `c3e3cee4e913ddc3f6592590452327b3db4bfc85a9d0d2ef151cce85ad522cf1`.

6. **Minor — the old-ABI gate skipped immediately stale ABI 6.**

   ABI 7 was correctly single-sourced, but the loader regression built only
   ABI 1 and ABI 5 fixtures. Commit `d705a1a` adds ABI 6 to the same real shared
   library load/rejection test. The twice-run six-target build regenerated ABI
   7 artifacts and produced no change on its second pass.

## Checked-clean defect-pattern sweep

1. **Transaction integrity — clean after M7-1 and M7-2.** An open transaction
   gates outer DB work on both paths; cancellation exhausts the caller context
   without interleaving. A second Begin is rejected before submission. Lease
   expiry rolls back partial writes; operation, Commit, and Rollback after
   expiry return `transaction_expired`, and read-after counts remain zero.
   An already-canceled transaction call is rejected before submission and does
   not poison the lease.

2. **Value fidelity — clean with documented SQLite semantics.** Both candidates
   execute the same test for NULL, min/max `int64`, ordinary REAL, positive and
   negative infinity, NaN, embedded-NUL UTF-8 text, empty and non-empty blobs,
   and a blob at the 1 MiB cap. Empty blob and NULL remain distinct. SQLite
   converts bound NaN to NULL; infinities remain REAL. Go rejects invalid UTF-8
   text arguments. The shared pinned Depot decoder uses replacement characters
   if an SQLite TEXT cell already contains invalid bytes, before either
   transport encodes the result.

3. **Result-limit honesty — clean after M7-3.** Rust and Go enforce the FFI
   aggregate limit, exact chunk/value boundaries are executable, and the
   socket one-frame overflow returns `sqlite_result_too_large` without poisoning
   the connection. Both candidates enforce the shared 32 MiB contract.

4. **Socket lifecycle races — clean after M7-1.** Sleep uses the shared
   admission/rollback/close fence. Wake conformance requires a greater actor
   generation and successfully constructs its DB from that generation's new
   endpoint. Abrupt peer close terminates the read loop under `goleak`.
   Concurrent real queries share one connection, and unit coverage allocates
   unique nonzero request IDs concurrently across u32 wrap while skipping a
   still-live ID.

5. **Exec-via-Query equivalence — clean.** Socket public Exec deliberately uses
   `SqliteQuery`, maps `changes` and `lastInsertRowId`, and drops returned rows.
   CRUD asserts insert ID and update count on both paths. Both FFI commands and
   socket Query reach Depot's `execute_single_statement`; a two-statement Exec
   returns `sqlite_error`, SQLite code -1, statement index zero, and executes
   neither statement on either path.

6. **Cross-transport parity — clean after M7-1 through M7-4.** The entire
   SQLite conformance test is parameterized over both candidates. Documented
   differences are confined to transport framing, platform reach, and FFI
   chunking. Public behavior and structured codes match.

7. **KV/state coexistence — clean with an important non-atomicity clarification.**
   The same action performs a SQL mutation and an explicit public state Save;
   both survive sleep and same-directory engine replacement in the same actor
   lineage. SQL commit and state/KV save are separate durable engine commits,
   not one atomic cross-store transaction. A successful SQL call is durable
   before return, Save is durable before return, action completion waits for
   its state save, and ActorStopResult follows the SQL close and core operation
   fences.

8. **Benchmark honesty — clean.** All cells use the same 32 actors/workers,
   deterministic 50/40/10 operation cycle, statement text, transaction body,
   fresh engine directory, connection reuse, 10-second warmup, and 45-second
   measurement. TypeScript uses raw `c.db.execute`/`transaction`, not an ORM.
   Expected rows are the post-warmup baseline plus inserts counted only after
   successful issued operations; the final database read is an independent
   observed value, so it cannot define its own oracle. The committed Go
   averages differ by 2.7%, less than the roughly 4% repetition movement.
   Engine CPU near 127% in every cell means S5 is principally engine/Depot-bound:
   the near tie is end-to-end evidence, not an isolated transport-cost result.
   The review smoke produced 322.94 ops/s for FFI and 322.39 ops/s for socket,
   reversing the committed order by 0.17%. Both cells were valid with exact
   row reconciliation. The exact rank is therefore not reproducible; the
   near-tie and engine-bound conclusion is.

9. **Windows and unsupported platforms — clean.** Windows amd64 embeds the FFI
   candidate. Socket is rejected by Rust `validate_config` as structured
   `invalid_config` before runner startup on non-Unix builds. Other targets
   retain the existing unsupported-platform stub. `docs/OPERATIONS.md` now has
   an explicit platform/transport table.

10. **M1-M6 and boundary regression sweep — clean.** The final full race suite
    covers the pre-M7 conformance and hibernation tests. MessagePack scanner
    limits still match the 1 MiB value and 1,024-value chunk schema, while the
    pump now enforces the aggregate result cap. Rust generates the M7 command
    and event goldens; Go decodes all new kinds and byte-identically re-encodes
    the bidirectional config/command shapes. ABI 7 is the only generated ABI,
    ABI 6 is rejected, and no stale ABI artifact remains in the six-target set.

## Depot vendor patch audit

The audit used the local checkout at exact commit
`957d4e482f404913ca1955d8ecc357533f6fd081`. The seven files under
`crates/rivet-depot-client-pinned/src` have the same names and content as
upstream except one hunk in `query.rs`. After `sqlite3_column_type` has already
reported `SQLITE_BLOB`, the patch reads `sqlite3_column_bytes`; length zero
returns `Blob(Vec::new())`, while the old null-pointer fallback remains for an
impossible nonzero-length/null-pointer result. This is narrow and does not
alter any other SQLite value or statement behavior.

The standalone Cargo manifest necessarily expands workspace dependencies to
their pinned crates/tag, supplies local lint compatibility, and disables the
upstream package's unavailable workspace-only test target. Those are packaging
differences, not source behavior changes. The behavior correction is exercised
through both real transports: a bound zero-length blob must return a non-nil,
zero-length `[]byte`, while the adjacent SQL NULL remains nil. The Rust-generated
wire golden independently contains an empty blob. No additional vendored
source deviation was found.

A later full-implementation review added the bounded final-flush teardown
correction documented in `docs/FFI-BOUNDARY.md`; the statement above records
the vendor diff at the time of the M7 audit.

## Transport comparison inputs

These are objective inputs for a later selection; this review does not choose
a winner.

| Input | FFI pump | Actor Runtime Socket v1 |
|---|---|---|
| Committed S5 throughput | 317.6 / 304.8 ops/s, 311.2 average | 326.0 / 313.3 ops/s, 319.7 average |
| Committed S5 runner / engine CPU | 10.6% / 128.0% average | 9.0% / 128.0% average |
| Platform coverage | macOS arm64; Linux amd64/arm64 glibc and musl; Windows amd64 | Unix targets only; Windows rejected during configuration |
| Framing and limits | Existing MessagePack pump; u64 correlations; ordered 1 MiB / 1,024-value chunks; 32 MiB content cap | Separate BARE v1 client; u32 correlations; one negotiated frame; 32 MiB including encoding overhead |
| Protocol capability used by public Exec | FFI command calls the shared single-statement execute method directly | Must use `SqliteQuery`; upstream `SqliteExec` is unparameterized script-only and returns no mutation metadata |
| M7 implementation surface | Builder diff added about 572 Rust lines in `actor_proxy.rs` and 293 Go lines in `internal/pump`; it reuses the existing two packages and shared wire package | Dedicated `internal/sqlitesocket` package: 745 Go implementation lines plus the 165-line pinned schema; Rust still provisions and delivers the endpoint |
| Release/compatibility burden | Six native ABI artifacts must be regenerated and shipped; ABI changes are SDK-owned | Pure-Go client must track experimental upstream BARE v1 and Unix socket lifecycle; no socket path exists on Windows |
| Failure modes observed in review | Lifecycle no-op, aggregate receiver trust, repeated chunk metadata, worker errors collapsed to `internal_error` | Close-before-sleep semantics initially differed, larger hello ceiling was rejected, one-frame result limit; schema copy had drifted textually |

## Verification evidence

- Initial builder-tip `go test -race -count=1 ./...`: passed; conformance
  package 598.474 seconds.
- Final post-fix `go test -race -count=1 ./...`: passed; conformance package
  599.883 seconds, including both transports and all M1-M6/hibernation cases.
- Targeted post-fix `TestPerActorSQLiteConformance`: both `ffi` and `socket`
  passed in 66.676 seconds.
- `cargo test --workspace`: 34 passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `go vet ./...`: passed.
- All six `scripts/build-ffi.sh` targets, twice: passed; the second pass had
  zero generated-output difference, and all checksums validate.
- ABI source and rejection: Rust constant, generated header, and generated Go
  expectation are 7; real loader fixtures reject ABI 1, 5, and 6.
- Default `go run ./cmd/soak`: passed in 2m15.45s with seed
  `1785920606495812000`, 400/400 chat receipts, three intentional action
  panics, one engine restart, and 3/3 goroutines before/after.
- One-repetition 10s/45s S5 smoke per Go transport: FFI 322.94 ops/s,
  9,394/9,394 rows, 128.28% engine CPU; socket 322.39 ops/s,
  9,351/9,351 rows, 127.59% engine CPU. Both were valid with no warmup or load
  errors. The 0.17% reversal does not reproduce the committed rank and does
  reproduce its near-tie interpretation.
