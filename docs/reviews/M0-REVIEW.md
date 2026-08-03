# M0 adversarial review

## Summary

| Severity | Found | Fixed | Outstanding |
| --- | ---: | ---: | ---: |
| Blocker | 1 | 1 | 0 |
| Major | 4 | 4 | 0 |
| Minor | 1 | 1 | 0 |
| **Total** | **6** | **6** | **0** |

Review baseline: `4cb3698` (the builder's last commit after the authoritative
`45669fa docs: rivet-go plan and FFI boundary spec` baseline). The fixes are in
`566d215` and `333a6ee`. Neither `docs/PLAN.md` nor
`docs/FFI-BOUNDARY.md` was changed.

## Findings

1. **Blocker — the advertised target matrix compiled unsupported Go stubs and
   did not build musl at all.**

   Evidence: at `4cb3698`, the only real loader had the build constraint
   `darwin && arm64`; `GOOS=linux GOARCH=amd64 go test ./...` and the equivalent
   Windows command reported `[no test files]`. Thus the Linux and Windows CI
   jobs could pass without loading a library or crossing the ABI. Running
   `bash scripts/build-ffi.sh x86_64-unknown-linux-musl` exited 1 with
   `TODO: musl shared-library builds require a dedicated builder`. Meanwhile,
   `4cb3698:.github/workflows/verify.yml:62-67` implemented musl as an
   unconditional-success `echo` job.

   Fix: `566d215` adds supported loaders, embedded artifacts, and real boundary
   tests for Darwin arm64, Linux amd64/arm64 (glibc and musl), and Windows
   amd64. It also makes the build script produce all six artifacts. `333a6ee`
   replaces the placeholder workflow with six native jobs that test the
   committed artifact before rebuilding and the rebuilt artifact afterward.
   Musl tests run under Alpine on native-architecture Linux runners.

2. **Major — the panic-firewall test never exercised the loaded cdylib.**

   Evidence: `4cb3698:crates/rivetkit-go-ffi/src/lib.rs:270-274` guarded
   `rk_test_panic` with `#[cfg(test)]`, so it existed only in the Rust test
   executable. `nm` on the committed cdylib found no `rk_test_panic` export,
   and there was no Go panic-boundary test. The Rust unit test therefore could
   not prove that unwinding was caught inside the actual dynamically loaded
   `extern "C"` function.

   Fix: `566d215` adds a private `ffi-test` build feature for the committed
   testable libraries, keeps the probe out of the public header, and invokes it
   through the loaded library in
   `internal/ffi/loader_supported_test.go:109-142`. The test requires the exact
   structured `internal_panic` response and successfully calls
   `rk_abi_version` after the panic. The crate now rejects `panic=abort` at
   compile time (`crates/rivetkit-go-ffi/src/lib.rs:14-15`).

3. **Major — the upstream `rivetkit-core` pin was not load-bearing.**

   Evidence: the only use was the compile-time function-item type assertion at
   `4cb3698:crates/rivetkit-go-ffi/src/lib.rs:17-19`. It made Cargo resolve and
   type-check the dependency but did not require any upstream implementation in
   the optimized cdylib. This contradicted `docs/PINNED-VERSION.md`, which said
   the pin was load-bearing.

   Fix: `566d215` adds a non-inlined runtime probe that constructs an upstream
   `ActorKey` and calls `rivetkit_core::format_actor_key`
   (`crates/rivetkit-go-ffi/src/lib.rs:20-30`). `rk_abi_version` executes that
   probe through the loaded boundary (`:187-194`), and a Rust regression test
   also executes it. The exact `v2.3.10` tag and locked Git SHA remain in
   `Cargo.toml` and committed `Cargo.lock`; the pin document now describes the
   runtime dependency accurately.

4. **Major — a Go finalizer could free an error handle during
   `rk_error_json`.**

   Evidence: `4cb3698:internal/ffi/ffi_darwin_arm64.go:218-232` passed the
   borrowed `e.ptr` into native code but never kept the owning Go object live
   across the call. Once the receiver's pointer was fetched, a concurrent
   finalizer could call `rk_error_free` while Rust serialized the error.

   Fix: `566d215` places `runtime.KeepAlive(e)` immediately after the native
   call (`internal/ffi/ffi_supported.go:237-251`). The existing deferred
   `rk_bytes_free` continues to release the returned Rust buffer exactly once.

5. **Major — native-library extraction trusted an unsafe, predictable cache
   path and could not reliably repair a corrupt Windows entry.**

   Evidence: `4cb3698:internal/ffi/ffi_darwin_arm64.go:291-323` used
   `MkdirAll(..., 0755)`, followed existing path components, accepted existing
   directories without checking their type or permissions, and emitted a
   world-readable/executable library. The fallback root is under the shared
   system temporary directory. The replacement logic also attempted
   `os.Rename` over a corrupt destination, which fails on Windows.

   Fix: `566d215` requires real (non-symlink) cache directories, forces both
   cache levels to `0700`, forces libraries to `0500`, validates cached bytes,
   and removes an invalid destination before the atomic rename
   (`internal/ffi/ffi_supported.go:281-376`). Regression tests begin with an
   intentionally `0777` cache and tamper an extracted file before re-extraction
   (`internal/ffi/loader_supported_test.go:213-296`).

6. **Minor — the committed Mach-O library embedded the builder's absolute
   workspace path as its install name.**

   Evidence: before the fix, `otool -D
   internal/ffi/lib/darwin_arm64/librivetkit_go_ffi.dylib` printed
   `/Users/ewhauser/working/rivet-go/target/aarch64-apple-darwin/release/librivetkit_go_ffi.dylib`.
   This leaked a local path and made otherwise equivalent checkouts produce
   different library bytes and checksums.

   Fix: `566d215` normalizes the install name to
   `@rpath/librivetkit_go_ffi.dylib` before stripping and checksumming
   (`scripts/build-ffi.sh:150-153`). `otool -D` now reports that normalized
   value.

## Checked, clean

- **Vacuous tests:** `TestLoadAndABIVersion` fails on any load error and obtains
  the version from the registered native function. The checksum test writes
  bytes different from the manifested bytes and requires `checksum mismatch`.
  The loaded-artifact test compares every embedded artifact with its on-disk
  file and manifest, then verifies that the actually loaded path is under the
  matching SHA-256 directory.
- **Panic mechanics:** `cargo rustc --release -- --print cfg` reported
  `panic="unwind"`; the crate makes an abort profile a compile error. Every
  exported function in the generated C header is `extern "C"` and contains or
  delegates to an internal `catch_unwind`. The test-only panic export is present
  in each committed library but absent from the public header.
- **Pin consistency:** `Cargo.toml` pins `rivetkit-core` to exact tag `v2.3.10`;
  committed `Cargo.lock` resolves it to
  `957d4e482f710721d9617b63634cb72479c5330f`; the pin document agrees.
- **ABI source of truth:** `RK_ABI_VERSION` originates in Rust, cbindgen writes
  the committed header, and the build script derives the generated Go constant
  from that header. Two clean builds converged with no generated-file diff.
- **Binding and ownership audit:** every purego signature was compared by hand
  with the generated C header. The boundary test invokes every M0 binding.
  Borrowed Go config and error handles have `runtime.KeepAlive`; returned error
  JSON is copied before exactly one `rk_bytes_free`; runner/error handles clear
  finalizers and free once.
- **Unsupported-platform stub:** `GOOS=linux` (amd64 and arm64), `GOOS=windows`
  (amd64), and `GOOS=freebsd` (amd64) vet builds pass. A forced
  `rivetgo_ffi_stub` test runs on the host and proves every public stub API
  returns a non-nil unsupported-platform error.
- **Scope:** no pump, envelope, wire protocol, or other M1 implementation was
  added.
- **Workflow:** `actionlint -shellcheck shellcheck
  .github/workflows/verify.yml` and `shellcheck scripts/build-ffi.sh` pass. The
  workflow has no `|| true`, TODO success job, or missing script reference.

## Final verification

The following commands were run after the fixes:

```text
cargo clean
bash scripts/build-ffi.sh
git status --porcelain && git diff --exit-code
bash scripts/build-ffi.sh
git status --porcelain && git diff --exit-code
```

Both builds produced Darwin artifact SHA-256
`6786609ffc7e814e303a94ebb7ad8018246f8f59382231d834bf0a17b6c3683b`.
Both diff checks were empty, including the required second build.

```text
(cd crates/rivetkit-go-ffi && cargo test)       # 3 passed
go test -count=1 ./...                          # passed
go test -race -count=1 ./...                    # passed
go test -count=1 -tags rivetgo_ffi_stub ./...   # passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --check
(cd internal/ffi && shasum -a 256 -c checksums.txt)
```

All commands passed. The checksum command reported `OK` for all six committed
libraries. All six targets were also rebuilt twice; each target's artifact hash
was identical across the two builds. Format, architecture, exports, and dynamic
dependencies were inspected with `file`, `nm`/`llvm-nm`, `otool`, `readelf`,
and `llvm-readobj`. In particular, the glibc artifacts require no GLIBC symbol
newer than 2.14, and the Windows DLL imports only system DLLs rather than the
dynamic MSVC runtime.

This review host is Darwin arm64 and has no working Docker/QEMU/Windows VM, so
the cross-built Linux and Windows libraries could not be executed locally.
Their compile-time Go coverage, binary structure, exports, dependencies, and
checksums were validated locally. The corrected workflow makes runtime loading,
ABI, panic-firewall, ownership, and checksum tests mandatory on native runners
for all six target/libc combinations; that workflow has not been run remotely
because this review did not push changes.
