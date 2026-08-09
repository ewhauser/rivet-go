# Continuous integration

The `verify` workflow runs for pull requests and pushes to `main` or
`ci-green`. It exercises every shipped native library and keeps the expensive
real-engine build separate from the tests that consume it.

## Jobs and timeouts

| Job | Purpose | Timeout |
| --- | --- | ---: |
| `darwin-arm64` | Test the committed library, rebuild it, run Rust tests, and retest Go | 45 minutes |
| `linux-amd64-gnu` | Test and rebuild the glibc x86-64 library | 45 minutes |
| `linux-arm64-gnu` | Test and rebuild the glibc arm64 library | 45 minutes |
| `linux-amd64-musl` | Test and rebuild the musl x86-64 library inside Alpine | 45 minutes |
| `linux-arm64-musl` | Test and rebuild the musl arm64 library inside Alpine | 45 minutes |
| `windows-amd64` | Test and rebuild the Windows x86-64 library | 45 minutes |
| `linux-amd64-engine` | Build or verify the exact pinned Rivet engine | 60 minutes |
| `linux-amd64-conformance` | Run the complete race-enabled Go and real-engine suite | 35 minutes |
| `linux-amd64-soak-smoke` | Run the strict two-minute chaos smoke | 15 minutes |

The musl containers install Alpine's native `rustc` before both Go test
passes. The loader's ABI-compatibility tests compile small fixture libraries,
so the compiler is a test dependency even when the committed FFI library is
already present.

## Pinned engine cache

`linux-amd64-engine` is the only job allowed to build Rivet Engine from source.
The acquisition helper checks out tag `v2.3.10` at commit
`957d4e482f404913ca1955d8ecc357533f6fd081`, runs Cargo with the checkout as
its working directory, and verifies the resulting binary's reported version
and commit. Running from the checkout is required for Rivet's
`.cargo/config.toml`, including its `tokio_unstable` flags, to apply.

The cache key contains the engine tag, runner OS and architecture, exact commit,
and acquisition-format suffix. Only the verified `rivet-engine` binary and
`build.log` are cached; the source and Cargo target tree are deliberately left
out. Conformance and soak both depend on the builder and restore that exact key
with `fail-on-cache-miss`, so they never start independent source builds. The
first run for a new key pays the build cost once; later runs normally verify a
cache hit in seconds.

Conformance passes `-timeout=25m` explicitly because the complete ABI-9,
race-enabled suite can exceed Go's default ten-minute alarm. The job timeout
leaves ten more minutes for checkout, cache restore, package setup, and log
upload.

## Failure artifacts

Engine-dependent commands stream their full output to the Actions log and a
local file. On failure, the workflow uploads artifacts retained for 14 days:

- `linux-amd64-engine-logs`: engine acquisition output and Cargo `build.log`;
- `linux-amd64-conformance-logs`: complete test output and the engine build
  log; and
- `linux-amd64-soak-smoke-logs`: complete soak output and the engine build log.

Artifact upload runs only after a failure and does not change the failing job's
conclusion.
