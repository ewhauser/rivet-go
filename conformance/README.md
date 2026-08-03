# M1 real-engine conformance

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
