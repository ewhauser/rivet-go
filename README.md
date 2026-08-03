# rivet-go

`rivet-go` is a Go SDK for hosting Rivet actors by loading a small Rust
`rivetkit-core` adapter through [purego](https://github.com/ebitengine/purego).
Applications do not need cgo or a native toolchain at Go build time.

Status: **M0 skeleton**. The pinned Rust FFI crate builds, all six matrix
libraries are embedded and checksum-verified, and Go validates ABI version 1.
Linux selects the glibc or musl artifact at load time. The runner entry points
intentionally return `not_implemented`; actor and pump semantics begin in M1.

## Build and test

Install Rust 1.97, Go 1.26, and `cbindgen`, then run:

```sh
cargo install cbindgen --version 0.29.4 --locked
scripts/build-ffi.sh
go test ./...
cargo test --workspace
```

Linux builds additionally require Zig 0.16 and `cargo-zigbuild` 0.23. Cross
compiling the Windows MSVC artifact requires `cargo-xwin` 0.23 and `lld-link`.

See [the implementation plan](docs/PLAN.md), [FFI contract](docs/FFI-BOUNDARY.md),
and [pinned Rivet version](docs/PINNED-VERSION.md) for the design and roadmap.
