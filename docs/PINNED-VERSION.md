# Pinned Rivet version

M0 pins `rivetkit-core` to the `rivet-dev/rivet` tag `v2.3.10`.

On 2026-08-02, `git ls-remote --tags --refs https://github.com/rivet-dev/rivet.git`
showed `v2.3.10` as the newest stable engine release. The newer `v2.3.11` tags
were release candidates only, so they were excluded by the stable-release rule.

The Cargo dependency uses `default-features = false` with only
`features = ["native-runtime"]`. An initial build with no optional features
failed at `rivet-envoy-client`'s compile-time transport guard because core must
select either its native or WASM WebSocket transport. `native-runtime` is the
smallest feature that satisfies that requirement on macOS; no SQLite feature is
enabled, and M0 does not instantiate the runtime or a storage backend.

The FFI crate has a compile-time assertion against the real
`rivetkit_core::format_actor_key` function. That assertion makes the exact
upstream pin a load-bearing build dependency without leaking an upstream type
or function into the C ABI.
