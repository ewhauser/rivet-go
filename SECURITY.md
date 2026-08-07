# Security Policy

## Reporting a vulnerability

Report vulnerabilities privately through GitHub's private vulnerability
reporting on this repository (Security → Report a vulnerability). Please do
not open public issues for security reports. You should receive an initial
response within a few days.

## Supported versions

Only the latest tagged release receives security fixes.

## Provenance of the native libraries

This SDK embeds a Rust runtime distributed as prebuilt libraries. Their
integrity story is designed so you never have to trust an opaque blob:

- **Pinned by source.** Each release tag pins the exact SHA-256 of every
  platform's library in `internal/ffi/checksums.txt`, and the loader refuses
  to run anything that does not hash-match before `dlopen`.
- **Reproducibly built by CI.** `.github/workflows/release.yml` rebuilds all
  six artifacts from source on pinned toolchains (Rust 1.97.0, Zig 0.16,
  cargo-zigbuild 0.23, cbindgen 0.29.4, MSVC with `/Brepro`) and refuses to
  publish unless every artifact byte-matches its pinned checksum. Release
  builds use no caches.
- **Attested.** Every release asset carries a Sigstore build-provenance
  attestation linking it to the exact workflow run and commit; verify with
  `gh attestation verify <asset> --repo ewhauser/rivet-go`.
- **Rebuildable by you.** `scripts/build-ffi.sh <target>` reproduces an
  artifact from source with the same pins so you can diff against the
  published bytes.
- **Third-party inventory.** Everything statically linked into the libraries
  is inventoried in `THIRD-PARTY-NOTICES.md` and license-gated in CI by
  `cargo deny`.

The pinned Rivet engine used by tests and the dev launcher is built locally
from a commit-pinned upstream checkout, never downloaded as a binary.

## Workflow hardening

CI follows the practices described in Astral's open-source security posts:
actions pinned to commit SHAs, `permissions: {}` by default with per-job
grants, no credential persistence in checkouts, no `pull_request_target` or
`workflow_run` triggers, cache-free release builds, and `zizmor` auditing
every workflow change in CI. Branch and tag rulesets block force-pushes to
`main` and make release tags immutable.

Some organization-level practices (mandatory 2FA policy, multi-person
release approval environments) do not apply to a single-maintainer personal
repository; they should be adopted if the project moves to an organization.
