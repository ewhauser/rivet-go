#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="$ROOT_DIR/crates/rivetkit-go-ffi"
HEADER_PATH="$ROOT_DIR/internal/ffi/include/rivetkit_go_ffi.h"
LIB_ROOT="$ROOT_DIR/internal/ffi/lib"
CHECKSUMS_PATH="$ROOT_DIR/internal/ffi/checksums.txt"
ABI_GO_PATH="$ROOT_DIR/internal/ffi/abi_version_generated.go"

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required" >&2
  exit 1
fi
if ! command -v cbindgen >/dev/null 2>&1; then
  echo "cbindgen is required" >&2
  exit 1
fi

if [[ $# -gt 1 ]]; then
  echo "usage: scripts/build-ffi.sh [target-triple]" >&2
  exit 1
fi

if [[ $# -eq 1 ]]; then
  target="$1"
else
  case "$(uname -s)/$(uname -m)" in
    Darwin/arm64) target="aarch64-apple-darwin" ;;
    Linux/x86_64) target="x86_64-unknown-linux-gnu" ;;
    Linux/aarch64|Linux/arm64) target="aarch64-unknown-linux-gnu" ;;
    MINGW*/*|MSYS*/*|CYGWIN*/*) target="x86_64-pc-windows-msvc" ;;
    *)
      echo "unsupported local platform: $(uname -s)/$(uname -m)" >&2
      exit 1
      ;;
  esac
fi

case "$target" in
  aarch64-apple-darwin)
    platform="darwin_arm64"
    artifact_name="librivetkit_go_ffi.dylib"
    ;;
  x86_64-unknown-linux-gnu)
    platform="linux_amd64"
    artifact_name="librivetkit_go_ffi.so"
    ;;
  aarch64-unknown-linux-gnu)
    platform="linux_arm64"
    artifact_name="librivetkit_go_ffi.so"
    ;;
  x86_64-unknown-linux-musl)
    platform="linux_amd64_musl"
    artifact_name="librivetkit_go_ffi.so"
    ;;
  aarch64-unknown-linux-musl)
    platform="linux_arm64_musl"
    artifact_name="librivetkit_go_ffi.so"
    ;;
  x86_64-pc-windows-msvc)
    platform="windows_amd64"
    artifact_name="rivetkit_go_ffi.dll"
    ;;
  *)
    echo "unsupported target: $target" >&2
    exit 1
    ;;
esac

echo "Building rivetkit-go-ffi for $target"
host_target="$(rustc -vV | awk '/^host: / { print $2 }')"
build_command=(cargo build)
build_target="$target"
cross_rustc=""
if [[ "$target" == *-unknown-linux-* || "$target" != "$host_target" ]]; then
  cross_cargo=(cargo)
  if command -v rustup >/dev/null 2>&1 && rustup run 1.97.0 rustc --version >/dev/null 2>&1; then
    cross_cargo=(rustup run 1.97.0 cargo)
    cross_rustc="$(rustup which --toolchain 1.97.0 rustc)"
  fi
  case "$target" in
    *-unknown-linux-gnu)
      if ! command -v zig >/dev/null 2>&1 || ! cargo zigbuild --help >/dev/null 2>&1; then
        echo "zig and cargo-zigbuild are required to build $target reproducibly" >&2
        exit 1
      fi
      build_command=("${cross_cargo[@]}" zigbuild)
      build_target="$target.2.17"
      ;;
    *-unknown-linux-musl)
      if ! command -v zig >/dev/null 2>&1 || ! cargo zigbuild --help >/dev/null 2>&1; then
        echo "zig and cargo-zigbuild are required to build $target reproducibly" >&2
        exit 1
      fi
      build_command=("${cross_cargo[@]}" zigbuild)
      ;;
    *-pc-windows-msvc)
      if ! cargo xwin --help >/dev/null 2>&1; then
        echo "cargo-xwin is required to cross-compile $target" >&2
        exit 1
      fi
      if ! command -v lld-link >/dev/null 2>&1 && [[ -x /opt/homebrew/opt/lld@21/bin/lld-link ]]; then
        export PATH="/opt/homebrew/opt/lld@21/bin:$PATH"
      fi
      if ! command -v lld-link >/dev/null 2>&1; then
        echo "lld-link is required to cross-compile $target" >&2
        exit 1
      fi
      build_command=("${cross_cargo[@]}" xwin build)
      ;;
    *)
      if ! command -v zig >/dev/null 2>&1 || ! cargo zigbuild --help >/dev/null 2>&1; then
        echo "zig and cargo-zigbuild are required to cross-compile $target" >&2
        exit 1
      fi
      build_command=("${cross_cargo[@]}" zigbuild)
      ;;
  esac
fi

if [[ -n "$cross_rustc" ]]; then
  export RUSTC="$cross_rustc"
fi
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
if [[ "$target" == *-unknown-linux-musl ]]; then
  export RUSTFLAGS="-C target-feature=-crt-static"
elif [[ "$target" == *-pc-windows-msvc ]]; then
  export RUSTFLAGS="-C target-feature=+crt-static -C link-arg=/ignore:4099"
fi

"${build_command[@]}" \
  --manifest-path "$ROOT_DIR/Cargo.toml" \
  --package rivetkit-go-ffi \
  --features ffi-test \
  --locked \
  --release \
  --target "$build_target"

artifact="$ROOT_DIR/target/$target/release/$artifact_name"
if [[ ! -f "$artifact" ]]; then
  echo "expected artifact not found: $artifact" >&2
  exit 1
fi

lib_dir="$LIB_ROOT/$platform"
mkdir -p "$lib_dir" "$(dirname "$HEADER_PATH")"
cp "$artifact" "$lib_dir/$artifact_name"

case "$target" in
  *-apple-darwin)
    install_name_tool -id "@rpath/$artifact_name" "$lib_dir/$artifact_name"
    strip -x "$lib_dir/$artifact_name"
    ;;
  *-unknown-linux-*)
    if command -v llvm-strip >/dev/null 2>&1; then
      llvm-strip --strip-unneeded "$lib_dir/$artifact_name"
    elif [[ -x /opt/homebrew/opt/llvm/bin/llvm-strip ]]; then
      /opt/homebrew/opt/llvm/bin/llvm-strip --strip-unneeded "$lib_dir/$artifact_name"
    else
      strip --strip-unneeded "$lib_dir/$artifact_name"
    fi
    ;;
  *-pc-windows-msvc)
    if command -v llvm-strip >/dev/null 2>&1; then
      llvm-strip "$lib_dir/$artifact_name"
    fi
    ;;
esac

echo "Generating C header"
cbindgen \
  --config "$CRATE_DIR/cbindgen.toml" \
  --crate rivetkit-go-ffi \
  --output "$HEADER_PATH" \
  "$CRATE_DIR"

abi_version="$(awk '/^#define RK_ABI_VERSION / { print $3 }' "$HEADER_PATH")"
if [[ ! "$abi_version" =~ ^[0-9]+$ ]]; then
  echo "could not read RK_ABI_VERSION from $HEADER_PATH" >&2
  exit 1
fi

abi_tmp="$(mktemp "$ROOT_DIR/internal/ffi/.abi-version.XXXXXX")"
trap 'rm -f "$abi_tmp"' EXIT
cat >"$abi_tmp" <<EOF
// Code generated by scripts/build-ffi.sh; DO NOT EDIT.

package ffi

const ExpectedABIVersion uint32 = $abi_version
EOF
if [[ ! -f "$ABI_GO_PATH" ]] || ! cmp -s "$abi_tmp" "$ABI_GO_PATH"; then
  mv "$abi_tmp" "$ABI_GO_PATH"
fi

checksums_tmp="$(mktemp "$ROOT_DIR/internal/ffi/.checksums.XXXXXX")"
trap 'rm -f "$abi_tmp" "$checksums_tmp"' EXIT
(
  cd "$ROOT_DIR/internal/ffi"
  find lib -type f | LC_ALL=C sort | while IFS= read -r file; do
    if command -v shasum >/dev/null 2>&1; then
      shasum -a 256 "$file"
    else
      sha256sum "$file"
    fi
  done
) >"$checksums_tmp"
if [[ ! -f "$CHECKSUMS_PATH" ]] || ! cmp -s "$checksums_tmp" "$CHECKSUMS_PATH"; then
  mv "$checksums_tmp" "$CHECKSUMS_PATH"
fi

echo "Wrote internal/ffi/lib/$platform/$artifact_name"
