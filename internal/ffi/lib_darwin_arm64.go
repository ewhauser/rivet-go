//go:build !rivetgo_ffi_stub && darwin && arm64

package ffi

var nativeArtifacts = []nativeArtifact{{dir: "lib/darwin_arm64", filename: "librivetkit_go_ffi.dylib"}}
