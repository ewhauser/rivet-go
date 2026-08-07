//go:build !rivetgo_ffi_stub && windows && amd64

package ffi

var nativeArtifacts = []nativeArtifact{{dir: "lib/windows_amd64", filename: "rivetkit_go_ffi.dll"}}
