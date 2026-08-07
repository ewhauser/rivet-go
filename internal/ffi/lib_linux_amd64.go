//go:build !rivetgo_ffi_stub && linux && amd64

package ffi

// The glibc build is tried first; the musl build is the dlopen fallback for
// musl-libc systems such as Alpine.
var nativeArtifacts = []nativeArtifact{
	{dir: "lib/linux_amd64", filename: "librivetkit_go_ffi.so"},
	{dir: "lib/linux_amd64_musl", filename: "librivetkit_go_ffi.so"},
}
