//go:build !rivetgo_ffi_stub && linux && arm64

package ffi

import "embed"

//go:embed lib/linux_arm64/librivetkit_go_ffi.so lib/linux_arm64_musl/librivetkit_go_ffi.so checksums.txt
var embeddedFiles embed.FS

var embeddedLibraries = []embeddedLibrary{
	{dir: "lib/linux_arm64", filename: "librivetkit_go_ffi.so"},
	{dir: "lib/linux_arm64_musl", filename: "librivetkit_go_ffi.so"},
}
