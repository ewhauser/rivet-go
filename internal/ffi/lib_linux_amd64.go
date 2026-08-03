//go:build !rivetgo_ffi_stub && linux && amd64

package ffi

import "embed"

//go:embed lib/linux_amd64/librivetkit_go_ffi.so lib/linux_amd64_musl/librivetkit_go_ffi.so checksums.txt
var embeddedFiles embed.FS

var embeddedLibraries = []embeddedLibrary{
	{dir: "lib/linux_amd64", filename: "librivetkit_go_ffi.so"},
	{dir: "lib/linux_amd64_musl", filename: "librivetkit_go_ffi.so"},
}
