//go:build !rivetgo_ffi_stub && windows && amd64

package ffi

import "embed"

//go:embed lib/windows_amd64/rivetkit_go_ffi.dll checksums.txt
var embeddedFiles embed.FS

var embeddedLibraries = []embeddedLibrary{{
	dir:      "lib/windows_amd64",
	filename: "rivetkit_go_ffi.dll",
}}
