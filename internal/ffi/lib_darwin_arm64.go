//go:build darwin && arm64

package ffi

import "embed"

//go:embed lib/darwin_arm64/librivetkit_go_ffi.dylib checksums.txt
var embeddedFiles embed.FS

const (
	embeddedLibraryDir      = "lib/darwin_arm64"
	embeddedLibraryFilename = "librivetkit_go_ffi.dylib"
)
