//go:build !rivetgo_ffi_stub && ((darwin && arm64) || (linux && (amd64 || arm64)) || (windows && amd64))

package ffi

import "embed"

// Only the checksum manifest is embedded; the native libraries themselves are
// release assets acquired by acquire.go against these pinned digests.
//
//go:embed checksums.txt
var embeddedFiles embed.FS
