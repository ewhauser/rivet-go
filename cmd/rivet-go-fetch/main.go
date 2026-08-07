// Command rivet-go-fetch pre-seeds the local native-library cache with the
// checksum-verified release artifacts for this platform, so machines that
// later run offline (CI images, air-gapped hosts) never download at runtime.
//
//	go run github.com/ewhauser/rivet-go/cmd/rivet-go-fetch
//
// The cache lives under the user cache directory (rivet-go/<sha256>/...).
// RIVET_GO_FFI_BASE_URL overrides the release-asset host for mirrors;
// RIVET_GO_FFI_LIB bypasses acquisition entirely with an operator-owned
// library path.
package main

import (
	"fmt"
	"os"

	"github.com/ewhauser/rivet-go/internal/ffi"
)

func main() {
	paths, err := ffi.Prefetch()
	if err != nil {
		fmt.Fprintln(os.Stderr, "rivet-go-fetch:", err)
		os.Exit(1)
	}
	for _, path := range paths {
		fmt.Println(path)
	}
}
