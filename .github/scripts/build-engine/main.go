// Command build-engine prepares and verifies the exact engine used by CI.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ewhauser/rivet-go/internal/devengine"
)

func main() {
	path, err := devengine.Acquire(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "acquire pinned engine: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("verified pinned engine: %s\n", path)
}
