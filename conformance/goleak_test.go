package conformance

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// Timeout configuration belongs at the go test invocation. The go command
	// enforces its own deadline outside this process, so changing test.timeout
	// here cannot extend the parent command's kill deadline.
	goleak.VerifyTestMain(m)
}
