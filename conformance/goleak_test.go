package conformance

import (
	"flag"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// go test supplies its 10-minute default as -test.timeout. The complete
	// race-enabled real-engine suite exceeded that bound once M7 began running
	// both SQLite transports. Parse first, then enforce the package's documented
	// ceiling so `go test -race -count=1 ./...` remains the complete gate.
	flag.Parse()
	if configured, err := time.ParseDuration(flag.Lookup("test.timeout").Value.String()); err == nil && configured < 20*time.Minute {
		if err := flag.Set("test.timeout", (20 * time.Minute).String()); err != nil {
			panic(err)
		}
	}
	goleak.VerifyTestMain(m)
}
