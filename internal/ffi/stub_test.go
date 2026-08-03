//go:build rivetgo_ffi_stub || !((darwin && arm64) || (linux && (amd64 || arm64)) || (windows && amd64))

package ffi

import (
	"errors"
	"testing"
	"time"
)

func TestUnsupportedPlatformReturnsErrors(t *testing.T) {
	if err := Load(); !errors.Is(err, errUnsupportedPlatform) {
		t.Fatalf("Load error = %v, want %v", err, errUnsupportedPlatform)
	}
	if version, err := ABIVersion(); version != 0 || !errors.Is(err, errUnsupportedPlatform) {
		t.Fatalf("ABIVersion = (%d, %v), want (0, %v)", version, err, errUnsupportedPlatform)
	}
	if result, err := NewRunner(nil); result != (RunnerResult{}) || !errors.Is(err, errUnsupportedPlatform) {
		t.Fatalf("NewRunner = (%v, %v), want zero result and %v", result, err, errUnsupportedPlatform)
	}

	var runner *Runner
	if data, err := runner.Poll(time.Millisecond); data != nil || !errors.Is(err, errUnsupportedPlatform) {
		t.Fatalf("Runner.Poll = (%v, %v), want (nil, %v)", data, err, errUnsupportedPlatform)
	}
	if err := runner.Submit(nil); !errors.Is(err, errUnsupportedPlatform) {
		t.Fatalf("Runner.Submit error = %v, want %v", err, errUnsupportedPlatform)
	}
	if err := runner.Shutdown(time.Millisecond); !errors.Is(err, errUnsupportedPlatform) {
		t.Fatalf("Runner.Shutdown error = %v, want %v", err, errUnsupportedPlatform)
	}
	runner.Close()
	var nativeError *Error
	if data, err := nativeError.JSON(); data != nil || !errors.Is(err, errUnsupportedPlatform) {
		t.Fatalf("Error.JSON = (%v, %v), want (nil, %v)", data, err, errUnsupportedPlatform)
	}
	if payload, err := nativeError.Payload(); payload != (ErrorPayload{}) || !errors.Is(err, errUnsupportedPlatform) {
		t.Fatalf("Error.Payload = (%v, %v), want zero payload and %v", payload, err, errUnsupportedPlatform)
	}
	nativeError.Close()
}
