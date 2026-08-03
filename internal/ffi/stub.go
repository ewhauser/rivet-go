//go:build rivetgo_ffi_stub || !((darwin && arm64) || (linux && (amd64 || arm64)) || (windows && amd64))

package ffi

import (
	"errors"
	"time"
)

var errUnsupportedPlatform = errors.New("rivet-go native FFI is unsupported on this platform")

// Runner is the unsupported-platform runner stub.
type Runner struct{}

// Error is the unsupported-platform native error stub.
type Error struct{}

// RunnerResult is the unsupported-platform runner result stub.
type RunnerResult struct {
	Runner *Runner
	Error  *Error
}

// ErrorPayload is the stable JSON representation of an FFI error.
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e ErrorPayload) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

func (e ErrorPayload) ErrorCode() string { return e.Code }

// Load reports that the current target has no embedded native library.
func Load() error { return errUnsupportedPlatform }

// ABIVersion reports that the current target has no embedded native library.
func ABIVersion() (uint32, error) { return 0, errUnsupportedPlatform }

// NewRunner reports that the current target has no embedded native library.
func NewRunner([]byte) (RunnerResult, error) { return RunnerResult{}, errUnsupportedPlatform }

// Close is a no-op for an unsupported-platform runner.
func (*Runner) Close() {}

// Poll reports that the current target has no embedded native library.
func (*Runner) Poll(time.Duration) ([]byte, error) { return nil, errUnsupportedPlatform }

// Submit reports that the current target has no embedded native library.
func (*Runner) Submit([]byte) error { return errUnsupportedPlatform }

// Shutdown reports that the current target has no embedded native library.
func (*Runner) Shutdown(time.Duration) error { return errUnsupportedPlatform }

// JSON reports that the current target has no embedded native library.
func (*Error) JSON() ([]byte, error) { return nil, errUnsupportedPlatform }

// Payload reports that the current target has no embedded native library.
func (*Error) Payload() (ErrorPayload, error) { return ErrorPayload{}, errUnsupportedPlatform }

// Close is a no-op for an unsupported-platform error.
func (*Error) Close() {}
