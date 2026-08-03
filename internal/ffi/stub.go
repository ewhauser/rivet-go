//go:build !(darwin && arm64)

package ffi

import "errors"

var errUnsupportedPlatform = errors.New("rivet-go native FFI is unsupported on this platform; M0 supports darwin/arm64")

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

// Load reports that the current target has no embedded native library.
func Load() error { return errUnsupportedPlatform }

// ABIVersion reports that the current target has no embedded native library.
func ABIVersion() (uint32, error) { return 0, errUnsupportedPlatform }

// NewRunner reports that the current target has no embedded native library.
func NewRunner([]byte) (RunnerResult, error) { return RunnerResult{}, errUnsupportedPlatform }

// Close is a no-op for an unsupported-platform runner.
func (*Runner) Close() {}

// JSON reports that the current target has no embedded native library.
func (*Error) JSON() ([]byte, error) { return nil, errUnsupportedPlatform }

// Payload reports that the current target has no embedded native library.
func (*Error) Payload() (ErrorPayload, error) { return ErrorPayload{}, errUnsupportedPlatform }

// Close is a no-op for an unsupported-platform error.
func (*Error) Close() {}
