//go:build !rivetgo_ffi_stub && windows && amd64

package ffi

import (
	"fmt"

	"github.com/ebitengine/purego"
)

func (a *nativeAPI) register(handle uintptr) error {
	var bytesFree func(*byte, uintptr, uintptr)
	var stringFree func(*byte, uintptr, uintptr)
	var errorJSON func(*cError, *cBytes)
	var runnerNew func(*byte, uintptr, *cRunnerResult)
	var runnerPoll func(*cRunner, uint32, *cPollResult)
	var runnerSubmit func(*cRunner, *byte, uintptr, *cSubmitResult)
	var runnerShutdown func(*cRunner, uint32, *cSubmitResult)

	for _, binding := range []struct {
		name string
		dst  any
	}{
		{"rk_windows_bytes_free", &bytesFree},
		{"rk_windows_string_free", &stringFree},
		{"rk_windows_error_json", &errorJSON},
		{"rk_error_free", &a.errorFree},
		{"rk_windows_runner_new", &runnerNew},
		{"rk_runner_free", &a.runnerFree},
		{"rk_windows_runner_poll", &runnerPoll},
		{"rk_windows_runner_submit", &runnerSubmit},
		{"rk_windows_runner_shutdown", &runnerShutdown},
	} {
		if err := registerLibraryFunc(handle, binding.name, binding.dst); err != nil {
			return err
		}
	}

	a.bytesFree = func(bytes cBytes) {
		bytesFree(bytes.ptr, bytes.len, bytes.cap)
	}
	a.stringFree = func(bytes cBytes) {
		stringFree(bytes.ptr, bytes.len, bytes.cap)
	}
	a.errorJSON = func(nativeError *cError) (result cBytes) {
		errorJSON(nativeError, &result)
		return result
	}
	a.runnerNew = func(config *byte, length uintptr) (result cRunnerResult) {
		runnerNew(config, length, &result)
		return result
	}
	a.runnerPoll = func(runner *cRunner, timeout uint32) (result cPollResult) {
		runnerPoll(runner, timeout, &result)
		return result
	}
	a.runnerSubmit = func(runner *cRunner, batch *byte, length uintptr) (result cSubmitResult) {
		runnerSubmit(runner, batch, length, &result)
		return result
	}
	a.runnerShutdown = func(runner *cRunner, timeout uint32) (result cSubmitResult) {
		runnerShutdown(runner, timeout, &result)
		return result
	}
	return nil
}

func registerPanicProbe(handle uintptr) (func() cSubmitResult, error) {
	var probe func(*cSubmitResult)
	if err := registerLibraryFunc(handle, "rk_windows_test_panic", &probe); err != nil {
		return nil, err
	}
	return func() (result cSubmitResult) {
		probe(&result)
		return result
	}, nil
}

func registerLibraryFunc(handle uintptr, name string, dst any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("register native function %s: %v", name, recovered)
		}
	}()
	purego.RegisterLibFunc(dst, handle, name)
	return nil
}
