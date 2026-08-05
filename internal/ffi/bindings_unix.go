//go:build !rivetgo_ffi_stub && ((darwin && arm64) || (linux && (amd64 || arm64)))

package ffi

import (
	"fmt"

	"github.com/ebitengine/purego"
)

func (a *nativeAPI) register(handle uintptr) error {
	for _, binding := range []struct {
		name string
		dst  any
	}{
		{"rk_bytes_free", &a.bytesFree},
		{"rk_string_free", &a.stringFree},
		{"rk_error_json", &a.errorJSON},
		{"rk_error_free", &a.errorFree},
		{"rk_runner_new", &a.runnerNew},
		{"rk_runner_free", &a.runnerFree},
		{"rk_runner_poll", &a.runnerPoll},
		{"rk_runner_submit", &a.runnerSubmit},
		{"rk_runner_shutdown", &a.runnerShutdown},
	} {
		if err := registerLibraryFunc(handle, binding.name, binding.dst); err != nil {
			return err
		}
	}
	return nil
}

func registerPanicProbe(handle uintptr) (func() cSubmitResult, error) {
	var probe func() cSubmitResult
	if err := registerLibraryFunc(handle, "rk_test_panic", &probe); err != nil {
		return nil, err
	}
	return probe, nil
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
