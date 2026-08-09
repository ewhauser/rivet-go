//go:build !rivetgo_ffi_stub && ((darwin && arm64) || (linux && (amd64 || arm64)) || (windows && amd64))

package ffi

import (
	"errors"
	"sync"
	"testing"
)

func TestRunnerLeasePreventsProcessGlobalReuse(t *testing.T) {
	first, err := acquireRunnerLease()
	if err != nil {
		t.Fatalf("acquire first runner lease: %v", err)
	}
	t.Cleanup(first.release)

	const contenders = 32
	var wait sync.WaitGroup
	errorsSeen := make(chan error, contenders)
	wait.Add(contenders)
	for range contenders {
		go func() {
			defer wait.Done()
			lease, acquireErr := acquireRunnerLease()
			if lease != nil {
				lease.release()
			}
			errorsSeen <- acquireErr
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for acquireErr := range errorsSeen {
		if !errors.Is(acquireErr, errRunnerAlreadyActive) {
			t.Fatalf("concurrent runner lease error = %v, want %v", acquireErr, errRunnerAlreadyActive)
		}
	}

	first.release()
	replacement, err := acquireRunnerLease()
	if err != nil {
		t.Fatalf("acquire runner lease after release: %v", err)
	}
	replacement.release()
}
