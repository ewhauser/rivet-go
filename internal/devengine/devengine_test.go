package devengine

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestPinnedBuildRunsFromSourceCheckout(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	target := filepath.Join(t.TempDir(), "target")
	command := pinnedBuildCommand(context.Background(), source, target)
	if command.Dir != source {
		t.Fatalf("cargo working directory = %q, want source checkout %q", command.Dir, source)
	}
	wantManifest := filepath.Join(source, "Cargo.toml")
	foundManifest := false
	for index, argument := range command.Args[:len(command.Args)-1] {
		if argument == "--manifest-path" && command.Args[index+1] == wantManifest {
			foundManifest = true
			break
		}
	}
	if !foundManifest {
		t.Fatalf("cargo arguments %q do not select manifest %q", command.Args, wantManifest)
	}
}

func TestEngineProcessExitCanBeObservedByConcurrentKillCallers(t *testing.T) {
	process := &engineProcess{
		command: &exec.Cmd{},
		done:    make(chan struct{}),
	}
	close(process.done)

	engine := &Engine{process: process}
	const callers = 10
	start := make(chan struct{})
	results := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			results <- engine.killProcess(process)
		}()
	}
	close(start)

	for range callers {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("observe completed process: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent kill caller did not observe process completion")
		}
	}
}
