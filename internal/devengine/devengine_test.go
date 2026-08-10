package devengine

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

func TestEngineStopFallsBackToKillOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-specific process signaling behavior")
	}

	command := exec.Command(os.Args[0], "-test.run=^TestEngineStopHelperProcess$")
	command.Env = append(os.Environ(), "RIVET_GO_ENGINE_STOP_HELPER=1")
	if err := command.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	process := &engineProcess{
		command: command,
		done:    make(chan struct{}),
	}
	go func() {
		process.waitErr = command.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
			t.Error("helper process was not reaped during cleanup")
		}
	})

	engine := &Engine{process: process}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := engine.Stop(ctx); err != nil {
		t.Fatalf("stop engine: %v", err)
	}
	select {
	case <-process.done:
	case <-time.After(time.Second):
		t.Fatal("Stop returned before helper process was reaped")
	}
}

func TestEngineStopHelperProcess(t *testing.T) {
	if os.Getenv("RIVET_GO_ENGINE_STOP_HELPER") != "1" {
		return
	}
	time.Sleep(time.Minute)
}
