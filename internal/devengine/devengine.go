// Package devengine acquires and runs the exact engine pinned by rivet-go.
package devengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Tag        = "v2.3.10"
	Version    = "2.3.10"
	Commit     = "957d4e482f404913ca1955d8ecc357533f6fd081"
	Repository = "https://github.com/rivet-dev/rivet.git"
)

const startupTimeout = 20 * time.Second

// Acquire returns an executable for the pinned engine. RIVET_GO_ENGINE_BIN
// wins, followed by the conformance cache and an exact-tag source build.
func Acquire(ctx context.Context) (string, error) {
	if override := os.Getenv("RIVET_GO_ENGINE_BIN"); override != "" {
		return verifyBinary(ctx, override)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home directory: %w", err)
	}
	cache := filepath.Join(home, ".cache", "rivet-go", "engine-"+Tag)
	binary := filepath.Join(cache, executableName("rivet-engine"))
	if path, verifyErr := verifyBinary(ctx, binary); verifyErr == nil {
		return path, nil
	}
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return "", fmt.Errorf("create engine cache: %w", err)
	}
	return buildPinned(ctx, cache)
}

func buildPinned(ctx context.Context, cache string) (string, error) {
	source := filepath.Join(cache, "source")
	if _, err := os.Stat(filepath.Join(source, ".git")); errors.Is(err, os.ErrNotExist) {
		command := exec.CommandContext(ctx, "git", "clone", "--depth", "1", "--branch", Tag, Repository, source)
		if output, runErr := command.CombinedOutput(); runErr != nil {
			return "", fmt.Errorf("clone pinned engine source: %w: %s", runErr, tail(output, 20))
		}
	} else if err != nil {
		return "", fmt.Errorf("inspect cached engine source: %w", err)
	}
	commit, err := exec.CommandContext(ctx, "git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("verify cached engine source: %w", err)
	}
	if got := strings.TrimSpace(string(commit)); got != Commit {
		return "", fmt.Errorf("cached source is %s, want pinned commit %s; move %s aside and retry", got, Commit, source)
	}

	target := filepath.Join(cache, "target")
	command := pinnedBuildCommand(ctx, source, target)
	logPath := filepath.Join(cache, "build.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return "", fmt.Errorf("create engine build log: %w", err)
	}
	command.Stdout = logFile
	command.Stderr = logFile
	runErr := command.Run()
	closeErr := logFile.Close()
	if runErr != nil {
		logData, _ := os.ReadFile(logPath)
		return "", fmt.Errorf("build pinned engine: %w\nlast build output:\n%s", runErr, tail(logData, 40))
	}
	if closeErr != nil {
		return "", fmt.Errorf("close engine build log: %w", closeErr)
	}

	built := filepath.Join(target, "release", executableName("rivet-engine"))
	data, err := os.ReadFile(built)
	if err != nil {
		return "", fmt.Errorf("read built engine: %w", err)
	}
	destination := filepath.Join(cache, executableName("rivet-engine"))
	if err := os.WriteFile(destination, data, 0o755); err != nil {
		return "", fmt.Errorf("cache built engine: %w", err)
	}
	return verifyBinary(ctx, destination)
}

func pinnedBuildCommand(ctx context.Context, source, target string) *exec.Cmd {
	command := exec.CommandContext(
		ctx,
		"cargo", "build",
		"--manifest-path", filepath.Join(source, "Cargo.toml"),
		"-p", "rivet-engine",
		"--release",
		"--target-dir", target,
	)
	// Cargo discovers .cargo/config.toml from its working directory, not from
	// --manifest-path. Rivet's checkout enables tokio_unstable there for the
	// runtime metrics APIs used by the pinned engine.
	command.Dir = source
	return command
}

// Engine is one restartable local engine process. Restarts retain StorageDir.
type Engine struct {
	Binary     string
	Endpoint   string
	StorageDir string
	LogPath    string
	Port       int

	mu      sync.Mutex
	process *engineProcess
}

type engineProcess struct {
	command *exec.Cmd
	done    chan struct{}
	waitErr error
}

// New constructs an engine process definition without starting it.
func New(binary, storageDir string, port int) (*Engine, error) {
	if _, err := verifyExecutable(binary); err != nil {
		return nil, fmt.Errorf("verify engine executable: %w", err)
	}
	if strings.TrimSpace(storageDir) == "" {
		return nil, errors.New("engine storage directory must not be empty")
	}
	if port <= 1024 || port >= 65525 {
		return nil, fmt.Errorf("engine base port %d must be between 1025 and 65524", port)
	}
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return nil, fmt.Errorf("create engine storage directory: %w", err)
	}
	return &Engine{
		Binary:     binary,
		Endpoint:   fmt.Sprintf("http://127.0.0.1:%d", port),
		StorageDir: storageDir,
		LogPath:    filepath.Join(storageDir, "engine.log"),
		Port:       port,
	}, nil
}

// Start launches the engine and waits for its health endpoint.
func (e *Engine) Start(ctx context.Context) error {
	if e == nil {
		return errors.New("engine is nil")
	}
	e.mu.Lock()
	if e.process != nil {
		e.mu.Unlock()
		return errors.New("engine process is already running")
	}
	logFile, err := os.OpenFile(e.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		e.mu.Unlock()
		return fmt.Errorf("open engine log: %w", err)
	}
	command := exec.Command(e.Binary, "start")
	command.Env = append(os.Environ(),
		"RIVET__GUARD__HOST=127.0.0.1",
		"RIVET__GUARD__PORT="+strconv.Itoa(e.Port),
		"RIVET__API_PEER__HOST=127.0.0.1",
		"RIVET__API_PEER__PORT="+strconv.Itoa(e.Port+1),
		"RIVET__METRICS__HOST=127.0.0.1",
		"RIVET__METRICS__PORT="+strconv.Itoa(e.Port+10),
		"RIVET__FILE_SYSTEM__PATH="+filepath.Join(e.StorageDir, "db"),
	)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		e.mu.Unlock()
		_ = logFile.Close()
		return fmt.Errorf("start engine %s: %w", e.Binary, err)
	}
	process := &engineProcess{
		command: command,
		done:    make(chan struct{}),
	}
	e.process = process
	e.mu.Unlock()
	go func() {
		process.waitErr = command.Wait()
		_ = logFile.Close()
		close(process.done)
	}()

	if err := e.waitReady(ctx, process); err != nil {
		_ = e.killProcess(process)
		return err
	}
	return nil
}

func (e *Engine) waitReady(ctx context.Context, process *engineProcess) error {
	startupCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: time.Second}
	var lastErr error
	for {
		request, err := http.NewRequestWithContext(startupCtx, http.MethodGet, e.Endpoint+"/health", nil)
		if err != nil {
			return fmt.Errorf("build engine health request: %w", err)
		}
		response, err := client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("engine health returned %s", response.Status)
		} else {
			lastErr = err
		}
		select {
		case <-process.done:
			e.clearProcess(process)
			return fmt.Errorf("engine exited before health check: %v\nlast engine output:\n%s", process.waitErr, ReadLogTail(e.LogPath))
		case <-startupCtx.Done():
			return fmt.Errorf("engine did not become healthy within %s: %v\nlast engine output:\n%s", startupTimeout, lastErr, ReadLogTail(e.LogPath))
		case <-ticker.C:
		}
	}
}

// Kill terminates the process and waits for it to be reaped.
func (e *Engine) Kill() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	process := e.process
	e.mu.Unlock()
	if process == nil {
		return nil
	}
	return e.killProcess(process)
}

func (e *Engine) killProcess(process *engineProcess) error {
	if process.command.Process != nil {
		if err := process.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("kill engine: %w", err)
		}
	}
	select {
	case <-process.done:
		e.clearProcess(process)
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("engine process was not reaped within 5s after kill")
	}
}

// Stop asks the engine to exit and falls back to Kill when ctx expires.
func (e *Engine) Stop(ctx context.Context) error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	process := e.process
	e.mu.Unlock()
	if process == nil {
		return nil
	}
	if process.command.Process != nil {
		// Windows does not implement os.Interrupt for child processes. Fall back
		// to the same hard kill Stop uses when a graceful shutdown times out.
		if runtime.GOOS == "windows" {
			return e.killProcess(process)
		}
		if err := process.command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("signal engine: %w", err)
		}
	}
	select {
	case <-process.done:
		e.clearProcess(process)
		return nil
	case <-ctx.Done():
		if err := e.killProcess(process); err != nil {
			return errors.Join(ctx.Err(), err)
		}
		return nil
	}
}

func (e *Engine) clearProcess(process *engineProcess) {
	e.mu.Lock()
	if e.process == process {
		e.process = nil
	}
	e.mu.Unlock()
}

// ReservePortRange finds a base port whose guard, peer, and metrics ports are
// all available at the time of the check.
func ReservePortRange() (int, error) {
	for range 100 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return 0, fmt.Errorf("reserve engine port: %w", err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
		if port > 1024 && port < 65525 && portsAvailable(port, port+1, port+10) {
			return port, nil
		}
	}
	return 0, errors.New("could not find three available engine ports")
}

func portsAvailable(ports ...int) bool {
	listeners := make([]net.Listener, 0, len(ports))
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	for _, port := range ports {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return false
		}
		listeners = append(listeners, listener)
	}
	return true
}

func verifyBinary(ctx context.Context, path string) (string, error) {
	path, err := verifyExecutable(path)
	if err != nil {
		return "", err
	}
	output, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s --version: %w: %s", path, err, tail(output, 5))
	}
	text := string(output)
	if !strings.Contains(text, Version) || !strings.Contains(text, Commit) {
		return "", fmt.Errorf("%s reports %q, want version %s commit %s", path, strings.TrimSpace(text), Version, Commit)
	}
	return path, nil
}

func verifyExecutable(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", path)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("%s is not executable", path)
	}
	return path, nil
}

func executableName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

// ReadLogTail returns the final 60 lines of an engine log.
func ReadLogTail(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return tail(data, 60)
}

func tail(data []byte, lines int) string {
	parts := bytes.Split(data, []byte("\n"))
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return string(bytes.Join(parts, []byte("\n")))
}
