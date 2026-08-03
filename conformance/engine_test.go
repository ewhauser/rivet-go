package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/internal/ffi"
	"github.com/ewhauser/rivet-go/internal/wire"
	"github.com/ewhauser/rivet-go/rivet"
)

const (
	engineTag        = "v2.3.10"
	engineVersion    = "2.3.10"
	engineCommit     = "957d4e482f404913ca1955d8ecc357533f6fd081"
	engineRepository = "https://github.com/rivet-dev/rivet.git"
	startupBound     = 13 * time.Second
	// rivet-envoy-client v2.3.10 declares a 20-second ping-health threshold;
	// the adapter samples that status every 250 milliseconds.
	disconnectLivenessWindow = 22 * time.Second
)

type runningEngine struct {
	endpoint string
	command  *exec.Cmd
	logPath  string
}

type envoyListResponse struct {
	Envoys []envoyRecord `json:"envoys"`
}

type envoyRecord struct {
	EnvoyKey string `json:"envoy_key"`
	PoolName string `json:"pool_name"`
	StopTS   *int64 `json:"stop_ts"`
	Version  int    `json:"version"`
}

func TestRunnerRegistersWithEngine(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	runnerName := fmt.Sprintf("rivet-go-conformance-%d", time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	registry := rivet.NewRegistry()
	go func() {
		serveResult <- registry.Serve(ctx, rivet.Config{
			Endpoint:   engine.endpoint,
			Namespace:  "default",
			RunnerName: runnerName,
			Version:    1,
			TotalSlots: 4,
			LogLevel:   "info",
		})
	}()

	var registered envoyRecord
	eventually(t, 20*time.Second, func() (bool, error) {
		select {
		case err := <-serveResult:
			if err == nil {
				return false, errors.New("runner Serve exited before registration without an error")
			}
			return false, fmt.Errorf("runner Serve exited before registration: %w", err)
		default:
		}
		envoys, err := listEnvoys(engine.endpoint, runnerName)
		if err != nil {
			return false, err
		}
		for _, envoy := range envoys {
			if envoy.PoolName == runnerName && envoy.StopTS == nil {
				registered = envoy
				return true, nil
			}
		}
		return false, nil
	})
	if registered.EnvoyKey == "" {
		t.Fatal("engine management API returned a registration with an empty envoy_key")
	}
	if registered.Version != 1 {
		t.Fatalf("engine-visible runner version = %d, want 1", registered.Version)
	}

	cancel()
	select {
	case err := <-serveResult:
		if err != nil {
			t.Fatalf("runner Serve shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runner did not finish graceful shutdown")
	}

	// /envoys is an active-only list at this pin, so graceful shutdown is
	// observable as removal from the management API.
	eventually(t, 10*time.Second, func() (bool, error) {
		envoys, err := listEnvoys(engine.endpoint, runnerName)
		if err != nil {
			return false, err
		}
		return len(envoys) == 0, nil
	})
}

func TestRunnerNewFailuresAreStructuredAndBounded(t *testing.T) {
	tests := []struct {
		name     string
		endpoint func(*testing.T) string
	}{
		{name: "dead endpoint", endpoint: silentEndpoint},
		{name: "wrong port", endpoint: closedEndpoint},
		{name: "non-engine HTTP server", endpoint: nonEngineEndpoint},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			started := time.Now()
			payload := expectRunnerNewError(t, test.endpoint(t))
			if payload.Code == "" {
				t.Fatal("runner_new returned an error without a structured code")
			}
			if elapsed := time.Since(started); elapsed > startupBound {
				t.Fatalf("runner_new took %s, want at most %s", elapsed, startupBound)
			}
		})
	}
}

func TestNativeBoundaryConcurrencyAndLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)

	runner := startNativeRunner(t, engine.endpoint, "rivet-go-boundary")
	connectedBatch, connected := waitForNativeEvent(t, runner, wire.EventRunnerConnected, 5*time.Second)
	if connected.RunnerID == "" {
		t.Fatal("RunnerConnected has an empty runner_id")
	}

	firstPoll := make(chan struct {
		batch wire.EventBatch
		err   error
	}, 1)
	go func() {
		data, pollErr := runner.Poll(2 * time.Second)
		if pollErr != nil {
			firstPoll <- struct {
				batch wire.EventBatch
				err   error
			}{err: pollErr}
			return
		}
		batch, decodeErr := wire.DecodeEventBatch(data)
		firstPoll <- struct {
			batch wire.EventBatch
			err   error
		}{batch: batch, err: decodeErr}
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := runner.Poll(10 * time.Millisecond); nativeErrorCode(err) != "poll_in_progress" {
		t.Fatalf("second concurrent Poll error = %v, want poll_in_progress", err)
	}
	first := <-firstPoll
	if first.err != nil {
		t.Fatalf("first concurrent Poll: %v", first.err)
	}
	if first.batch.Seq <= connectedBatch.Seq {
		t.Fatalf("poll sequence = %d after %d, want strictly increasing", first.batch.Seq, connectedBatch.Seq)
	}

	emptyBatch, err := wire.EncodeCommandBatch(wire.CommandBatch{Commands: []wire.Command{}})
	if err != nil {
		t.Fatalf("encode empty command batch: %v", err)
	}
	const (
		submitters = 16
		submits    = 32
	)
	var submitWG sync.WaitGroup
	submitErrors := make(chan error, submitters)
	submitWG.Add(submitters)
	for range submitters {
		go func() {
			defer submitWG.Done()
			for range submits {
				if err := runner.Submit(emptyBatch); err != nil {
					submitErrors <- err
					return
				}
			}
		}()
	}
	submitWG.Wait()
	close(submitErrors)
	for err := range submitErrors {
		t.Fatalf("concurrent native Submit: %v", err)
	}

	if err := runner.Shutdown(3 * time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	stoppedBatch, stopped := waitForNativeEvent(t, runner, wire.EventRunnerStopped, 5*time.Second)
	if stoppedBatch.Seq <= first.batch.Seq {
		t.Fatalf("shutdown sequence = %d after %d, want strictly increasing", stoppedBatch.Seq, first.batch.Seq)
	}
	if stopped.DrainReport == nil || !stopped.DrainReport.Graceful {
		t.Fatalf("RunnerStopped drain report = %#v, want graceful", stopped.DrainReport)
	}
	runner.Close()
	runner.Close() // The owning Go handle must make a duplicate free harmless.

	forced := startNativeRunner(t, engine.endpoint, "rivet-go-forced-free")
	waitForNativeEvent(t, forced, wire.EventRunnerConnected, 5*time.Second)
	started := time.Now()
	forced.Close() // Free without a preceding shutdown or poll drain.
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("free without shutdown took %s", elapsed)
	}
	forced.Close()
}

func TestRunnerReportsEngineDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("real-engine conformance is disabled by -short")
	}
	engineBinary, err := acquireEngine(context.Background())
	if err != nil {
		t.Fatalf("obtain Rivet engine %s: %v\n%s", engineTag, err, engineRemediation())
	}
	engine := startEngine(t, engineBinary)
	runner := startNativeRunner(t, engine.endpoint, "rivet-go-disconnect")
	defer runner.Close()
	waitForNativeEvent(t, runner, wire.EventRunnerConnected, 5*time.Second)

	started := time.Now()
	if err := engine.command.Process.Kill(); err != nil {
		t.Fatalf("kill engine: %v", err)
	}
	_, disconnected := waitForNativeEvent(t, runner, wire.EventRunnerDisconnected, disconnectLivenessWindow)
	if disconnected.Reason == "" {
		t.Fatal("RunnerDisconnected has an empty reason")
	}
	if elapsed := time.Since(started); elapsed > disconnectLivenessWindow {
		t.Fatalf("RunnerDisconnected arrived after %s, liveness window is %s", elapsed, disconnectLivenessWindow)
	}
	if err := runner.Shutdown(2 * time.Second); err != nil {
		t.Fatalf("Shutdown after disconnect: %v", err)
	}
	waitForNativeEvent(t, runner, wire.EventRunnerStopped, 5*time.Second)
}

func acquireEngine(ctx context.Context) (string, error) {
	if override := os.Getenv("RIVET_GO_ENGINE_BIN"); override != "" {
		return verifyEngineBinary(ctx, override)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home directory: %w", err)
	}
	cache := filepath.Join(home, ".cache", "rivet-go", "engine-"+engineTag)
	binary := filepath.Join(cache, executableName("rivet-engine"))
	if path, err := verifyEngineBinary(ctx, binary); err == nil {
		return path, nil
	}
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return "", fmt.Errorf("create engine cache: %w", err)
	}

	return buildPinnedEngine(ctx, cache)
}

func buildPinnedEngine(ctx context.Context, cache string) (string, error) {
	source := filepath.Join(cache, "source")
	if _, err := os.Stat(filepath.Join(source, ".git")); errors.Is(err, os.ErrNotExist) {
		command := exec.CommandContext(ctx, "git", "clone", "--depth", "1", "--branch", engineTag, engineRepository, source)
		if output, err := command.CombinedOutput(); err != nil {
			return "", fmt.Errorf("clone pinned engine source: %w: %s", err, tail(output, 20))
		}
	} else if err != nil {
		return "", fmt.Errorf("inspect cached engine source: %w", err)
	}
	commit, err := exec.CommandContext(ctx, "git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("verify cached engine source: %w", err)
	}
	if got := strings.TrimSpace(string(commit)); got != engineCommit {
		return "", fmt.Errorf("cached source is %s, want pinned commit %s; remove %s and retry", got, engineCommit, source)
	}

	target := filepath.Join(cache, "target")
	command := exec.CommandContext(ctx, "cargo", "build", "--manifest-path", filepath.Join(source, "Cargo.toml"), "-p", "rivet-engine", "--release", "--target-dir", target)
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
	return verifyEngineBinary(ctx, destination)
}

func startEngine(t *testing.T, binary string) *runningEngine {
	t.Helper()
	guardPort := reservePortRange(t)
	storage := t.TempDir()
	logPath := filepath.Join(storage, "engine.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create engine log: %v", err)
	}
	command := exec.Command(binary, "start")
	command.Env = append(os.Environ(),
		"RIVET__GUARD__HOST=127.0.0.1",
		"RIVET__GUARD__PORT="+strconv.Itoa(guardPort),
		"RIVET__API_PEER__HOST=127.0.0.1",
		"RIVET__API_PEER__PORT="+strconv.Itoa(guardPort+1),
		"RIVET__METRICS__HOST=127.0.0.1",
		"RIVET__METRICS__PORT="+strconv.Itoa(guardPort+10),
		"RIVET__FILE_SYSTEM__PATH="+filepath.Join(storage, "db"),
	)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start engine %s: %v", binary, err)
	}
	engine := &runningEngine{
		endpoint: fmt.Sprintf("http://127.0.0.1:%d", guardPort),
		command:  command,
		logPath:  logPath,
	}
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		_ = command.Wait()
		_ = logFile.Close()
	})

	eventually(t, 20*time.Second, func() (bool, error) {
		response, err := http.Get(engine.endpoint + "/health")
		if err != nil {
			return false, fmt.Errorf("engine health request: %w\nlast engine output:\n%s", err, readLogTail(logPath))
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return false, fmt.Errorf("engine health returned %s\nlast engine output:\n%s", response.Status, readLogTail(logPath))
		}
		return true, nil
	})
	return engine
}

func listEnvoys(endpoint, runnerName string) ([]envoyRecord, error) {
	request, err := http.NewRequest(http.MethodGet, endpoint+"/envoys?namespace=default&name="+runnerName, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer dev")
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return nil, fmt.Errorf("GET /envoys returned %s: %s", response.Status, body)
	}
	var list envoyListResponse
	if err := json.NewDecoder(response.Body).Decode(&list); err != nil {
		return nil, err
	}
	return list.Envoys, nil
}

func eventually(t *testing.T, timeout time.Duration, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if ok {
			return
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("condition did not become true within %s: %v", timeout, lastErr)
	}
	t.Fatalf("condition did not become true within %s", timeout)
}

func reservePortRange(t *testing.T) int {
	t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve engine port: %v", err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
		if port > 1024 && port < 65525 && portsAvailable(port, port+1, port+10) {
			return port
		}
	}
	t.Fatal("could not find three available engine ports")
	return 0
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

func readLogTail(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return tail(data, 60)
}

func tail(data []byte, lines int) string {
	parts := strings.Split(string(data), "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n")
}

func engineRemediation() string {
	return "Set RIVET_GO_ENGINE_BIN to a v2.3.10 rivet-engine binary, or install git + Rust and retry. " +
		"The automatic fallback clones tag v2.3.10 and runs `cargo build -p rivet-engine --release`; see conformance/README.md."
}

// silentEndpoint returns a URL whose listener accepts connections but never
// responds, forcing rk_runner_new to hit its startup deadline.
func silentEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
	})
	return "http://" + listener.Addr().String()
}

// closedEndpoint returns a URL for a port that was just released, so
// connections are refused.
func closedEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	endpoint := "http://" + listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return endpoint
}

// nonEngineEndpoint returns a live HTTP server that is not a Rivet engine.
func nonEngineEndpoint(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not a rivet engine", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func expectRunnerNewError(t *testing.T, endpoint string) ffi.ErrorPayload {
	t.Helper()
	config, err := wire.EncodeRunnerConfig(wire.RunnerConfig{
		EngineEndpoint: endpoint,
		Namespace:      "default",
		RunnerName:     "rivet-go-error-probe",
		Version:        1,
		TotalSlots:     1,
		ActorNames:     []string{},
		LogLevel:       "error",
	})
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	result, err := ffi.NewRunner(config)
	if err != nil {
		t.Fatalf("invoke rk_runner_new: %v", err)
	}
	if result.Runner != nil {
		result.Runner.Close()
		t.Fatalf("rk_runner_new against %s returned a runner, want error", endpoint)
	}
	if result.Error == nil {
		t.Fatal("rk_runner_new returned neither runner nor error")
	}
	defer result.Error.Close()
	payload, err := result.Error.Payload()
	if err != nil {
		t.Fatalf("decode rk_runner_new error: %v", err)
	}
	return payload
}

func startNativeRunner(t *testing.T, endpoint, name string) *ffi.Runner {
	t.Helper()
	config, err := wire.EncodeRunnerConfig(wire.RunnerConfig{
		EngineEndpoint: endpoint,
		Namespace:      "default",
		RunnerName:     name,
		Version:        1,
		TotalSlots:     1,
		ActorNames:     []string{},
		LogLevel:       "error",
	})
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	result, err := ffi.NewRunner(config)
	if err != nil {
		t.Fatalf("invoke rk_runner_new: %v", err)
	}
	if result.Error != nil {
		defer result.Error.Close()
		payload, decodeErr := result.Error.Payload()
		if decodeErr != nil {
			t.Fatalf("start native runner: decode error: %v", decodeErr)
		}
		t.Fatalf("start native runner: %v", payload)
	}
	if result.Runner == nil {
		t.Fatal("rk_runner_new returned neither runner nor error")
	}
	t.Cleanup(result.Runner.Close)
	return result.Runner
}

// waitForNativeEvent polls until an event of the wanted kind arrives and
// returns it with its enclosing batch.
func waitForNativeEvent(t *testing.T, runner *ffi.Runner, kind wire.EventKind, timeout time.Duration) (wire.EventBatch, wire.Event) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("no %s event within %s", kind, timeout)
		}
		pollTimeout := remaining
		if pollTimeout > 500*time.Millisecond {
			pollTimeout = 500 * time.Millisecond
		}
		data, err := runner.Poll(pollTimeout)
		if err != nil {
			t.Fatalf("poll while waiting for %s: %v", kind, err)
		}
		if len(data) == 0 {
			continue
		}
		batch, err := wire.DecodeEventBatch(data)
		if err != nil {
			t.Fatalf("decode poll batch: %v", err)
		}
		for _, event := range batch.Events {
			if event.Kind == kind {
				return batch, event
			}
		}
	}
}

func nativeErrorCode(err error) string {
	var payload ffi.ErrorPayload
	if errors.As(err, &payload) {
		return payload.Code
	}
	return ""
}

// verifyEngineBinary checks that path is executable and reports exactly the
// pinned engine version and commit.
func verifyEngineBinary(ctx context.Context, path string) (string, error) {
	path, err := verifyExecutable(path)
	if err != nil {
		return "", err
	}
	output, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s --version: %w: %s", path, err, tail(output, 5))
	}
	text := string(output)
	if !strings.Contains(text, engineVersion) {
		return "", fmt.Errorf("%s reports %q, want version %s", path, strings.TrimSpace(text), engineVersion)
	}
	if !strings.Contains(text, engineCommit) {
		return "", fmt.Errorf("%s reports %q, want commit %s", path, strings.TrimSpace(text), engineCommit)
	}
	return path, nil
}
