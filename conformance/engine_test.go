package conformance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"testing"
	"time"

	"github.com/ewhauser/rivet-go/rivet"
)

const (
	engineTag        = "v2.3.10"
	engineVersion    = "2.3.10"
	engineCommit     = "957d4e482f404913ca1955d8ecc357533f6fd081"
	engineRepository = "https://github.com/rivet-dev/rivet.git"
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

func acquireEngine(ctx context.Context) (string, error) {
	if override := os.Getenv("RIVET_GO_ENGINE_BIN"); override != "" {
		return verifyExecutable(override)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home directory: %w", err)
	}
	cache := filepath.Join(home, ".cache", "rivet-go", "engine-"+engineTag)
	binary := filepath.Join(cache, executableName("rivet-engine"))
	if path, err := verifyExecutable(binary); err == nil {
		return path, nil
	}
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return "", fmt.Errorf("create engine cache: %w", err)
	}

	if downloaded, err := downloadPinnedEngine(ctx, cache); err == nil {
		return downloaded, nil
	}
	return buildPinnedEngine(ctx, cache)
}

func downloadPinnedEngine(ctx context.Context, cache string) (string, error) {
	artifact, err := engineArtifactName()
	if err != nil {
		return "", err
	}
	base := "https://releases.rivet.dev/rivet/" + engineVersion + "/engine/"
	client := &http.Client{Timeout: 60 * time.Second}
	manifest, err := fetch(ctx, client, base+"SHA256SUMS")
	if err != nil {
		return "", err
	}
	expected := checksumFor(manifest, artifact)
	if expected == "" {
		return "", fmt.Errorf("prebuilt manifest has no %s", artifact)
	}
	data, err := fetch(ctx, client, base+artifact)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	actual := hex.EncodeToString(digest[:])
	if !strings.EqualFold(actual, expected) {
		return "", fmt.Errorf("prebuilt checksum mismatch: got %s, want %s", actual, expected)
	}
	temporary := filepath.Join(cache, artifact+".tmp")
	if err := os.WriteFile(temporary, data, 0o755); err != nil {
		return "", fmt.Errorf("write downloaded engine: %w", err)
	}
	destination := filepath.Join(cache, executableName("rivet-engine"))
	if err := os.Rename(temporary, destination); err != nil {
		return "", fmt.Errorf("install downloaded engine: %w", err)
	}
	return verifyExecutable(destination)
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
	return verifyExecutable(destination)
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

func fetch(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %s", url, response.Status)
	}
	return io.ReadAll(response.Body)
}

func checksumFor(manifest []byte, artifact string) string {
	scanner := bufio.NewScanner(bytes.NewReader(manifest))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && strings.TrimPrefix(fields[1], "*") == artifact {
			return fields[0]
		}
	}
	return ""
}

func engineArtifactName() (string, error) {
	arch := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[runtime.GOARCH]
	if arch == "" {
		return "", fmt.Errorf("no engine prebuilt naming rule for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	var target string
	switch runtime.GOOS {
	case "darwin":
		target = arch + "-apple-darwin"
	case "linux":
		target = arch + "-unknown-linux-musl"
	default:
		return "", fmt.Errorf("no engine prebuilt naming rule for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	return "rivet-engine-" + target, nil
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
