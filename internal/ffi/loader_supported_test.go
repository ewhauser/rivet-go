//go:build !rivetgo_ffi_stub && ((darwin && arm64) || (linux && (amd64 || arm64)) || (windows && amd64))

package ffi

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestLoadAndABIVersion(t *testing.T) {
	if err := Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	got, err := ABIVersion()
	if err != nil {
		t.Fatalf("ABIVersion: %v", err)
	}
	if got != ExpectedABIVersion {
		t.Fatalf("ABIVersion = %d, want %d", got, ExpectedABIVersion)
	}
}

func TestLoaderRejectsOlderABILibraries(t *testing.T) {
	for _, version := range []uint32{1, 5, 6, 7, 8} {
		t.Run(fmt.Sprintf("ABI%d", version), func(t *testing.T) {
			filename := fmt.Sprintf("librivetkit_go_ffi_abi%d.so", version)
			switch runtime.GOOS {
			case "darwin":
				filename = fmt.Sprintf("librivetkit_go_ffi_abi%d.dylib", version)
			case "windows":
				filename = fmt.Sprintf("rivetkit_go_ffi_abi%d.dll", version)
			}
			libraryPath := filepath.Join(t.TempDir(), filename)
			arguments := []string{
				"--crate-type=cdylib",
				filepath.Join("testdata", "abi1_fixture.rs"),
				"-o",
				libraryPath,
			}
			if version == 5 || version == 6 || version == 7 || version == 8 {
				arguments = append([]string{"--cfg", fmt.Sprintf("rk_abi_%d", version)}, arguments...)
			}
			command := exec.Command("rustc", arguments...)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("build ABI-%d fixture: %v: %s", version, err, output)
			}
			handle, err := openLibrary(libraryPath)
			if err != nil {
				t.Fatalf("open ABI-%d fixture: %v", version, err)
			}
			defer func() {
				if err := closeLibrary(handle); err != nil {
					t.Errorf("close ABI-%d fixture: %v", version, err)
				}
			}()
			var candidate nativeAPI
			err = candidate.bindAndValidate(handle)
			want := fmt.Sprintf("library reports %d", version)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("ABI-%d validation error = %v", version, err)
			}
		})
	}
}

func TestLoadedArtifactMatchesPinnedChecksum(t *testing.T) {
	if os.Getenv(envLibraryOverride) != "" {
		t.Skipf("%s overrides the pinned-acquisition path", envLibraryOverride)
	}
	if err := Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	manifest, err := fs.ReadFile(embeddedFiles, "checksums.txt")
	if err != nil {
		t.Fatalf("read embedded checksums: %v", err)
	}
	expected := make(map[string]string, len(nativeArtifacts))
	for _, artifact := range nativeArtifacts {
		digest, err := checksumFor(manifest, artifact.manifestPath())
		if err != nil {
			t.Fatalf("manifest checksum for %s: %v", artifact.manifestPath(), err)
		}
		expected[digest] = artifact.manifestPath()
	}
	loaded, err := os.ReadFile(api.path)
	if err != nil {
		t.Fatalf("read loaded library %s: %v", api.path, err)
	}
	loadedDigest := digestHex(loaded)
	if _, ok := expected[loadedDigest]; !ok {
		t.Fatalf("loaded library digest %s matches no pinned platform artifact %v", loadedDigest, expected)
	}
	if got := filepath.Base(filepath.Dir(api.path)); got != loadedDigest {
		t.Fatalf("loaded library cache directory = %s, want sha256 %s", got, loadedDigest)
	}
	if runtime.GOOS != "windows" {
		assertPrivateMode(t, filepath.Dir(api.path), 0o077)
		assertPrivateMode(t, api.path, 0o077)
	}
}

func TestRunnerNewRejectsInvalidConfig(t *testing.T) {
	config, err := msgpack.Marshal(map[string]any{
		"engine_endpoint": "http://127.0.0.1:6420",
		"namespace":       "default",
		"runner_name":     "m0-test",
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	result, err := NewRunner(config)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if result.Runner != nil {
		result.Runner.Close()
		t.Fatal("NewRunner unexpectedly returned a runner")
	}
	if result.Error == nil {
		t.Fatal("NewRunner returned no structured error")
	}
	defer result.Error.Close()
	payload, err := result.Error.Payload()
	if err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	if payload.Code != "invalid_config" {
		t.Fatalf("error code = %q, want invalid_config", payload.Code)
	}
	if !strings.Contains(payload.Message, "RunnerConfig") {
		t.Fatalf("error message %q does not identify RunnerConfig", payload.Message)
	}
}

func TestPanicFirewallThroughLoadedLibrary(t *testing.T) {
	if err := Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	panicProbe, err := registerPanicProbe(api.handle)
	if err != nil {
		t.Fatalf("register rk_test_panic: %v", err)
	}
	result := panicProbe()
	if result.err == nil {
		t.Fatal("rk_test_panic returned no structured error")
	}
	nativeError := newError(result.err)
	defer nativeError.Close()
	payload, err := nativeError.Payload()
	if err != nil {
		t.Fatalf("decode panic error: %v", err)
	}
	if payload.Code != "internal_panic" {
		t.Fatalf("panic error code = %q, want internal_panic", payload.Code)
	}
	if payload.Message != "panic firewall probe" {
		t.Fatalf("panic error message = %q, want panic firewall probe", payload.Message)
	}

	got, err := ABIVersion()
	if err != nil {
		t.Fatalf("ABIVersion after panic: %v", err)
	}
	if got != ExpectedABIVersion {
		t.Fatalf("ABIVersion after panic = %d, want %d", got, ExpectedABIVersion)
	}
}

func TestAllBindingsRejectNullRunner(t *testing.T) {
	if err := Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	api.bytesFree(cBytes{})
	api.stringFree(cBytes{})
	api.runnerFree(nil)

	tests := []struct {
		name string
		err  *cError
	}{
		{name: "rk_runner_poll", err: api.runnerPoll(nil, 0).err},
		{name: "rk_runner_submit", err: api.runnerSubmit(nil, nil, 0).err},
		{name: "rk_runner_shutdown", err: api.runnerShutdown(nil, 0).err},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.err == nil {
				t.Fatal("binding returned no structured error")
			}
			nativeError := newError(test.err)
			defer nativeError.Close()
			payload, err := nativeError.Payload()
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if payload.Code != "invalid_runner" {
				t.Fatalf("error code = %q, want invalid_runner", payload.Code)
			}
			if !strings.Contains(payload.Message, "null") {
				t.Fatalf("error message %q does not identify the null handle for %s", payload.Message, test.name)
			}
		})
	}
}

type countingArtifactServer struct {
	server   *httptest.Server
	requests atomic.Int64
}

// newArtifactServer serves body for every request, standing in for the
// pinned release asset host.
func newArtifactServer(t *testing.T, status int, body []byte) *countingArtifactServer {
	t.Helper()
	counting := &countingArtifactServer{}
	counting.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		counting.requests.Add(1)
		writer.WriteHeader(status)
		_, _ = writer.Write(body)
	}))
	t.Cleanup(counting.server.Close)
	return counting
}

func testArtifactManifest(artifact nativeArtifact, body []byte) []byte {
	return fmt.Appendf(nil, "%x  %s\n", sha256.Sum256(body), artifact.manifestPath())
}

func TestAcquireDownloadsVerifiesAndCaches(t *testing.T) {
	artifact := nativeArtifact{dir: "lib/test_platform", filename: "library.dylib"}
	library := []byte("authentic native library")
	host := newArtifactServer(t, http.StatusOK, library)
	cacheRoot := t.TempDir()

	first, err := acquireVerifiedLibrary(artifact, testArtifactManifest(artifact, library), cacheRoot, host.server.URL)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	got, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read cached library: %v", err)
	}
	if string(got) != string(library) {
		t.Fatalf("cached library = %q, want %q", got, library)
	}
	if dir := filepath.Base(filepath.Dir(first)); dir != digestHex(library) {
		t.Fatalf("cache directory = %s, want sha256 %s", dir, digestHex(library))
	}
	if runtime.GOOS != "windows" {
		assertPrivateMode(t, filepath.Dir(first), 0o077)
		assertPrivateMode(t, first, 0o077)
	}

	second, err := acquireVerifiedLibrary(artifact, testArtifactManifest(artifact, library), cacheRoot, host.server.URL)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if second != first {
		t.Fatalf("second acquire path = %s, want cached %s", second, first)
	}
	if requests := host.requests.Load(); requests != 1 {
		t.Fatalf("host requests = %d, want 1 (second acquire must hit the cache)", requests)
	}
}

func TestAcquireRejectsChecksumMismatch(t *testing.T) {
	artifact := nativeArtifact{dir: "lib/test_platform", filename: "library.dylib"}
	library := []byte("authentic native library")
	host := newArtifactServer(t, http.StatusOK, []byte("tampered artifact"))
	cacheRoot := t.TempDir()

	_, err := acquireVerifiedLibrary(artifact, testArtifactManifest(artifact, library), cacheRoot, host.server.URL)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("acquire error = %v, want checksum mismatch", err)
	}
	cached := filepath.Join(cacheRoot, "rivet-go", digestHex(library), artifact.filename)
	if _, statErr := os.Stat(cached); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("rejected artifact must not be cached: %v", statErr)
	}
}

func TestAcquireReplacesTamperedCacheEntry(t *testing.T) {
	artifact := nativeArtifact{dir: "lib/test_platform", filename: "library.dylib"}
	library := []byte("authentic native library")
	host := newArtifactServer(t, http.StatusOK, library)
	cacheRoot := t.TempDir()

	cached, err := acquireVerifiedLibrary(artifact, testArtifactManifest(artifact, library), cacheRoot, host.server.URL)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := os.Chmod(cached, 0o700); err != nil {
		t.Fatalf("make cached library writable: %v", err)
	}
	if err := os.WriteFile(cached, []byte("tampered"), 0o700); err != nil {
		t.Fatalf("tamper cached library: %v", err)
	}

	replaced, err := acquireVerifiedLibrary(artifact, testArtifactManifest(artifact, library), cacheRoot, host.server.URL)
	if err != nil {
		t.Fatalf("replace tampered cache entry: %v", err)
	}
	got, err := os.ReadFile(replaced)
	if err != nil {
		t.Fatalf("read replaced library: %v", err)
	}
	if string(got) != string(library) {
		t.Fatalf("replaced library = %q, want %q", got, library)
	}
	if requests := host.requests.Load(); requests != 2 {
		t.Fatalf("host requests = %d, want 2 (tampered entry must be re-downloaded)", requests)
	}
}

func TestAcquireSecuresCachePermissions(t *testing.T) {
	artifact := nativeArtifact{dir: "lib/test_platform", filename: "library.dylib"}
	library := []byte("native library")
	host := newArtifactServer(t, http.StatusOK, library)
	cacheRoot := t.TempDir()
	cacheBase := filepath.Join(cacheRoot, "rivet-go")
	if err := os.Mkdir(cacheBase, 0o777); err != nil {
		t.Fatalf("Mkdir cache base: %v", err)
	}
	if err := os.Chmod(cacheBase, 0o777); err != nil {
		t.Fatalf("make cache base unsafe: %v", err)
	}

	acquired, err := acquireVerifiedLibrary(artifact, testArtifactManifest(artifact, library), cacheRoot, host.server.URL)
	if err != nil {
		t.Fatalf("acquire verified library: %v", err)
	}
	if runtime.GOOS != "windows" {
		assertPrivateMode(t, cacheBase, 0o077)
		assertPrivateMode(t, filepath.Dir(acquired), 0o077)
		assertPrivateMode(t, acquired, 0o077)
	}
}

func TestAcquireReportsDownloadFailureWithPreSeedHint(t *testing.T) {
	artifact := nativeArtifact{dir: "lib/test_platform", filename: "library.dylib"}
	library := []byte("unavailable library")
	host := newArtifactServer(t, http.StatusNotFound, nil)
	cacheRoot := t.TempDir()

	_, err := acquireVerifiedLibrary(artifact, testArtifactManifest(artifact, library), cacheRoot, host.server.URL)
	if err == nil {
		t.Fatal("expected download failure")
	}
	for _, want := range []string{"404", artifact.assetName(), "pre-seed", envLibraryOverride} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("download error %q does not mention %q", err, want)
		}
	}
}

func assertPrivateMode(t *testing.T, filename string, forbidden fs.FileMode) {
	t.Helper()
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Stat %s: %v", filename, err)
	}
	if got := info.Mode().Perm() & forbidden; got != 0 {
		t.Fatalf("%s mode %04o has group/world permissions %04o", filename, info.Mode().Perm(), got)
	}
}
