//go:build !rivetgo_ffi_stub && ((darwin && arm64) || (linux && (amd64 || arm64)) || (windows && amd64))

package ffi

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
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
	for _, version := range []uint32{1, 5, 6, 7} {
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
			if version == 5 || version == 6 || version == 7 {
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

func TestEmbeddedDiskManifestAndLoadedArtifactAgree(t *testing.T) {
	if err := Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	manifest, err := fs.ReadFile(embeddedFiles, "checksums.txt")
	if err != nil {
		t.Fatalf("read embedded checksums: %v", err)
	}
	for _, library := range embeddedLibraries {
		libraryPath := path.Join(library.dir, library.filename)
		embedded, err := fs.ReadFile(embeddedFiles, libraryPath)
		if err != nil {
			t.Fatalf("read embedded %s: %v", libraryPath, err)
		}
		onDisk, err := os.ReadFile(libraryPath)
		if err != nil {
			t.Fatalf("read on-disk %s: %v", libraryPath, err)
		}
		expected, err := checksumFor(manifest, libraryPath)
		if err != nil {
			t.Fatalf("manifest checksum for %s: %v", libraryPath, err)
		}
		if got := digestHex(embedded); got != expected {
			t.Fatalf("embedded checksum for %s = %s, want %s", libraryPath, got, expected)
		}
		if got := digestHex(onDisk); got != expected {
			t.Fatalf("on-disk checksum for %s = %s, want %s", libraryPath, got, expected)
		}
	}

	loaded, err := os.ReadFile(api.path)
	if err != nil {
		t.Fatalf("read loaded library %s: %v", api.path, err)
	}
	loadedDigest := digestHex(loaded)
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

func TestChecksumMismatchIsARegularError(t *testing.T) {
	root := t.TempDir()
	libraryDir := filepath.Join(root, "lib", "test")
	if err := os.MkdirAll(libraryDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	original := []byte("original native library")
	digest := sha256.Sum256(original)
	manifest := fmt.Sprintf("%x  lib/test/library.dylib\n", digest)
	if err := os.WriteFile(filepath.Join(root, "checksums.txt"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("write checksums: %v", err)
	}
	if err := os.WriteFile(filepath.Join(libraryDir, "library.dylib"), []byte("tampered"), 0o755); err != nil {
		t.Fatalf("write tampered library: %v", err)
	}

	_, err := extractVerifiedLibrary(
		os.DirFS(root),
		"lib/test",
		"library.dylib",
		"checksums.txt",
		t.TempDir(),
	)
	if err == nil {
		t.Fatal("expected checksum mismatch")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("unexpected checksum error: %v", err)
	}
}

func TestExtractionSecuresCachePermissions(t *testing.T) {
	root := t.TempDir()
	libraryDir := filepath.Join(root, "source")
	if err := os.Mkdir(libraryDir, 0o755); err != nil {
		t.Fatalf("Mkdir source: %v", err)
	}
	library := []byte("native library")
	digest := sha256.Sum256(library)
	manifest := fmt.Sprintf("%x  source/library.dylib\n", digest)
	if err := os.WriteFile(filepath.Join(root, "checksums.txt"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("write checksums: %v", err)
	}
	if err := os.WriteFile(filepath.Join(libraryDir, "library.dylib"), library, 0o755); err != nil {
		t.Fatalf("write library: %v", err)
	}

	cacheRoot := t.TempDir()
	cacheBase := filepath.Join(cacheRoot, "rivet-go")
	if err := os.Mkdir(cacheBase, 0o777); err != nil {
		t.Fatalf("Mkdir cache base: %v", err)
	}
	if err := os.Chmod(cacheBase, 0o777); err != nil {
		t.Fatalf("make cache base unsafe: %v", err)
	}
	extracted, err := extractVerifiedLibrary(
		os.DirFS(root),
		"source",
		"library.dylib",
		"checksums.txt",
		cacheRoot,
	)
	if err != nil {
		t.Fatalf("extract verified library: %v", err)
	}
	if runtime.GOOS != "windows" {
		assertPrivateMode(t, cacheBase, 0o077)
		assertPrivateMode(t, filepath.Dir(extracted), 0o077)
		assertPrivateMode(t, extracted, 0o077)
	}
}

func TestExtractionReplacesTamperedCacheEntry(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "source"), 0o755); err != nil {
		t.Fatalf("Mkdir source: %v", err)
	}
	library := []byte("authentic native library")
	digest := sha256.Sum256(library)
	manifest := fmt.Sprintf("%x  source/library.dylib\n", digest)
	if err := os.WriteFile(filepath.Join(root, "checksums.txt"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("write checksums: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "source", "library.dylib"), library, 0o755); err != nil {
		t.Fatalf("write library: %v", err)
	}

	cacheRoot := t.TempDir()
	extracted, err := extractVerifiedLibrary(
		os.DirFS(root), "source", "library.dylib", "checksums.txt", cacheRoot,
	)
	if err != nil {
		t.Fatalf("first extraction: %v", err)
	}
	if err := os.Chmod(extracted, 0o700); err != nil {
		t.Fatalf("make cached library writable: %v", err)
	}
	if err := os.WriteFile(extracted, []byte("tampered"), 0o700); err != nil {
		t.Fatalf("tamper cached library: %v", err)
	}

	replaced, err := extractVerifiedLibrary(
		os.DirFS(root), "source", "library.dylib", "checksums.txt", cacheRoot,
	)
	if err != nil {
		t.Fatalf("replace tampered extraction: %v", err)
	}
	got, err := os.ReadFile(replaced)
	if err != nil {
		t.Fatalf("read replaced library: %v", err)
	}
	if string(got) != string(library) {
		t.Fatalf("replaced library = %q, want %q", got, library)
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
