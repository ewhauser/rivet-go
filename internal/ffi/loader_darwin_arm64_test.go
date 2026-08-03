//go:build darwin && arm64

package ffi

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
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

func TestRunnerNewReturnsStructuredNotImplemented(t *testing.T) {
	config, err := msgpack.Marshal(map[string]any{
		"endpoint":    "http://127.0.0.1:6420",
		"namespace":   "default",
		"runner_name": "m0-test",
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
	if payload.Code != "not_implemented" {
		t.Fatalf("error code = %q, want not_implemented", payload.Code)
	}
	if !strings.Contains(payload.Message, "rk_runner_new") {
		t.Fatalf("error message %q does not name rk_runner_new", payload.Message)
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
