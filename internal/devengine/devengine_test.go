package devengine

import (
	"context"
	"path/filepath"
	"testing"
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
