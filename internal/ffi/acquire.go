//go:build !rivetgo_ffi_stub && ((darwin && arm64) || (linux && (amd64 || arm64)) || (windows && amd64))

package ffi

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Native library acquisition. The repository pins each platform's artifact by
// SHA-256 in checksums.txt; the artifacts themselves ship as release assets
// rather than inside the module. Resolution order:
//
//  1. RIVET_GO_FFI_LIB — an operator-provided library path, used as-is. This
//     is the air-gapped escape hatch, mirroring RIVET_GO_ENGINE_BIN: the
//     operator owns the file and no checksum is enforced.
//  2. The local cache: <user cache dir>/rivet-go/<sha256>/<filename>,
//     verified against the pinned checksum on every load.
//  3. A checksummed download of the pinned release asset, then cached.
//     RIVET_GO_FFI_BASE_URL overrides the asset host for mirrors.
//
// scripts/build-ffi.sh seeds the cache directly after a local build, so
// development never downloads. Operators can pre-seed offline machines with
// cmd/rivet-go-fetch or by downloading the release asset and setting
// RIVET_GO_FFI_LIB.
const (
	// artifactReleaseTag names the release whose assets match checksums.txt.
	// The release workflow refuses to publish unless the pushed tag equals
	// this constant and every rebuilt artifact hashes to its checksums.txt
	// entry, so a loader at this commit can trust tag + checksum together.
	artifactReleaseTag = "v0.2.0"

	defaultArtifactBaseURL = "https://github.com/ewhauser/rivet-go/releases/download"
	envLibraryOverride     = "RIVET_GO_FFI_LIB"
	envArtifactBaseURL     = "RIVET_GO_FFI_BASE_URL"

	// maxArtifactBytes bounds a single artifact download; the largest shipped
	// library is ~15 MiB.
	maxArtifactBytes = 64 << 20
)

// Capture the standard transport during package initialization so artifact
// downloads do not depend on, or close idle connections from, an application's
// later replacement of http.DefaultTransport.
var artifactTransportTemplate = http.DefaultTransport.(*http.Transport).Clone()

type nativeArtifact struct {
	dir      string
	filename string
}

func (a nativeArtifact) manifestPath() string {
	return path.Join(a.dir, a.filename)
}

// assetName is the flat release-asset name: "<platform>-<filename>".
func (a nativeArtifact) assetName() string {
	return strings.TrimPrefix(a.dir, "lib/") + "-" + a.filename
}

func artifactBaseURL() string {
	if base := os.Getenv(envArtifactBaseURL); base != "" {
		return base
	}
	return defaultArtifactBaseURL
}

func (a nativeArtifact) url(baseURL string) string {
	return strings.TrimRight(baseURL, "/") + "/" + artifactReleaseTag + "/" + a.assetName()
}

// acquireVerifiedLibrary returns a cached, checksum-verified library path for
// the artifact, downloading the pinned release asset on a cache miss.
func acquireVerifiedLibrary(
	artifact nativeArtifact,
	manifest []byte,
	cacheRoot string,
	baseURL string,
) (string, error) {
	expected, err := checksumFor(manifest, artifact.manifestPath())
	if err != nil {
		return "", err
	}
	if cached, ok, err := cachedVerifiedLibrary(cacheRoot, expected, artifact.filename); err != nil {
		return "", err
	} else if ok {
		return cached, nil
	}
	url := artifact.url(baseURL)
	data, err := downloadArtifact(url)
	if err != nil {
		return "", fmt.Errorf(
			"%w (pre-seed the cache with `go run github.com/ewhauser/rivet-go/cmd/rivet-go-fetch`, "+
				"or download the asset and set %s to its path)",
			err, envLibraryOverride,
		)
	}
	if actual := digestHex(data); actual != expected {
		return "", fmt.Errorf(
			"native library checksum mismatch for %s: got %s, want %s",
			url, actual, expected,
		)
	}
	return storeVerifiedLibrary(data, expected, cacheRoot, artifact.filename)
}

// cachedVerifiedLibrary probes the cache for a library whose content still
// hashes to digest. Tampered or truncated entries are removed so the caller
// re-downloads.
func cachedVerifiedLibrary(cacheRoot, digest, filename string) (string, bool, error) {
	targetPath := filepath.Join(cacheRoot, "rivet-go", digest, filename)
	info, err := os.Lstat(targetPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("inspect cached native library %s: %w", targetPath, err)
	}
	if info.Mode().IsRegular() {
		existing, readErr := os.ReadFile(targetPath)
		if readErr == nil && digestHex(existing) == digest {
			if err := os.Chmod(targetPath, 0o500); err != nil {
				return "", false, fmt.Errorf("secure cached native library %s: %w", targetPath, err)
			}
			return targetPath, true, nil
		}
	}
	if err := os.Remove(targetPath); err != nil {
		return "", false, fmt.Errorf("remove invalid cached native library %s: %w", targetPath, err)
	}
	return "", false, nil
}

// storeVerifiedLibrary writes already-verified library bytes into the cache
// layout via a same-directory temporary file and atomic rename.
func storeVerifiedLibrary(libraryBytes []byte, digest, cacheRoot, filename string) (string, error) {
	// A fresh container may not have the user cache directory itself yet
	// (~/.cache, ~/Library/Caches); it is a shared system path, not ours to
	// make private.
	if err := os.MkdirAll(cacheRoot, 0o755); err != nil {
		return "", fmt.Errorf("create user cache directory %s: %w", cacheRoot, err)
	}
	cacheBase := filepath.Join(cacheRoot, "rivet-go")
	if err := ensurePrivateDirectory(cacheBase); err != nil {
		return "", err
	}
	cacheDir := filepath.Join(cacheBase, digest)
	if err := ensurePrivateDirectory(cacheDir); err != nil {
		return "", err
	}
	targetPath := filepath.Join(cacheDir, filename)
	temporary, err := os.CreateTemp(cacheDir, filename+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create temporary native library: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(libraryBytes); err != nil {
		temporary.Close()
		return "", fmt.Errorf("write temporary native library: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close temporary native library: %w", err)
	}
	if err := os.Chmod(temporaryPath, 0o500); err != nil {
		return "", fmt.Errorf("make temporary native library executable: %w", err)
	}
	if err := os.Rename(temporaryPath, targetPath); err != nil {
		if existing, readErr := os.ReadFile(targetPath); readErr == nil && digestHex(existing) == digest {
			return targetPath, nil
		}
		return "", fmt.Errorf("move verified native library into cache: %w", err)
	}
	return targetPath, nil
}

func downloadArtifact(url string) ([]byte, error) {
	transport := artifactTransportTemplate.Clone()
	client := &http.Client{Timeout: 5 * time.Minute, Transport: transport}
	defer transport.CloseIdleConnections()
	response, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("download native library %s: %w", url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download native library %s: %s", url, response.Status)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxArtifactBytes+1))
	if err != nil {
		return nil, fmt.Errorf("download native library %s: %w", url, err)
	}
	if len(data) > maxArtifactBytes {
		return nil, fmt.Errorf("download native library %s: exceeded %d bytes", url, maxArtifactBytes)
	}
	return data, nil
}

// Prefetch downloads and verifies every native library candidate for this
// platform into the local cache without loading any of them, so a machine can
// later run offline. On glibc Linux this includes the musl fallback. It
// returns the cached paths.
func Prefetch() ([]string, error) {
	if override := os.Getenv(envLibraryOverride); override != "" {
		return []string{override}, nil
	}
	manifest, err := fs.ReadFile(embeddedFiles, "checksums.txt")
	if err != nil {
		return nil, fmt.Errorf("read embedded checksum manifest: %w", err)
	}
	paths := make([]string, 0, len(nativeArtifacts))
	for _, artifact := range nativeArtifacts {
		cached, err := acquireVerifiedLibrary(artifact, manifest, libraryCacheRoot(), artifactBaseURL())
		if err != nil {
			return paths, err
		}
		paths = append(paths, cached)
	}
	return paths, nil
}
