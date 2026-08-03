//go:build !rivetgo_ffi_stub && ((darwin && arm64) || (linux && (amd64 || arm64)) || (windows && amd64))

package ffi

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

type cRunner struct{}
type cError struct{}

type cBytes struct {
	ptr *byte
	len uintptr
	cap uintptr
}

type cRunnerResult struct {
	runner *cRunner
	err    *cError
}

type cPollResult struct {
	payload cBytes
	err     *cError
}

type cSubmitResult struct {
	err *cError
}

type nativeAPI struct {
	once    sync.Once
	loadErr error
	handle  uintptr
	path    string

	abiVersion     func() uint32
	bytesFree      func(cBytes)
	stringFree     func(cBytes)
	errorJSON      func(*cError) cBytes
	errorFree      func(*cError)
	runnerNew      func(*byte, uintptr) cRunnerResult
	runnerFree     func(*cRunner)
	runnerPoll     func(*cRunner, uint32) cPollResult
	runnerSubmit   func(*cRunner, *byte, uintptr) cSubmitResult
	runnerShutdown func(*cRunner, uint32) cSubmitResult
}

var api nativeAPI

type embeddedLibrary struct {
	dir      string
	filename string
}

// Runner is an owned native runner handle.
type Runner struct {
	mu   sync.RWMutex
	ptr  *cRunner
	once sync.Once
}

// Error is an owned structured native error handle.
type Error struct {
	mu   sync.RWMutex
	ptr  *cError
	once sync.Once
}

// RunnerResult is the native result of constructing a runner.
type RunnerResult struct {
	Runner *Runner
	Error  *Error
}

// ErrorPayload is the stable JSON representation of an FFI error.
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e ErrorPayload) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Load extracts, verifies, opens, and binds the embedded native library.
func Load() error {
	api.once.Do(func() {
		api.loadErr = api.load()
	})
	return api.loadErr
}

func (a *nativeAPI) load() error {
	if len(embeddedLibraries) == 0 {
		return errors.New("no native libraries are embedded for this platform")
	}
	var loadErrors []error
	for _, library := range embeddedLibraries {
		libraryPath, err := extractVerifiedLibrary(
			embeddedFiles,
			library.dir,
			library.filename,
			"checksums.txt",
			libraryCacheRoot(),
		)
		if err != nil {
			return err
		}

		handle, err := openLibrary(libraryPath)
		if err != nil {
			loadErrors = append(loadErrors, fmt.Errorf("load native library %s: %w", libraryPath, err))
			continue
		}
		if err := a.bindAndValidate(handle); err != nil {
			_ = closeLibrary(handle)
			return err
		}
		a.handle = handle
		a.path = libraryPath
		return nil
	}
	return errors.Join(loadErrors...)
}

func (a *nativeAPI) bindAndValidate(handle uintptr) error {
	if err := a.register(handle); err != nil {
		return err
	}
	return requireABIVersion(a.abiVersion())
}

func requireABIVersion(got uint32) error {
	if got != ExpectedABIVersion {
		return fmt.Errorf(
			"native ABI version mismatch: library reports %d, Go expects %d",
			got,
			ExpectedABIVersion,
		)
	}
	return nil
}

func (a *nativeAPI) register(handle uintptr) error {
	for _, binding := range []struct {
		name string
		dst  any
	}{
		{"rk_abi_version", &a.abiVersion},
		{"rk_bytes_free", &a.bytesFree},
		{"rk_string_free", &a.stringFree},
		{"rk_error_json", &a.errorJSON},
		{"rk_error_free", &a.errorFree},
		{"rk_runner_new", &a.runnerNew},
		{"rk_runner_free", &a.runnerFree},
		{"rk_runner_poll", &a.runnerPoll},
		{"rk_runner_submit", &a.runnerSubmit},
		{"rk_runner_shutdown", &a.runnerShutdown},
	} {
		if err := registerLibraryFunc(handle, binding.name, binding.dst); err != nil {
			return err
		}
	}
	return nil
}

func registerLibraryFunc(handle uintptr, name string, dst any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("register native function %s: %v", name, recovered)
		}
	}()
	purego.RegisterLibFunc(dst, handle, name)
	return nil
}

// ABIVersion returns the version reported by the loaded native library.
func ABIVersion() (uint32, error) {
	if err := Load(); err != nil {
		return 0, err
	}
	return api.abiVersion(), nil
}

// NewRunner starts the native runner and waits for bounded engine
// registration. Native failures are returned in RunnerResult.Error.
func NewRunner(config []byte) (RunnerResult, error) {
	if err := Load(); err != nil {
		return RunnerResult{}, err
	}
	var configPtr *byte
	if len(config) > 0 {
		configPtr = unsafe.SliceData(config)
	}
	result := api.runnerNew(configPtr, uintptr(len(config)))
	runtime.KeepAlive(config)

	var runner *Runner
	if result.runner != nil {
		runner = &Runner{ptr: result.runner}
		runtime.SetFinalizer(runner, finalizeRunner)
	}
	var nativeError *Error
	if result.err != nil {
		nativeError = &Error{ptr: result.err}
		runtime.SetFinalizer(nativeError, finalizeError)
	}
	return RunnerResult{Runner: runner, Error: nativeError}, nil
}

func finalizeRunner(runner *Runner) {
	log.Printf("rivet-go: leaked native runner; releasing from finalizer")
	runner.Close()
}

func finalizeError(nativeError *Error) {
	log.Printf("rivet-go: leaked native error; releasing from finalizer")
	nativeError.Close()
}

// Close releases the native runner handle exactly once.
func (r *Runner) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		runtime.SetFinalizer(r, nil)
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.ptr != nil {
			api.runnerFree(r.ptr)
			r.ptr = nil
		}
	})
}

// Poll blocks for at most timeout and copies the returned EventBatch into Go
// memory before releasing its native buffer.
func (r *Runner) Poll(timeout time.Duration) ([]byte, error) {
	if r == nil {
		return nil, errors.New("native runner is nil")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.ptr == nil {
		return nil, errors.New("native runner is closed")
	}
	result := api.runnerPoll(r.ptr, durationMillis(timeout))
	runtime.KeepAlive(r)
	if result.payload.ptr != nil {
		defer api.bytesFree(result.payload)
	}
	if result.err != nil {
		return nil, consumeNativeError(result.err)
	}
	if result.payload.ptr == nil {
		if result.payload.len == 0 {
			return nil, nil
		}
		return nil, errors.New("native poll returned a nil pointer with non-zero length")
	}
	return append([]byte(nil), unsafe.Slice(result.payload.ptr, result.payload.len)...), nil
}

// Submit enqueues an encoded CommandBatch. It is safe to call concurrently.
func (r *Runner) Submit(batch []byte) error {
	if r == nil {
		return errors.New("native runner is nil")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.ptr == nil {
		return errors.New("native runner is closed")
	}
	var batchPtr *byte
	if len(batch) > 0 {
		batchPtr = unsafe.SliceData(batch)
	}
	result := api.runnerSubmit(r.ptr, batchPtr, uintptr(len(batch)))
	runtime.KeepAlive(batch)
	runtime.KeepAlive(r)
	if result.err != nil {
		return consumeNativeError(result.err)
	}
	return nil
}

// Shutdown begins the graceful drain. Poll remains valid until it returns a
// RunnerStopped event.
func (r *Runner) Shutdown(deadline time.Duration) error {
	if r == nil {
		return errors.New("native runner is nil")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.ptr == nil {
		return errors.New("native runner is closed")
	}
	result := api.runnerShutdown(r.ptr, durationMillis(deadline))
	runtime.KeepAlive(r)
	if result.err != nil {
		return consumeNativeError(result.err)
	}
	return nil
}

func durationMillis(duration time.Duration) uint32 {
	if duration <= 0 {
		return 0
	}
	millis := duration.Milliseconds()
	if millis > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(millis)
}

func consumeNativeError(ptr *cError) error {
	nativeError := &Error{ptr: ptr}
	payload, err := nativeError.Payload()
	nativeError.Close()
	if err != nil {
		return fmt.Errorf("decode native error: %w", err)
	}
	return payload
}

// JSON copies the native error's JSON representation into Go memory.
func (e *Error) JSON() ([]byte, error) {
	if e == nil {
		return nil, errors.New("native error is closed")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.ptr == nil {
		return nil, errors.New("native error is closed")
	}
	bytes := api.errorJSON(e.ptr)
	runtime.KeepAlive(e)
	if bytes.ptr == nil {
		if bytes.len == 0 {
			return nil, nil
		}
		return nil, errors.New("native error JSON returned a nil pointer with non-zero length")
	}
	defer api.bytesFree(bytes)
	return append([]byte(nil), unsafe.Slice(bytes.ptr, bytes.len)...), nil
}

// Payload decodes the native error's stable JSON representation.
func (e *Error) Payload() (ErrorPayload, error) {
	data, err := e.JSON()
	if err != nil {
		return ErrorPayload{}, err
	}
	var payload ErrorPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return ErrorPayload{}, fmt.Errorf("decode native error JSON: %w", err)
	}
	return payload, nil
}

// Close releases the native error handle exactly once.
func (e *Error) Close() {
	if e == nil {
		return
	}
	e.once.Do(func() {
		runtime.SetFinalizer(e, nil)
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.ptr != nil {
			api.errorFree(e.ptr)
			e.ptr = nil
		}
	})
}

func extractVerifiedLibrary(
	files fs.FS,
	dir string,
	filename string,
	checksumsPath string,
	cacheRoot string,
) (string, error) {
	libraryPath := path.Join(dir, filename)
	manifest, err := fs.ReadFile(files, checksumsPath)
	if err != nil {
		return "", fmt.Errorf("read embedded checksum manifest %s: %w", checksumsPath, err)
	}
	expected, err := checksumFor(manifest, libraryPath)
	if err != nil {
		return "", err
	}
	libraryBytes, err := fs.ReadFile(files, libraryPath)
	if err != nil {
		return "", fmt.Errorf("read embedded native library %s: %w", libraryPath, err)
	}
	actual := digestHex(libraryBytes)
	if actual != expected {
		return "", fmt.Errorf(
			"embedded native library checksum mismatch for %s: got %s, want %s",
			libraryPath,
			actual,
			expected,
		)
	}

	cacheBase := filepath.Join(cacheRoot, "rivet-go")
	if err := ensurePrivateDirectory(cacheBase); err != nil {
		return "", err
	}
	cacheDir := filepath.Join(cacheBase, actual)
	if err := ensurePrivateDirectory(cacheDir); err != nil {
		return "", err
	}
	targetPath := filepath.Join(cacheDir, filename)
	if info, err := os.Lstat(targetPath); err == nil {
		if info.Mode().IsRegular() {
			existing, readErr := os.ReadFile(targetPath)
			if readErr == nil && digestHex(existing) == actual {
				if err := os.Chmod(targetPath, 0o500); err != nil {
					return "", fmt.Errorf("secure cached native library %s: %w", targetPath, err)
				}
				return targetPath, nil
			}
		}
		if err := os.Remove(targetPath); err != nil {
			return "", fmt.Errorf("remove invalid cached native library %s: %w", targetPath, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("inspect cached native library %s: %w", targetPath, err)
	}

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
		if existing, readErr := os.ReadFile(targetPath); readErr == nil && digestHex(existing) == actual {
			return targetPath, nil
		}
		return "", fmt.Errorf("move verified native library into cache: %w", err)
	}
	return targetPath, nil
}

func ensurePrivateDirectory(directory string) error {
	if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("create private native cache directory %s: %w", directory, err)
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect native cache directory %s: %w", directory, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("native cache path %s is not a real directory", directory)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("secure native cache directory %s: %w", directory, err)
	}
	return nil
}

func checksumFor(manifest []byte, libraryPath string) (string, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(manifest)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || strings.TrimPrefix(fields[1], "*") != libraryPath {
			continue
		}
		if len(fields[0]) != sha256.Size*2 {
			return "", fmt.Errorf("invalid SHA-256 for %s in checksum manifest", libraryPath)
		}
		if _, err := hex.DecodeString(fields[0]); err != nil {
			return "", fmt.Errorf("invalid SHA-256 for %s in checksum manifest: %w", libraryPath, err)
		}
		return strings.ToLower(fields[0]), nil
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read checksum manifest: %w", err)
	}
	return "", fmt.Errorf("checksum manifest has no entry for %s", libraryPath)
}

func digestHex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func libraryCacheRoot() string {
	if root, err := os.UserCacheDir(); err == nil && root != "" {
		return root
	}
	return os.TempDir()
}
