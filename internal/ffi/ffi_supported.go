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
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
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

var (
	api           nativeAPI
	activeRunners atomic.Int64
	activeErrors  atomic.Int64
	activeBuffers atomic.Int64
)

// HandleCounts reports native allocations currently owned by Go wrappers.
// It is used by the soak oracle after the runner has fully drained.
type HandleCounts struct {
	Runners int64
	Errors  int64
	Buffers int64
}

func ActiveHandleCounts() HandleCounts {
	return HandleCounts{
		Runners: activeRunners.Load(),
		Errors:  activeErrors.Load(),
		Buffers: activeBuffers.Load(),
	}
}

// Runner is an owned native runner handle.
type Runner struct {
	mu     sync.RWMutex
	ptr    *cRunner
	once   sync.Once
	logger *slog.Logger
}

// Error is an owned structured native error handle.
type Error struct {
	mu     sync.RWMutex
	ptr    *cError
	once   sync.Once
	logger *slog.Logger
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

func (e ErrorPayload) ErrorCode() string { return e.Code }

// Load acquires (cache or checksummed release download), verifies, opens,
// and binds the native library for this platform.
func Load() error {
	api.once.Do(func() {
		api.loadErr = api.load()
	})
	return api.loadErr
}

func (a *nativeAPI) load() error {
	if len(nativeArtifacts) == 0 {
		return errors.New("no native artifacts are pinned for this platform")
	}
	if override := os.Getenv(envLibraryOverride); override != "" {
		handle, err := openLibrary(override)
		if err != nil {
			return fmt.Errorf("load native library from %s=%s: %w", envLibraryOverride, override, err)
		}
		if err := a.bindAndValidate(handle); err != nil {
			_ = closeLibrary(handle)
			return err
		}
		a.handle = handle
		a.path = override
		return nil
	}
	manifest, err := fs.ReadFile(embeddedFiles, "checksums.txt")
	if err != nil {
		return fmt.Errorf("read embedded checksum manifest: %w", err)
	}
	var loadErrors []error
	for _, artifact := range nativeArtifacts {
		libraryPath, err := acquireVerifiedLibrary(artifact, manifest, libraryCacheRoot(), artifactBaseURL())
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
	if err := registerLibraryFunc(handle, "rk_abi_version", &a.abiVersion); err != nil {
		return err
	}
	if err := requireABIVersion(a.abiVersion()); err != nil {
		return err
	}
	return a.register(handle)
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
		activeRunners.Add(1)
		runtime.SetFinalizer(runner, finalizeRunner)
	}
	var nativeError *Error
	if result.err != nil {
		nativeError = newError(result.err)
	}
	return RunnerResult{Runner: runner, Error: nativeError}, nil
}

func finalizeRunner(runner *Runner) {
	runner.mu.RLock()
	logger := runner.logger
	runner.mu.RUnlock()
	if logger != nil {
		logger.Error("leaked native runner; releasing from finalizer")
	}
	runner.Close()
}

func finalizeError(nativeError *Error) {
	nativeError.mu.RLock()
	logger := nativeError.logger
	nativeError.mu.RUnlock()
	if logger != nil {
		logger.Error("leaked native error; releasing from finalizer")
	}
	nativeError.Close()
}

// SetLogger configures structured diagnostics for handle-leak backstops and
// native errors returned by subsequent runner calls. Nil discards them.
func (r *Runner) SetLogger(logger *slog.Logger) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.logger = logger
	r.mu.Unlock()
}

// SetLogger configures structured diagnostics for this owned error handle.
func (e *Error) SetLogger(logger *slog.Logger) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.logger = logger
	e.mu.Unlock()
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
			activeRunners.Add(-1)
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
		activeBuffers.Add(1)
		defer func() {
			api.bytesFree(result.payload)
			activeBuffers.Add(-1)
		}()
	}
	if result.err != nil {
		return nil, consumeNativeError(result.err, r.logger)
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
		return consumeNativeError(result.err, r.logger)
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
		return consumeNativeError(result.err, r.logger)
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

func consumeNativeError(ptr *cError, logger *slog.Logger) error {
	nativeError := newError(ptr)
	nativeError.SetLogger(logger)
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
	activeBuffers.Add(1)
	defer func() {
		api.bytesFree(bytes)
		activeBuffers.Add(-1)
	}()
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
			activeErrors.Add(-1)
		}
	})
}

func newError(ptr *cError) *Error {
	if ptr == nil {
		return nil
	}
	nativeError := &Error{ptr: ptr}
	activeErrors.Add(1)
	runtime.SetFinalizer(nativeError, finalizeError)
	return nativeError
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
