//! Stable C ABI used by the pure-Go loader.
//!
//! The boundary owns the RivetKit runtime and exposes it as a callback-free
//! event pump to Go.

use std::any::Any;
use std::mem;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use serde::{Deserialize, Serialize};

mod actor_proxy;
mod correlation;
mod runner;
mod wire;

use runner::RunnerInner;
use wire::RunnerConfig;

#[cfg(panic = "abort")]
compile_error!("rivetkit-go-ffi requires panic=unwind for its C ABI firewall");

/// The single source of truth for the native ABI version.
pub const RK_ABI_VERSION: u32 = 13;

// Keep the upstream dependency in the release dylib rather than merely in the
// Cargo graph. Calling a small, stable core helper makes both the selected API
// and its implementation load-bearing while keeping upstream types private to
// this crate.
#[inline(never)]
fn core_pin_probe() -> bool {
    use rivetkit_core::{ActorKey, ActorKeySegment, format_actor_key};

    let key: ActorKey = vec![ActorKeySegment::String("rivet-go".to_owned())];
    std::hint::black_box(format_actor_key(std::hint::black_box(&key))) == "rivet-go"
}

/// An owned byte buffer allocated by Rust.
#[repr(C)]
#[derive(Debug, Default)]
pub struct RkBytes {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl RkBytes {
    fn from_vec(mut bytes: Vec<u8>) -> Self {
        let result = Self {
            ptr: bytes.as_mut_ptr(),
            len: bytes.len(),
            cap: bytes.capacity(),
        };
        mem::forget(bytes);
        result
    }
}

/// Opaque structured error handle. Its contents are accessed as JSON.
pub struct RkError {
    _private: [u8; 0],
}

/// Opaque runner handle.
pub struct RkRunner {
    _private: [u8; 0],
}

#[repr(C)]
pub struct RkRunnerResult {
    pub runner: *mut RkRunner,
    pub err: *mut RkError,
}

#[repr(C)]
pub struct RkPollResult {
    pub payload: RkBytes,
    pub err: *mut RkError,
}

#[repr(C)]
pub struct RkSubmitResult {
    pub err: *mut RkError,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct ErrorPayload {
    pub(crate) code: String,
    pub(crate) message: String,
}

impl ErrorPayload {
    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    fn internal_panic(payload: Box<dyn Any + Send>) -> Self {
        let message = if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_owned()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "non-string Rust panic".to_owned()
        };
        Self {
            code: "internal_panic".to_owned(),
            message,
        }
    }
}

fn error_into_raw(error: ErrorPayload) -> *mut RkError {
    Box::into_raw(Box::new(error)).cast()
}

fn runner_into_raw(runner: Box<RunnerInner>) -> *mut RkRunner {
    Box::into_raw(runner).cast()
}

unsafe fn runner_ref<'a>(runner: *const RkRunner) -> Result<&'a RunnerInner, ErrorPayload> {
    if runner.is_null() {
        return Err(ErrorPayload::new("invalid_runner", "runner handle is null"));
    }
    // SAFETY: The ownership contract requires a live RkRunner allocated by
    // runner_into_raw. Go serializes Close against all borrowed calls.
    Ok(unsafe { &*runner.cast::<RunnerInner>() })
}

unsafe fn borrowed_bytes<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], ErrorPayload> {
    if ptr.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(ErrorPayload::new(
            "invalid_buffer",
            "buffer pointer is null with non-zero length",
        ));
    }
    // SAFETY: The caller promises that ptr addresses len readable bytes for
    // the duration of this call.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

unsafe fn error_ref<'a>(error: *const RkError) -> &'a ErrorPayload {
    // SAFETY: The ownership contract requires callers to pass a live RkError
    // returned by this library.
    unsafe { &*error.cast::<ErrorPayload>() }
}

fn firewall_runner<F>(operation: F) -> RkRunnerResult
where
    F: FnOnce() -> Result<*mut RkRunner, ErrorPayload>,
{
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(runner)) => RkRunnerResult {
            runner,
            err: ptr::null_mut(),
        },
        Ok(Err(error)) => RkRunnerResult {
            runner: ptr::null_mut(),
            err: error_into_raw(error),
        },
        Err(payload) => RkRunnerResult {
            runner: ptr::null_mut(),
            err: error_into_raw(ErrorPayload::internal_panic(payload)),
        },
    }
}

fn firewall_poll<F>(operation: F) -> RkPollResult
where
    F: FnOnce() -> Result<RkBytes, ErrorPayload>,
{
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(payload)) => RkPollResult {
            payload,
            err: ptr::null_mut(),
        },
        Ok(Err(error)) => RkPollResult {
            payload: RkBytes::default(),
            err: error_into_raw(error),
        },
        Err(payload) => RkPollResult {
            payload: RkBytes::default(),
            err: error_into_raw(ErrorPayload::internal_panic(payload)),
        },
    }
}

fn firewall_submit<F>(operation: F) -> RkSubmitResult
where
    F: FnOnce() -> Result<(), ErrorPayload>,
{
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => RkSubmitResult {
            err: ptr::null_mut(),
        },
        Ok(Err(error)) => RkSubmitResult {
            err: error_into_raw(error),
        },
        Err(payload) => RkSubmitResult {
            err: error_into_raw(ErrorPayload::internal_panic(payload)),
        },
    }
}

fn free_bytes(bytes: RkBytes) {
    if bytes.ptr.is_null() {
        return;
    }
    // SAFETY: RkBytes values crossing this boundary originate in from_vec and
    // are freed exactly once by the receiving side.
    unsafe {
        drop(Vec::from_raw_parts(bytes.ptr, bytes.len, bytes.cap));
    }
}

/// Returns the native ABI version expected by the generated Go binding.
#[unsafe(no_mangle)]
pub extern "C" fn rk_abi_version() -> u32 {
    catch_unwind(AssertUnwindSafe(|| {
        if core_pin_probe() { RK_ABI_VERSION } else { 0 }
    }))
    .unwrap_or(0)
}

/// Releases an owned byte buffer returned by Rust.
#[unsafe(no_mangle)]
pub extern "C" fn rk_bytes_free(bytes: RkBytes) {
    let _ = catch_unwind(AssertUnwindSafe(|| free_bytes(bytes)));
}

/// Releases an owned UTF-8 string buffer returned by Rust.
#[unsafe(no_mangle)]
pub extern "C" fn rk_string_free(string: RkBytes) {
    let _ = catch_unwind(AssertUnwindSafe(|| free_bytes(string)));
}

/// Serializes an error as UTF-8 JSON owned by the caller.
///
/// # Safety
///
/// `error` must be null or a live `RkError` returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_error_json(error: *const RkError) -> RkBytes {
    match catch_unwind(AssertUnwindSafe(|| {
        if error.is_null() {
            return Vec::new();
        }
        // SAFETY: The caller promises a live error handle.
        serde_json::to_vec(unsafe { error_ref(error) }).unwrap_or_else(|_| {
            br#"{"code":"internal_panic","message":"failed to serialize error"}"#.to_vec()
        })
    })) {
        Ok(bytes) => RkBytes::from_vec(bytes),
        Err(_) => RkBytes::from_vec(
            br#"{"code":"internal_panic","message":"panic serializing error"}"#.to_vec(),
        ),
    }
}

/// Releases an owned error handle.
///
/// # Safety
///
/// `error` must be null or an `RkError` returned by this library that has not
/// already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_error_free(error: *mut RkError) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !error.is_null() {
            // SAFETY: Error pointers originate in error_into_raw and are freed once.
            unsafe {
                drop(Box::from_raw(error.cast::<ErrorPayload>()));
            }
        }
    }));
}

/// Creates a runner and waits for bounded, management-API-confirmed engine
/// registration before returning its owned handle.
///
/// # Safety
///
/// `config` must be null when `config_len` is zero or point to
/// `config_len` readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_runner_new(config: *const u8, config_len: usize) -> RkRunnerResult {
    firewall_runner(|| {
        // SAFETY: Forwarding the caller's buffer promise.
        let bytes = unsafe { borrowed_bytes(config, config_len) }?;
        let config: RunnerConfig = rmp_serde::from_slice(bytes).map_err(|error| {
            ErrorPayload::new(
                "invalid_config",
                format!("decode MessagePack RunnerConfig: {error}"),
            )
        })?;
        RunnerInner::start(config).map(runner_into_raw)
    })
}

/// Releases a runner handle, forcing a bounded drain if shutdown was omitted.
///
/// # Safety
///
/// `runner` must be null or a live handle returned by `rk_runner_new` that has
/// not already been freed. No other call may borrow it concurrently.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_runner_free(runner: *mut RkRunner) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !runner.is_null() {
            // SAFETY: The caller transfers the single live allocation back.
            let runner = unsafe { Box::from_raw(runner.cast::<RunnerInner>()) };
            runner.close();
        }
    }));
}

/// Polls a runner for an encoded EventBatch.
///
/// # Safety
///
/// `runner` must be a live handle for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_runner_poll(runner: *mut RkRunner, timeout_ms: u32) -> RkPollResult {
    firewall_poll(|| {
        // SAFETY: Forwarding the caller's live-handle promise.
        let runner = unsafe { runner_ref(runner) }?;
        runner.poll(timeout_ms).map(RkBytes::from_vec)
    })
}

/// Submits an encoded CommandBatch without blocking on command execution.
///
/// # Safety
///
/// `runner` must be live and `batch` must satisfy the borrowed-buffer rules.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_runner_submit(
    runner: *mut RkRunner,
    batch: *const u8,
    len: usize,
) -> RkSubmitResult {
    firewall_submit(|| {
        // SAFETY: Forwarding the caller's live-handle and buffer promises.
        let runner = unsafe { runner_ref(runner) }?;
        let batch = unsafe { borrowed_bytes(batch, len) }?;
        runner.submit(batch)
    })
}

/// Begins graceful shutdown; polling continues through RunnerStopped.
///
/// # Safety
///
/// `runner` must be a live handle for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_runner_shutdown(
    runner: *mut RkRunner,
    deadline_ms: u32,
) -> RkSubmitResult {
    firewall_submit(|| {
        // SAFETY: Forwarding the caller's live-handle promise.
        let runner = unsafe { runner_ref(runner) }?;
        runner.shutdown(deadline_ms)
    })
}

// purego intentionally does not marshal structs on Windows. These private,
// additive exports preserve the public ABI while giving the Go loader a
// pointer-only representation of the same calls on that platform.
#[cfg(windows)]
#[doc(hidden)]
#[unsafe(no_mangle)]
pub extern "C" fn rk_windows_bytes_free(ptr: *mut u8, len: usize, cap: usize) {
    rk_bytes_free(RkBytes { ptr, len, cap });
}

#[cfg(windows)]
#[doc(hidden)]
#[unsafe(no_mangle)]
pub extern "C" fn rk_windows_string_free(ptr: *mut u8, len: usize, cap: usize) {
    rk_string_free(RkBytes { ptr, len, cap });
}

#[cfg(windows)]
#[doc(hidden)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_windows_error_json(error: *const RkError, out: *mut RkBytes) {
    if out.is_null() {
        return;
    }
    // SAFETY: Forwarding the caller's live error handle; out is non-null and
    // points to writable storage supplied for this call.
    unsafe { out.write(rk_error_json(error)) };
}

#[cfg(windows)]
#[doc(hidden)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_windows_runner_new(
    config: *const u8,
    config_len: usize,
    out: *mut RkRunnerResult,
) {
    if out.is_null() {
        return;
    }
    // SAFETY: Forwarding the caller's borrowed-buffer promise; out is non-null
    // and points to writable storage supplied for this call.
    unsafe { out.write(rk_runner_new(config, config_len)) };
}

#[cfg(windows)]
#[doc(hidden)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_windows_runner_poll(
    runner: *mut RkRunner,
    timeout_ms: u32,
    out: *mut RkPollResult,
) {
    if out.is_null() {
        return;
    }
    // SAFETY: Forwarding the caller's live runner handle; out is non-null and
    // points to writable storage supplied for this call.
    unsafe { out.write(rk_runner_poll(runner, timeout_ms)) };
}

#[cfg(windows)]
#[doc(hidden)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_windows_runner_submit(
    runner: *mut RkRunner,
    batch: *const u8,
    len: usize,
    out: *mut RkSubmitResult,
) {
    if out.is_null() {
        return;
    }
    // SAFETY: Forwarding the caller's live runner and borrowed-buffer
    // promises; out is non-null and writable for this call.
    unsafe { out.write(rk_runner_submit(runner, batch, len)) };
}

#[cfg(windows)]
#[doc(hidden)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_windows_runner_shutdown(
    runner: *mut RkRunner,
    deadline_ms: u32,
    out: *mut RkSubmitResult,
) {
    if out.is_null() {
        return;
    }
    // SAFETY: Forwarding the caller's live runner handle; out is non-null and
    // writable for this call.
    unsafe { out.write(rk_runner_shutdown(runner, deadline_ms)) };
}

// This probe is deliberately excluded from the generated public header. The
// build script enables it only so Go tests can prove that a panic originating
// inside the loaded cdylib is caught before returning across extern "C".
#[cfg(any(test, feature = "ffi-test"))]
#[unsafe(no_mangle)]
pub extern "C" fn rk_test_panic() -> RkSubmitResult {
    firewall_submit(|| panic!("panic firewall probe"))
}

#[cfg(all(windows, any(test, feature = "ffi-test")))]
#[doc(hidden)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rk_windows_test_panic(out: *mut RkSubmitResult) {
    if out.is_null() {
        return;
    }
    // SAFETY: out is non-null and points to writable storage supplied for
    // this call.
    unsafe { out.write(rk_test_panic()) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::Result;
    use depot_client::query::{
        BindParam, ColumnValue, SqliteStatementError, exec_statements, execute_single_statement,
        execute_statement, query_statement,
    };
    use depot_client::vfs::{
        SqliteTransport, SqliteTransportHandle, SqliteVfs, VfsConfig, open_connection,
    };
    use libsqlite3_sys::{
        SQLITE_OK, SQLITE_OPEN_CREATE, SQLITE_OPEN_READWRITE, SQLITE_RANGE, sqlite3, sqlite3_close,
        sqlite3_open,
    };
    use rivet_envoy_protocol as protocol;

    #[derive(Clone, Copy)]
    enum FinalFlushBehavior {
        Error,
        Timeout,
    }

    struct FinalFlushTransport {
        behavior: FinalFlushBehavior,
    }

    struct MemoryDb(*mut sqlite3);

    impl MemoryDb {
        fn open() -> Self {
            let name = CString::new(":memory:").expect("valid SQLite name");
            let mut db = ptr::null_mut();
            // SAFETY: name is a live NUL-terminated string and db is writable.
            let rc = unsafe { sqlite3_open(name.as_ptr(), &mut db) };
            assert_eq!(rc, SQLITE_OK);
            Self(db)
        }

        fn as_ptr(&self) -> *mut sqlite3 {
            self.0
        }
    }

    impl Drop for MemoryDb {
        fn drop(&mut self) {
            // SAFETY: this test owns the live SQLite connection.
            let rc = unsafe { sqlite3_close(self.0) };
            assert_eq!(rc, SQLITE_OK);
        }
    }

    #[async_trait::async_trait]
    impl SqliteTransport for FinalFlushTransport {
        async fn get_pages(
            &self,
            _request: protocol::SqliteGetPagesRequest,
        ) -> Result<protocol::SqliteGetPagesResponse> {
            Ok(protocol::SqliteGetPagesResponse::SqliteErrorResponse(
                protocol::SqliteErrorResponse {
                    group: "actor".to_owned(),
                    code: "not_found".to_owned(),
                    message: "actor does not exist".to_owned(),
                },
            ))
        }

        async fn commit(
            &self,
            _request: protocol::SqliteCommitRequest,
        ) -> Result<protocol::SqliteCommitResponse> {
            match self.behavior {
                FinalFlushBehavior::Error => anyhow::bail!("forced final flush failure"),
                FinalFlushBehavior::Timeout => std::future::pending().await,
            }
        }
    }

    fn decode_and_free(error: *mut RkError) -> ErrorPayload {
        assert!(!error.is_null());
        // SAFETY: error is a live handle returned by this module.
        let bytes = unsafe { rk_error_json(error) };
        assert!(!bytes.ptr.is_null());
        // SAFETY: rk_error_json returned a live buffer for bytes.len bytes.
        let json = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec();
        rk_bytes_free(bytes);
        // SAFETY: error is live and has not previously been freed.
        unsafe { rk_error_free(error) };
        serde_json::from_slice(&json).expect("valid error JSON")
    }

    #[test]
    fn error_json_round_trip() {
        // SAFETY: null plus zero length is a valid borrowed empty buffer.
        let result = unsafe { rk_runner_new(ptr::null(), 0) };
        assert!(result.runner.is_null());
        let error = decode_and_free(result.err);
        assert_eq!(error.code, "invalid_config");
        assert!(error.message.contains("MessagePack RunnerConfig"));
    }

    #[test]
    fn panic_firewall_returns_an_error() {
        let result = rk_test_panic();
        let error = decode_and_free(result.err);
        assert_eq!(error.code, "internal_panic");
        assert_eq!(error.message, "panic firewall probe");
    }

    #[test]
    fn pinned_core_probe_executes_upstream_code() {
        assert!(core_pin_probe());
    }

    #[test]
    fn sqlite_execution_requires_exact_parameter_count() {
        let db = MemoryDb::open();
        let cases = [
            ("SELECT ?, ?;", Some(&[BindParam::Integer(1)][..]), 2, 1),
            ("SELECT ?;", None, 1, 0),
            (
                "SELECT ?;",
                Some(&[BindParam::Integer(1), BindParam::Integer(2)][..]),
                1,
                2,
            ),
        ];

        for (sql, params, expected, received) in cases {
            let err = execute_single_statement(db.as_ptr(), sql, params)
                .expect_err("mismatched parameter count should fail");
            assert_parameter_count_error(&err, expected, received);
        }

        let err = execute_statement(db.as_ptr(), "SELECT ?, ?;", None)
            .expect_err("legacy execute path should reject missing parameters");
        assert_parameter_count_error(&err, 2, 0);
        let err = query_statement(db.as_ptr(), "SELECT ?, ?;", Some(&[BindParam::Integer(1)]))
            .expect_err("legacy query path should reject missing parameters");
        assert_parameter_count_error(&err, 2, 1);

        exec_statements(
            db.as_ptr(),
            "CREATE TABLE exact_args(a INTEGER, b INTEGER);",
        )
        .expect("create test table");
        execute_single_statement(
            db.as_ptr(),
            "INSERT INTO exact_args VALUES (?, ?);",
            Some(&[BindParam::Integer(1)]),
        )
        .expect_err("missing insert parameter should fail before execution");
        let count = execute_single_statement(
            db.as_ptr(),
            "SELECT COUNT(*) AS count FROM exact_args;",
            None,
        )
        .expect("query test table");
        assert_eq!(count.rows, vec![vec![ColumnValue::Integer(0)]]);
    }

    fn assert_parameter_count_error(error: &anyhow::Error, expected: usize, received: usize) {
        let typed = error.downcast_ref::<SqliteStatementError>().unwrap();
        assert_eq!(typed.code, SQLITE_RANGE);
        assert_eq!(typed.statement_index, 0);
        assert_eq!(
            typed.message,
            format!("sqlite statement expects {expected} parameters, received {received}")
        );
    }

    #[test]
    fn native_database_closes_after_final_flush_error_and_timeout() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .build()
            .expect("build test runtime");

        for (behavior, suffix) in [
            (FinalFlushBehavior::Error, "error"),
            (FinalFlushBehavior::Timeout, "timeout"),
        ] {
            let actor_id = format!("final-flush-{suffix}");
            let vfs_name = format!("final-flush-vfs-{suffix}");
            let transport: SqliteTransportHandle = Arc::new(FinalFlushTransport { behavior });
            let config = VfsConfig::default();
            let vfs = Arc::new(
                SqliteVfs::register_with_transport(
                    &vfs_name,
                    transport,
                    actor_id.clone(),
                    runtime.handle().clone(),
                    config,
                    None,
                )
                .expect("register test VFS"),
            );
            let database = open_connection(
                Arc::clone(&vfs),
                &actor_id,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
            )
            .expect("open test database");
            vfs.stage_dirty_page_for_test();
            assert!(
                database.sqlite_vfs_metrics().write_buffer_dirty_pages > 0,
                "test setup must leave pages for the final flush"
            );

            let started = Instant::now();
            drop(database);
            assert_eq!(
                vfs.main_close_count_for_test(),
                1,
                "final flush {suffix} must still close the native SQLite file"
            );
            assert_eq!(
                vfs.native_close_rc_for_test(),
                libsqlite3_sys::SQLITE_OK,
                "final flush {suffix} must release the native SQLite connection"
            );
            if matches!(behavior, FinalFlushBehavior::Timeout) {
                assert!(
                    started.elapsed() < Duration::from_secs(2),
                    "timed-out final flush retried without its boundary"
                );
            }
        }
    }
}
