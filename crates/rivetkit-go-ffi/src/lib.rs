//! Stable C ABI used by the pure-Go loader.
//!
//! M0 deliberately implements only ABI identity, owned buffers, structured
//! errors, and not-implemented runner entry points. Runner behavior begins in
//! M1.

use std::any::Any;
use std::mem;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use serde::{Deserialize, Serialize};

/// The single source of truth for the native ABI version.
pub const RK_ABI_VERSION: u32 = 1;

// This compile-time assertion makes the rivetkit-core pin load-bearing without
// exporting any upstream symbol through our ABI.
const _: fn(&rivetkit_core::ActorKey) -> String = rivetkit_core::format_actor_key;

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

/// Opaque runner handle. M0 never creates one.
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
struct ErrorPayload {
    code: String,
    message: String,
}

impl ErrorPayload {
    fn not_implemented(function: &str) -> Self {
        Self {
            code: "not_implemented".to_owned(),
            message: format!("{function} is not implemented in M0"),
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
    catch_unwind(AssertUnwindSafe(|| RK_ABI_VERSION)).unwrap_or(0)
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
#[unsafe(no_mangle)]
pub extern "C" fn rk_error_json(error: *const RkError) -> RkBytes {
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
#[unsafe(no_mangle)]
pub extern "C" fn rk_error_free(error: *mut RkError) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !error.is_null() {
            // SAFETY: Error pointers originate in error_into_raw and are freed once.
            unsafe {
                drop(Box::from_raw(error.cast::<ErrorPayload>()));
            }
        }
    }));
}

/// Creates a runner. M0 returns a structured not-implemented error.
#[unsafe(no_mangle)]
pub extern "C" fn rk_runner_new(_config: *const u8, _config_len: usize) -> RkRunnerResult {
    firewall_runner(|| Err(ErrorPayload::not_implemented("rk_runner_new")))
}

/// Releases a runner handle. M0 has no constructible runner handles.
#[unsafe(no_mangle)]
pub extern "C" fn rk_runner_free(_runner: *mut RkRunner) {
    let _ = catch_unwind(AssertUnwindSafe(|| {}));
}

/// Polls a runner. M0 returns a structured not-implemented error.
#[unsafe(no_mangle)]
pub extern "C" fn rk_runner_poll(_runner: *mut RkRunner, _timeout_ms: u32) -> RkPollResult {
    firewall_poll(|| Err(ErrorPayload::not_implemented("rk_runner_poll")))
}

/// Submits a command batch. M0 returns a structured not-implemented error.
#[unsafe(no_mangle)]
pub extern "C" fn rk_runner_submit(
    _runner: *mut RkRunner,
    _batch: *const u8,
    len: usize,
) -> RkSubmitResult {
    let _ = len;
    firewall_submit(|| Err(ErrorPayload::not_implemented("rk_runner_submit")))
}

/// Begins graceful shutdown. M0 returns a structured not-implemented error.
#[unsafe(no_mangle)]
pub extern "C" fn rk_runner_shutdown(_runner: *mut RkRunner, _deadline_ms: u32) -> RkSubmitResult {
    firewall_submit(|| Err(ErrorPayload::not_implemented("rk_runner_shutdown")))
}

#[cfg(test)]
#[unsafe(no_mangle)]
extern "C" fn rk_test_panic() -> RkSubmitResult {
    firewall_submit(|| panic!("panic firewall probe"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_and_free(error: *mut RkError) -> ErrorPayload {
        assert!(!error.is_null());
        let bytes = rk_error_json(error);
        assert!(!bytes.ptr.is_null());
        // SAFETY: rk_error_json returned a live buffer for bytes.len bytes.
        let json = unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) }.to_vec();
        rk_bytes_free(bytes);
        rk_error_free(error);
        serde_json::from_slice(&json).expect("valid error JSON")
    }

    #[test]
    fn error_json_round_trip() {
        let result = rk_runner_new(ptr::null(), 0);
        assert!(result.runner.is_null());
        assert_eq!(
            decode_and_free(result.err),
            ErrorPayload::not_implemented("rk_runner_new")
        );
    }

    #[test]
    fn panic_firewall_returns_an_error() {
        let result = rk_test_panic();
        let error = decode_and_free(result.err);
        assert_eq!(error.code, "internal_panic");
        assert_eq!(error.message, "panic firewall probe");
    }
}
