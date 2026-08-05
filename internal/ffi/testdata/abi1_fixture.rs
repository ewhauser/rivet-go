use std::ffi::c_void;
use std::ptr;

#[repr(C)]
pub struct RkBytes {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

#[repr(C)]
pub struct RkRunnerResult {
    runner: *mut c_void,
    err: *mut c_void,
}

#[repr(C)]
pub struct RkPollResult {
    payload: RkBytes,
    err: *mut c_void,
}

#[repr(C)]
pub struct RkSubmitResult {
    err: *mut c_void,
}

#[no_mangle]
pub extern "C" fn rk_abi_version() -> u32 {
    if cfg!(rk_abi_7) {
        7
    } else if cfg!(rk_abi_6) {
        6
    } else if cfg!(rk_abi_5) {
        5
    } else {
        1
    }
}

#[no_mangle]
pub extern "C" fn rk_bytes_free(_: RkBytes) {}

#[no_mangle]
pub extern "C" fn rk_string_free(_: RkBytes) {}

#[no_mangle]
pub extern "C" fn rk_error_json(_: *const c_void) -> RkBytes {
    RkBytes {
        ptr: ptr::null_mut(),
        len: 0,
        cap: 0,
    }
}

#[no_mangle]
pub extern "C" fn rk_error_free(_: *mut c_void) {}

#[no_mangle]
pub extern "C" fn rk_runner_new(_: *const u8, _: usize) -> RkRunnerResult {
    RkRunnerResult {
        runner: ptr::null_mut(),
        err: ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn rk_runner_free(_: *mut c_void) {}

#[no_mangle]
pub extern "C" fn rk_runner_poll(_: *mut c_void, _: u32) -> RkPollResult {
    RkPollResult {
        payload: RkBytes {
            ptr: ptr::null_mut(),
            len: 0,
            cap: 0,
        },
        err: ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn rk_runner_submit(_: *mut c_void, _: *const u8, _: usize) -> RkSubmitResult {
    RkSubmitResult {
        err: ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn rk_runner_shutdown(_: *mut c_void, _: u32) -> RkSubmitResult {
    RkSubmitResult {
        err: ptr::null_mut(),
    }
}
