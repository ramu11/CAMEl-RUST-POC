//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::ffi::c_void;
use std::slice;

use crate::protocol::decode_request;
use crate::runtime::{CompletionCallback, RustRuntime, complete_invocation};

const CAMEL_RUST_OK: i32 = 0;
const CAMEL_RUST_INVALID_RUNTIME: i32 = 1;
const CAMEL_RUST_INVALID_REQUEST: i32 = 2;
const CAMEL_RUST_INVALID_CALLBACK: i32 = 3;

#[unsafe(no_mangle)]
pub extern "C" fn camel_rust_runtime_create() -> u64 {
    let runtime = Box::new(RustRuntime::new());

    Box::into_raw(runtime) as u64
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn camel_rust_execute(
    runtime_handle: u64,
    invocation_id: u64,
    request_ptr: *const u8,
    request_len: usize,
    callback: Option<CompletionCallback>,
    user_data: *mut c_void,
) -> i32 {
    if runtime_handle == 0 {
        return CAMEL_RUST_INVALID_RUNTIME;
    }

    let Some(callback) = callback else {
        return CAMEL_RUST_INVALID_CALLBACK;
    };

    if request_ptr.is_null() || request_len == 0 {
        return CAMEL_RUST_INVALID_REQUEST;
    }

    let runtime = unsafe { &*(runtime_handle as *const RustRuntime) };

    let request_bytes = unsafe { slice::from_raw_parts(request_ptr, request_len) };

    let request = match decode_request(request_bytes) {
        Ok(request) => request,
        Err(_) => return CAMEL_RUST_INVALID_REQUEST,
    };

    if !runtime.invocations.register(invocation_id) {
        return CAMEL_RUST_INVALID_REQUEST;
    }

    let registry = runtime.invocations.clone();
    let user_data = user_data as usize;

    std::thread::spawn(move || {
        complete_invocation(registry, invocation_id, request, callback, user_data);
    });

    CAMEL_RUST_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn camel_rust_cancel(runtime_handle: u64, invocation_id: u64) -> i32 {
    if runtime_handle == 0 {
        return CAMEL_RUST_INVALID_RUNTIME;
    }

    let runtime = unsafe { &*(runtime_handle as *const RustRuntime) };

    runtime.invocations.cancel(invocation_id);

    CAMEL_RUST_OK
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn camel_rust_runtime_destroy(runtime_handle: u64) {
    if runtime_handle == 0 {
        return;
    }

    let runtime = unsafe { Box::from_raw(runtime_handle as *mut RustRuntime) };

    runtime.invocations.begin_destroy();
    runtime.invocations.wait_for_empty();

    drop(runtime);
}
