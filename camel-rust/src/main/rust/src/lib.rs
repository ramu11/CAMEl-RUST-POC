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

use ciborium::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::slice;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

const CAMEL_RUST_OK: i32 = 0;
const CAMEL_RUST_INVALID_RUNTIME: i32 = 1;
const CAMEL_RUST_INVALID_REQUEST: i32 = 2;
const CAMEL_RUST_INVALID_CALLBACK: i32 = 3;

const PROTOCOL_VERSION: u64 = 1;
const WORK_DURATION: Duration = Duration::from_millis(50);

struct RustRuntime {
    invocations: Arc<InvocationRegistry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InvocationState {
    Active,
    CancellationRequested,
}

struct InvocationRegistryState {
    states: HashMap<u64, InvocationState>,
    destroying: bool,
}

struct InvocationRegistry {
    state: Mutex<InvocationRegistryState>,
    changed: Condvar,
}

type CompletionCallback = unsafe extern "C" fn(
    invocation_id: u64,
    response: *const u8,
    response_len: usize,
    user_data: *mut std::ffi::c_void,
);

#[derive(Debug, Serialize, Deserialize)]
struct RustInvocationRequest {
    version: u64,

    #[serde(rename = "invocationId")]
    invocation_id: String,

    body: Value,

    headers: Value,

    properties: Value,
}

impl RustInvocationRequest {
    fn validate(&self) -> Result<(), &'static str> {
        if self.version != PROTOCOL_VERSION {
            return Err("Unsupported protocol version");
        }

        if self.invocation_id.is_empty() {
            return Err("invocationId must not be empty");
        }

        if !matches!(self.headers, Value::Map(_)) {
            return Err("headers must be a CBOR map");
        }

        if !matches!(self.properties, Value::Map(_)) {
            return Err("properties must be a CBOR map");
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RustInvocationResponse {
    version: u64,

    #[serde(rename = "invocationId")]
    invocation_id: String,

    status: String,

    body: Value,

    headers: Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RustInvocationError>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RustInvocationError {
    #[serde(rename = "type")]
    error_type: String,

    message: String,
}

impl RustInvocationResponse {
    fn success(request: RustInvocationRequest) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            invocation_id: request.invocation_id,
            status: "SUCCESS".to_string(),
            body: request.body,
            headers: request.headers,
            error: None,
        }
    }

    fn failure(
        invocation_id: String,
        body: Value,
        headers: Value,
        error_type: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            invocation_id,
            status: "FAILURE".to_string(),
            body,
            headers,
            error: Some(RustInvocationError {
                error_type: error_type.into(),
                message: message.into(),
            }),
        }
    }
}

impl InvocationRegistry {
    fn new() -> Self {
        Self {
            state: Mutex::new(InvocationRegistryState {
                states: HashMap::new(),
                destroying: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn register(&self, invocation_id: u64) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        if state.destroying {
            return false;
        }

        state.states.insert(invocation_id, InvocationState::Active);

        true
    }

    fn begin_destroy(&self) {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        state.destroying = true;
        self.changed.notify_all();
    }

    fn wait_for_empty(&self) {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        while !state.states.is_empty() {
            state = self
                .changed
                .wait(state)
                .expect("invocation registry mutex must not be poisoned");
        }
    }

    fn cancel(&self, invocation_id: u64) {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        if let Some(invocation_state) = state.states.get_mut(&invocation_id) {
            *invocation_state = InvocationState::CancellationRequested;
            self.changed.notify_all();
        }
    }

    fn wait_for_work_or_cancellation(&self, invocation_id: u64, duration: Duration) -> bool {
        let state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        if matches!(
            state.states.get(&invocation_id),
            Some(InvocationState::CancellationRequested)
        ) {
            return true;
        }

        let (state, _) = self
            .changed
            .wait_timeout_while(state, duration, |state| {
                matches!(
                    state.states.get(&invocation_id),
                    Some(InvocationState::Active)
                )
            })
            .expect("invocation registry mutex must not be poisoned");

        matches!(
            state.states.get(&invocation_id),
            Some(InvocationState::CancellationRequested)
        )
    }

    fn remove(&self, invocation_id: u64) {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        state.states.remove(&invocation_id);
        self.changed.notify_all();
    }
}

impl RustRuntime {
    fn new() -> Self {
        Self {
            invocations: Arc::new(InvocationRegistry::new()),
        }
    }
}

fn decode_request(request: &[u8]) -> Result<RustInvocationRequest, String> {
    ciborium::from_reader(request).map_err(|error| error.to_string())
}

fn encode_response(response: &RustInvocationResponse) -> Result<Vec<u8>, String> {
    let mut encoded = Vec::new();

    ciborium::into_writer(response, &mut encoded).map_err(|error| error.to_string())?;

    Ok(encoded)
}

fn complete_invocation(
    registry: Arc<InvocationRegistry>,
    invocation_id: u64,
    request: RustInvocationRequest,
    completion_callback: CompletionCallback,
    user_data: usize,
) {
    let cancelled = registry.wait_for_work_or_cancellation(invocation_id, WORK_DURATION);

    let response = if cancelled {
        RustInvocationResponse::failure(
            request.invocation_id,
            request.body,
            request.headers,
            "org.apache.camel.component.rust.CancellationException",
            "Rust invocation was cancelled",
        )
    } else {
        RustInvocationResponse::success(request)
    };

    let response_bytes = match encode_response(&response) {
        Ok(response) => response,
        Err(_) => {
            registry.remove(invocation_id);
            return;
        }
    };

    unsafe {
        completion_callback(
            invocation_id,
            response_bytes.as_ptr(),
            response_bytes.len(),
            user_data as *mut std::ffi::c_void,
        );
    }

    registry.remove(invocation_id);
}

#[unsafe(no_mangle)]
pub extern "C" fn camel_rust_runtime_create() -> u64 {
    let runtime = Box::new(RustRuntime::new());
    Box::into_raw(runtime) as u64
}

#[unsafe(no_mangle)]
pub extern "C" fn camel_rust_execute(
    runtime_handle: u64,
    invocation_id: u64,
    request: *const u8,
    request_len: usize,
    completion_callback: Option<CompletionCallback>,
    user_data: *mut std::ffi::c_void,
) -> i32 {
    if runtime_handle == 0 {
        return CAMEL_RUST_INVALID_RUNTIME;
    }

    if request.is_null() || request_len == 0 {
        return CAMEL_RUST_INVALID_REQUEST;
    }

    let completion_callback = match completion_callback {
        Some(callback) => callback,
        None => return CAMEL_RUST_INVALID_CALLBACK,
    };

    let runtime = unsafe { &*(runtime_handle as *const RustRuntime) };

    let request_bytes = unsafe { slice::from_raw_parts(request, request_len) };

    let decoded_request = match decode_request(request_bytes) {
        Ok(request) => request,
        Err(_) => {
            return CAMEL_RUST_INVALID_REQUEST;
        }
    };

    if decoded_request.validate().is_err() {
        return CAMEL_RUST_INVALID_REQUEST;
    }

    if !runtime.invocations.register(invocation_id) {
        return CAMEL_RUST_INVALID_RUNTIME;
    }

    let registry = Arc::clone(&runtime.invocations);
    let user_data = user_data as usize;

    thread::spawn(move || {
        complete_invocation(
            registry,
            invocation_id,
            decoded_request,
            completion_callback,
            user_data,
        );
    });

    CAMEL_RUST_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn camel_rust_cancel(runtime_handle: u64, invocation_id: u64) -> i32 {
    if runtime_handle == 0 {
        return CAMEL_RUST_INVALID_RUNTIME;
    }

    let runtime = unsafe { &*(runtime_handle as *const RustRuntime) };

    runtime.invocations.cancel(invocation_id);

    CAMEL_RUST_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn camel_rust_runtime_destroy(runtime_handle: u64) {
    if runtime_handle == 0 {
        return;
    }

    let runtime = unsafe { &*(runtime_handle as *const RustRuntime) };

    runtime.invocations.begin_destroy();
    runtime.invocations.wait_for_empty();

    unsafe {
        drop(Box::from_raw(runtime_handle as *mut RustRuntime));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    unsafe extern "C" fn test_callback(
        _invocation_id: u64,
        _response: *const u8,
        _response_len: usize,
        user_data: *mut std::ffi::c_void,
    ) {
        let callback_count = unsafe { &*(user_data as *const AtomicUsize) };
        callback_count.fetch_add(1, Ordering::SeqCst);
    }

    fn test_request() -> RustInvocationRequest {
        RustInvocationRequest {
            version: PROTOCOL_VERSION,
            invocation_id: "test-invocation".to_string(),
            body: Value::Text("hello".to_string()),
            headers: Value::Map(Vec::new()),
            properties: Value::Map(Vec::new()),
        }
    }

    #[test]
    fn encodes_success_response() {
        let response = RustInvocationResponse::success(test_request());

        let encoded = encode_response(&response).expect("response should encode");

        assert!(!encoded.is_empty());
    }

    #[test]
    fn encodes_failure_response() {
        let request = test_request();

        let response = RustInvocationResponse::failure(
            request.invocation_id,
            request.body,
            request.headers,
            "java.lang.Exception",
            "Test failure",
        );

        let encoded = encode_response(&response).expect("response should encode");

        assert!(!encoded.is_empty());
    }

    #[test]
    fn response_round_trip_preserves_success_fields() {
        let response = RustInvocationResponse::success(test_request());

        let encoded = encode_response(&response).expect("response should encode");

        let decoded: RustInvocationResponse =
            ciborium::from_reader(encoded.as_slice()).expect("response should decode");

        assert_eq!(decoded.version, PROTOCOL_VERSION);
        assert_eq!(decoded.invocation_id, "test-invocation");
        assert_eq!(decoded.status, "SUCCESS");
        assert_eq!(decoded.body, Value::Text("hello".to_string()));
        assert!(decoded.error.is_none());
    }

    #[test]
    fn response_round_trip_preserves_failure_fields() {
        let request = test_request();

        let response = RustInvocationResponse::failure(
            request.invocation_id,
            request.body,
            request.headers,
            "java.lang.Exception",
            "Test failure",
        );

        let encoded = encode_response(&response).expect("response should encode");

        let decoded: RustInvocationResponse =
            ciborium::from_reader(encoded.as_slice()).expect("response should decode");

        assert_eq!(decoded.version, PROTOCOL_VERSION);
        assert_eq!(decoded.invocation_id, "test-invocation");
        assert_eq!(decoded.status, "FAILURE");

        let error = decoded
            .error
            .expect("failure response should contain error");

        assert_eq!(error.error_type, "java.lang.Exception");
        assert_eq!(error.message, "Test failure");
    }

    #[test]
    fn active_invocation_is_not_cancelled() {
        let registry = InvocationRegistry::new();
        let invocation_id = 42;

        registry.register(invocation_id);

        assert!(!registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(0)));
    }

    #[test]
    fn cancellation_marks_active_invocation() {
        let registry = InvocationRegistry::new();
        let invocation_id = 42;

        registry.register(invocation_id);
        registry.cancel(invocation_id);

        assert!(registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(0)));
    }

    #[test]
    fn cancellation_of_unknown_invocation_does_not_create_state() {
        let registry = InvocationRegistry::new();

        registry.cancel(999);

        assert!(!registry.wait_for_work_or_cancellation(999, Duration::from_millis(0)));
    }

    #[test]
    fn cancelled_invocation_is_detected() {
        let registry = InvocationRegistry::new();
        let invocation_id = 42;

        registry.register(invocation_id);
        registry.cancel(invocation_id);

        assert!(registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(0)));
    }

    #[test]
    fn completed_invocation_is_removed_from_registry() {
        let registry = InvocationRegistry::new();
        let invocation_id = 42;

        registry.register(invocation_id);
        registry.remove(invocation_id);

        assert!(!registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(1)));

        registry.remove(invocation_id);

        registry.cancel(invocation_id);

        assert!(!registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(1)));

        registry.remove(invocation_id);
        assert!(!registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(0)));
    }

    #[test]
    fn cancellation_wakes_pending_invocation() {
        let registry = Arc::new(InvocationRegistry::new());
        let invocation_id = 42;

        registry.register(invocation_id);

        let worker_registry = Arc::clone(&registry);

        let worker = thread::spawn(move || {
            worker_registry.wait_for_work_or_cancellation(invocation_id, Duration::from_secs(5))
        });

        thread::sleep(Duration::from_millis(10));
        registry.cancel(invocation_id);

        assert!(
            worker.join().expect("worker should finish"),
            "worker should observe cancellation"
        );

        registry.remove(invocation_id);
    }

    #[test]
    fn normal_pending_invocation_completes_without_cancellation() {
        let registry = Arc::new(InvocationRegistry::new());
        let invocation_id = 42;

        registry.register(invocation_id);

        let started = std::time::Instant::now();

        let cancelled =
            registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(10));

        assert!(!cancelled);
        assert!(started.elapsed() >= Duration::from_millis(5));

        registry.remove(invocation_id);
    }

    #[test]
    fn callback_is_invoked_once_for_successful_invocation() {
        let registry = Arc::new(InvocationRegistry::new());
        let callback_count = AtomicUsize::new(0);
        let invocation_id = 42;

        registry.register(invocation_id);

        complete_invocation(
            registry.clone(),
            invocation_id,
            test_request(),
            test_callback,
            &callback_count as *const AtomicUsize as usize,
        );

        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn callback_is_invoked_once_for_cancelled_invocation() {
        let registry = Arc::new(InvocationRegistry::new());
        let callback_count = AtomicUsize::new(0);
        let invocation_id = 42;

        registry.register(invocation_id);
        registry.cancel(invocation_id);

        complete_invocation(
            registry.clone(),
            invocation_id,
            test_request(),
            test_callback,
            &callback_count as *const AtomicUsize as usize,
        );

        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn runtime_can_accept_another_invocation_after_cancellation() {
        let runtime = RustRuntime::new();

        runtime.invocations.register(1);
        runtime.invocations.cancel(1);
        runtime.invocations.remove(1);

        runtime.invocations.register(2);

        assert!(
            !runtime
                .invocations
                .wait_for_work_or_cancellation(2, Duration::from_millis(0))
        );

        runtime.invocations.remove(2);
    }

    #[test]
    fn registry_rejects_new_invocation_after_destroy_begins() {
        let registry = InvocationRegistry::new();

        assert!(registry.register(1));

        registry.begin_destroy();

        assert!(!registry.register(2));

        registry.remove(1);
        registry.wait_for_empty();
    }
}
