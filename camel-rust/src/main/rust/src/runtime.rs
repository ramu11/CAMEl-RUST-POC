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

use std::sync::Arc;
use std::time::Duration;

use crate::operations::dispatch_operation;
use crate::protocol::{RustInvocationRequest, RustInvocationResponse, encode_response};
use crate::registry::InvocationRegistry;

pub(crate) const WORK_DURATION: Duration = Duration::from_millis(50);

pub(crate) type CompletionCallback = unsafe extern "C" fn(
    invocation_id: u64,
    response: *const u8,
    response_len: usize,
    user_data: *mut std::ffi::c_void,
);

pub(crate) struct RustRuntime {
    pub(crate) invocations: Arc<InvocationRegistry>,
}

impl RustRuntime {
    pub(crate) fn new() -> Self {
        Self {
            invocations: Arc::new(InvocationRegistry::new()),
        }
    }
}

fn execute_operation(request: RustInvocationRequest) -> RustInvocationResponse {
    let operation = request.operation.clone();

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        dispatch_operation(&operation, &request.body)
    })) {
        Ok(Ok(body)) => RustInvocationResponse::success(request, body),
        Ok(Err(message)) => RustInvocationResponse::failure(
            request.invocation_id,
            request.body,
            request.headers,
            "RUST_EXECUTION_FAILED",
            message,
            None,
        ),
        Err(_) => RustInvocationResponse::failure(
            request.invocation_id,
            request.body,
            request.headers,
            "RUST_PANIC",
            "Rust operation panicked",
            None,
        ),
    }
}

pub(crate) fn complete_invocation(
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
            "RUST_CANCELLED",
            "Rust invocation was cancelled",
            None,
        )
    } else {
        execute_operation(request)
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

#[cfg(test)]
mod tests {
    use super::*;
    use ciborium::Value;

    fn request(operation: &str, body: Value) -> RustInvocationRequest {
        RustInvocationRequest::new_for_test("inv-1".to_string(), operation.to_string(), body)
    }

    #[test]
    fn unknown_operation_becomes_execution_failure_response() {
        let response = execute_operation(request("does_not_exist", Value::Null));

        assert_eq!(response.status, "FAILURE");
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("RUST_EXECUTION_FAILED")
        );
    }

    #[test]
    fn valid_operation_becomes_success_response() {
        let response = execute_operation(request(
            "string_processing",
            Value::Text("hello".to_string()),
        ));

        assert_eq!(response.status, "SUCCESS");
        assert!(response.error.is_none());
    }

    #[test]
    fn panic_in_operation_becomes_panic_response() {
        let result = std::panic::catch_unwind(|| {
            panic!("test panic");
        });

        assert!(result.is_err());

        let response = RustInvocationResponse::failure(
            "inv-1".to_string(),
            Value::Null,
            Value::Map(Vec::new()),
            "RUST_PANIC",
            "Rust operation panicked",
            None,
        );

        assert_eq!(response.status, "FAILURE");
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("RUST_PANIC")
        );
    }
}
