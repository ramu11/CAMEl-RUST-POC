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

pub(crate) const PROTOCOL_VERSION: u64 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RustInvocationRequest {
    version: u64,

    #[serde(rename = "invocationId")]
    pub(crate) invocation_id: String,

    pub(crate) operation: String,

    pub(crate) body: Value,

    pub(crate) headers: Value,

    pub(crate) properties: Value,
}

impl RustInvocationRequest {
    #[cfg(test)]
    pub(crate) fn new_for_test(invocation_id: String, operation: String, body: Value) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            invocation_id,
            operation,
            body,
            headers: Value::Map(Vec::new()),
            properties: Value::Map(Vec::new()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.version != PROTOCOL_VERSION {
            return Err("Unsupported protocol version");
        }

        if self.invocation_id.trim().is_empty() {
            return Err("invocationId must not be blank");
        }

        if self.operation.trim().is_empty() {
            return Err("operation must not be blank");
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
pub(crate) struct RustInvocationResponse {
    version: u64,

    #[serde(rename = "invocationId")]
    pub(crate) invocation_id: String,

    pub(crate) status: String,

    pub(crate) body: Value,

    pub(crate) headers: Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<RustInvocationError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RustInvocationError {
    pub(crate) code: String,

    pub(crate) message: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) detail: Option<String>,
}

impl RustInvocationResponse {
    pub(crate) fn success(request: RustInvocationRequest, body: Value) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            invocation_id: request.invocation_id,
            status: "SUCCESS".to_string(),
            body,
            headers: request.headers,
            error: None,
        }
    }

    pub(crate) fn failure(
        invocation_id: String,
        body: Value,
        headers: Value,
        code: impl Into<String>,
        message: impl Into<String>,
        detail: Option<String>,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            invocation_id,
            status: "FAILURE".to_string(),
            body,
            headers,
            error: Some(RustInvocationError {
                code: code.into(),
                message: message.into(),
                detail,
            }),
        }
    }
}

pub(crate) fn decode_request(request: &[u8]) -> Result<RustInvocationRequest, String> {
    let request: RustInvocationRequest =
        ciborium::from_reader(request).map_err(|error| error.to_string())?;

    request.validate().map_err(str::to_string)?;

    Ok(request)
}

pub(crate) fn encode_response(response: &RustInvocationResponse) -> Result<Vec<u8>, String> {
    let mut encoded = Vec::new();

    ciborium::into_writer(response, &mut encoded).map_err(|error| error.to_string())?;

    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_request() -> RustInvocationRequest {
        RustInvocationRequest {
            version: PROTOCOL_VERSION,
            invocation_id: "test-invocation".to_string(),
            operation: "test-operation".to_string(),
            body: Value::Text("hello".to_string()),
            headers: Value::Map(Vec::new()),
            properties: Value::Map(Vec::new()),
        }
    }

    #[test]
    fn encodes_success_response() {
        let response =
            RustInvocationResponse::success(test_request(), Value::Text("hello".to_string()));

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
            "RUST_EXECUTION_FAILED",
            "Test failure",
            Some("test detail".to_string()),
        );

        let encoded = encode_response(&response).expect("response should encode");

        assert!(!encoded.is_empty());
    }

    #[test]
    fn response_round_trip_preserves_success_fields() {
        let response =
            RustInvocationResponse::success(test_request(), Value::Text("hello".to_string()));

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
            "RUST_EXECUTION_FAILED",
            "Test failure",
            Some("test detail".to_string()),
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

        assert_eq!(error.code, "RUST_EXECUTION_FAILED");
        assert_eq!(error.message, "Test failure");
        assert_eq!(error.detail.as_deref(), Some("test detail"));
    }

    #[test]
    fn request_validation_requires_operation() {
        let mut request = test_request();

        request.operation.clear();

        assert_eq!(request.validate(), Err("operation must not be blank"));
    }

    #[test]
    fn request_validation_rejects_blank_operation() {
        let mut request = test_request();

        request.operation = "   ".to_string();

        assert_eq!(request.validate(), Err("operation must not be blank"));
    }

    #[test]
    fn request_validation_requires_headers_map() {
        let mut request = test_request();

        request.headers = Value::Array(Vec::new());

        assert_eq!(request.validate(), Err("headers must be a CBOR map"));
    }

    #[test]
    fn request_validation_requires_properties_map() {
        let mut request = test_request();

        request.properties = Value::Array(Vec::new());

        assert_eq!(request.validate(), Err("properties must be a CBOR map"));
    }

    #[test]
    fn request_validation_requires_invocation_id() {
        let mut request = test_request();

        request.invocation_id.clear();

        assert_eq!(request.validate(), Err("invocationId must not be blank"));
    }

    #[test]
    fn request_validation_rejects_blank_invocation_id() {
        let mut request = test_request();

        request.invocation_id = "   ".to_string();

        assert_eq!(request.validate(), Err("invocationId must not be blank"));
    }

    #[test]
    fn request_validation_requires_supported_protocol_version() {
        let mut request = test_request();

        request.version = PROTOCOL_VERSION + 1;

        assert_eq!(request.validate(), Err("Unsupported protocol version"));
    }

    #[test]
    fn request_round_trip_preserves_fields() {
        let request = test_request();

        let mut encoded = Vec::new();

        ciborium::into_writer(&request, &mut encoded).expect("request should encode");

        let decoded = decode_request(&encoded).expect("request should decode");

        assert_eq!(decoded.invocation_id, "test-invocation");
        assert_eq!(decoded.operation, "test-operation");
        assert_eq!(decoded.body, Value::Text("hello".to_string()));
        assert_eq!(decoded.headers, Value::Map(Vec::new()));
        assert_eq!(decoded.properties, Value::Map(Vec::new()));
        assert_eq!(decoded.validate(), Ok(()));
    }

    #[test]
    fn decode_request_rejects_invalid_cbor() {
        let result = decode_request(&[0xff, 0xff, 0xff]);

        assert!(result.is_err());
    }

    #[test]
    fn decode_request_rejects_invalid_protocol_version() {
        let mut request = test_request();
        request.version = PROTOCOL_VERSION + 1;

        let mut encoded = Vec::new();
        ciborium::into_writer(&request, &mut encoded).expect("request should encode");

        let error = decode_request(&encoded).expect_err("request should be rejected");

        assert_eq!(error, "Unsupported protocol version");
    }

    #[test]
    fn decode_request_rejects_invalid_headers() {
        let mut request = test_request();
        request.headers = Value::Array(Vec::new());

        let mut encoded = Vec::new();
        ciborium::into_writer(&request, &mut encoded).expect("request should encode");

        let error = decode_request(&encoded).expect_err("request should be rejected");

        assert_eq!(error, "headers must be a CBOR map");
    }

    #[test]
    fn decode_request_preserves_unicode_fields() {
        let mut request = test_request();

        request.invocation_id = "invocation-世界".to_string();
        request.operation = "string_processing".to_string();
        request.body = Value::Text("héllo 世界 🌍".to_string());

        let mut encoded = Vec::new();
        ciborium::into_writer(&request, &mut encoded).expect("request should encode");

        let decoded = decode_request(&encoded).expect("request should decode");

        assert_eq!(decoded.invocation_id, "invocation-世界");
        assert_eq!(decoded.body, Value::Text("héllo 世界 🌍".to_string()));
    }
}
