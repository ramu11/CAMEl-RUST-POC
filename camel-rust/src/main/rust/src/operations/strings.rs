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

use super::{map_get, text_value};

pub(crate) fn string_processing(body: &Value) -> Result<Value, String> {
    let text = text_value(body)?;

    Ok(Value::Map(vec![
        (Value::Text("original".into()), Value::Text(text.clone())),
        (
            Value::Text("uppercase".into()),
            Value::Text(text.to_uppercase()),
        ),
        (
            Value::Text("length".into()),
            Value::Integer((text.chars().count() as i64).into()),
        ),
    ]))
}

pub(crate) fn option_result(body: &Value) -> Result<Value, String> {
    let Value::Map(_) = body else {
        return Err("option_result expects a CBOR map".to_string());
    };

    let value = map_get(body, "optional");

    match value {
        None | Some(Value::Null) => Ok(Value::Map(vec![
            (Value::Text("present".into()), Value::Bool(false)),
            (Value::Text("value".into()), Value::Null),
        ])),

        Some(Value::Text(value)) => Ok(Value::Map(vec![
            (Value::Text("present".into()), Value::Bool(true)),
            (Value::Text("value".into()), Value::Text(value.clone())),
        ])),

        Some(_) => Err("option_result expects 'optional' to be text or null".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_processing_returns_original_uppercase_and_length() {
        let body = Value::Text("Hello Rust".to_string());

        let result = string_processing(&body).expect("string processing should succeed");

        assert_eq!(
            result,
            Value::Map(vec![
                (
                    Value::Text("original".into()),
                    Value::Text("Hello Rust".into())
                ),
                (
                    Value::Text("uppercase".into()),
                    Value::Text("HELLO RUST".into())
                ),
                (Value::Text("length".into()), Value::Integer(10.into())),
            ])
        );
    }

    #[test]
    fn string_processing_counts_unicode_characters() {
        let body = Value::Text("héllo 世界".to_string());

        let result = string_processing(&body).expect("string processing should succeed");

        assert_eq!(
            result,
            Value::Map(vec![
                (
                    Value::Text("original".into()),
                    Value::Text("héllo 世界".into())
                ),
                (
                    Value::Text("uppercase".into()),
                    Value::Text("HÉLLO 世界".into())
                ),
                (Value::Text("length".into()), Value::Integer(8.into())),
            ])
        );
    }

    #[test]
    fn string_processing_rejects_non_text() {
        let body = Value::Integer(42.into());

        let error = string_processing(&body).expect_err("string processing should fail");

        assert_eq!(error, "Expected a CBOR text value");
    }

    #[test]
    fn option_result_returns_present_text() {
        let body = Value::Map(vec![(
            Value::Text("optional".into()),
            Value::Text("hello".into()),
        )]);

        let result = option_result(&body).expect("option result should succeed");

        assert_eq!(
            result,
            Value::Map(vec![
                (Value::Text("present".into()), Value::Bool(true)),
                (Value::Text("value".into()), Value::Text("hello".into())),
            ])
        );
    }

    #[test]
    fn option_result_returns_absent_for_null() {
        let body = Value::Map(vec![(Value::Text("optional".into()), Value::Null)]);

        let result = option_result(&body).expect("option result should succeed");

        assert_eq!(
            result,
            Value::Map(vec![
                (Value::Text("present".into()), Value::Bool(false)),
                (Value::Text("value".into()), Value::Null),
            ])
        );
    }

    #[test]
    fn option_result_returns_absent_when_field_is_missing() {
        let body = Value::Map(Vec::new());

        let result = option_result(&body).expect("option result should succeed");

        assert_eq!(
            result,
            Value::Map(vec![
                (Value::Text("present".into()), Value::Bool(false)),
                (Value::Text("value".into()), Value::Null),
            ])
        );
    }

    #[test]
    fn option_result_rejects_non_text_optional_value() {
        let body = Value::Map(vec![(
            Value::Text("optional".into()),
            Value::Integer(42.into()),
        )]);

        let error = option_result(&body).expect_err("option result should fail");

        assert_eq!(error, "option_result expects 'optional' to be text or null");
    }

    #[test]
    fn option_result_rejects_non_map() {
        let body = Value::Text("not a map".into());

        let error = option_result(&body).expect_err("option result should fail");

        assert_eq!(error, "option_result expects a CBOR map");
    }
}
