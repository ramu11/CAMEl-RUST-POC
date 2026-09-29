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
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use super::{array_value, integer_value};

pub(crate) fn parallel_transform(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let numbers = values
        .into_iter()
        .map(|value| integer_value(&value))
        .collect::<Result<Vec<_>, _>>()?;

    let handles = numbers
        .into_iter()
        .map(|number| {
            thread::spawn(move || {
                number
                    .checked_mul(3)
                    .ok_or_else(|| "parallel_transform integer overflow".to_string())
            })
        })
        .collect::<Vec<_>>();

    let mut output = Vec::with_capacity(handles.len());

    for handle in handles {
        let value = handle
            .join()
            .map_err(|_| "parallel_transform worker panicked".to_string())??;

        output.push(Value::Integer(value.into()));
    }

    Ok(Value::Array(output))
}

pub(crate) fn shared_counter(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let numbers = values
        .into_iter()
        .map(|value| integer_value(&value))
        .collect::<Result<Vec<_>, _>>()?;

    let counter = Arc::new(Mutex::new(0_i64));

    let handles = numbers
        .into_iter()
        .map(|number| {
            let counter = Arc::clone(&counter);

            thread::spawn(move || {
                let mut value = counter
                    .lock()
                    .map_err(|_| "shared_counter mutex poisoned".to_string())?;

                *value = value
                    .checked_add(number)
                    .ok_or_else(|| "shared_counter integer overflow".to_string())?;

                Ok::<(), String>(())
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle
            .join()
            .map_err(|_| "shared_counter worker panicked".to_string())??;
    }

    let result = *counter
        .lock()
        .map_err(|_| "shared_counter mutex poisoned".to_string())?;

    Ok(Value::Integer(result.into()))
}

pub(crate) fn channel_pipeline(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let numbers = values
        .into_iter()
        .map(|value| integer_value(&value))
        .collect::<Result<Vec<_>, _>>()?;

    let (sender, receiver) = mpsc::channel();

    let worker = thread::spawn(move || {
        for number in numbers {
            let transformed = match number.checked_mul(4) {
                Some(value) => value,
                None => break,
            };

            if sender.send(transformed).is_err() {
                break;
            }
        }
    });

    let mut output = Vec::new();

    for number in receiver {
        output.push(Value::Integer(number.into()));
    }

    worker
        .join()
        .map_err(|_| "channel_pipeline worker panicked".to_string())?;

    Ok(Value::Array(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integers(values: &[i64]) -> Value {
        Value::Array(
            values
                .iter()
                .map(|value| Value::Integer((*value).into()))
                .collect(),
        )
    }

    #[test]
    fn parallel_transform_multiplies_values() {
        let body = integers(&[1, 2, 3, 4]);

        let result = parallel_transform(&body).expect("parallel transform should succeed");

        assert_eq!(result, integers(&[3, 6, 9, 12]));
    }

    #[test]
    fn parallel_transform_preserves_input_order() {
        let body = integers(&[-3, 0, 7, 11]);

        let result = parallel_transform(&body).expect("parallel transform should succeed");

        assert_eq!(result, integers(&[-9, 0, 21, 33]));
    }

    #[test]
    fn parallel_transform_rejects_invalid_input() {
        let body = Value::Array(vec![
            Value::Integer(1.into()),
            Value::Text("invalid".into()),
        ]);

        let error = parallel_transform(&body).expect_err("parallel transform should fail");

        assert_eq!(error, "Expected a CBOR integer value");
    }

    #[test]
    fn parallel_transform_rejects_overflow() {
        let body = integers(&[i64::MAX]);

        let error = parallel_transform(&body).expect_err("parallel transform should fail");

        assert_eq!(error, "parallel_transform integer overflow");
    }

    #[test]
    fn shared_counter_sums_concurrently() {
        let body = integers(&[1, 2, 3, 4, 5]);

        let result = shared_counter(&body).expect("shared counter should succeed");

        assert_eq!(result, Value::Integer(15.into()));
    }

    #[test]
    fn shared_counter_handles_negative_values() {
        let body = integers(&[100, -25, 7, -2]);

        let result = shared_counter(&body).expect("shared counter should succeed");

        assert_eq!(result, Value::Integer(80.into()));
    }

    #[test]
    fn shared_counter_rejects_overflow() {
        let body = integers(&[i64::MAX, 1]);

        let error = shared_counter(&body).expect_err("shared counter should fail");

        assert_eq!(error, "shared_counter integer overflow");
    }

    #[test]
    fn channel_pipeline_multiplies_values() {
        let body = integers(&[1, 2, 3, 4]);

        let result = channel_pipeline(&body).expect("channel pipeline should succeed");

        assert_eq!(result, integers(&[4, 8, 12, 16]));
    }

    #[test]
    fn channel_pipeline_preserves_send_order() {
        let body = integers(&[-2, 0, 5, 9]);

        let result = channel_pipeline(&body).expect("channel pipeline should succeed");

        assert_eq!(result, integers(&[-8, 0, 20, 36]));
    }

    #[test]
    fn channel_pipeline_rejects_invalid_input() {
        let body = Value::Array(vec![Value::Integer(1.into()), Value::Bool(true)]);

        let error = channel_pipeline(&body).expect_err("channel pipeline should fail");

        assert_eq!(error, "Expected a CBOR integer value");
    }
}
