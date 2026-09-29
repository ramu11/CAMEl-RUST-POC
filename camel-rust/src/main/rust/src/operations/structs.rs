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

use super::array_value;

pub(crate) fn tuple_transform(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    if values.len() != 2 {
        return Err("tuple_transform expects exactly two values".to_string());
    }

    Ok(Value::Array(vec![values[1].clone(), values[0].clone()]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuple_transform_swaps_two_values() {
        let body = Value::Array(vec![
            Value::Text("first".to_string()),
            Value::Integer(42.into()),
        ]);

        let result = tuple_transform(&body).expect("tuple transform should succeed");

        assert_eq!(
            result,
            Value::Array(vec![
                Value::Integer(42.into()),
                Value::Text("first".to_string()),
            ])
        );
    }

    #[test]
    fn tuple_transform_rejects_wrong_length() {
        let body = Value::Array(vec![Value::Integer(1.into())]);

        let error = tuple_transform(&body).expect_err("tuple transform should fail");

        assert_eq!(error, "tuple_transform expects exactly two values");
    }

    #[test]
    fn tuple_transform_rejects_non_array() {
        let body = Value::Text("not a tuple".to_string());

        let error = tuple_transform(&body).expect_err("tuple transform should fail");

        assert_eq!(error, "Expected a CBOR array value");
    }
}
