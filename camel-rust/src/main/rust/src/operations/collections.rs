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
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use super::{array_value, integer_value, map_value, text_key_map, text_value};

pub(crate) fn vec_transform(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let transformed = values
        .into_iter()
        .map(|value| {
            let number = integer_value(&value)?;
            let transformed = number
                .checked_mul(2)
                .ok_or_else(|| "Integer overflow in vec_transform".to_string())?;

            Ok(Value::Integer(transformed.into()))
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(Value::Array(transformed))
}

pub(crate) fn vec_filter_map(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let transformed = values
        .into_iter()
        .filter_map(|value| {
            let number = integer_value(&value).ok()?;

            if number % 2 == 0 {
                number
                    .checked_mul(10)
                    .map(|value| Value::Integer(value.into()))
            } else {
                None
            }
        })
        .collect();

    Ok(Value::Array(transformed))
}

pub(crate) fn hashmap_transform(body: &Value) -> Result<Value, String> {
    let entries = text_key_map(body)?;

    let values = entries
        .into_iter()
        .map(|(key, value)| {
            let number = integer_value(&value)?;
            let transformed = number
                .checked_mul(2)
                .ok_or_else(|| "Integer overflow in hashmap_transform".to_string())?;

            Ok((key, transformed))
        })
        .collect::<Result<HashMap<_, _>, String>>()?;

    let output = values
        .into_iter()
        .map(|(key, value)| (Value::Text(key), Value::Integer(value.into())))
        .collect();

    Ok(Value::Map(output))
}

pub(crate) fn btree_map_transform(body: &Value) -> Result<Value, String> {
    let entries = text_key_map(body)?;

    let values = entries
        .into_iter()
        .map(|(key, value)| {
            let number = integer_value(&value)?;
            let transformed = number
                .checked_mul(2)
                .ok_or_else(|| "Integer overflow in btree_map_transform".to_string())?;

            Ok((key, transformed))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;

    let output = values
        .into_iter()
        .map(|(key, value)| (Value::Text(key), Value::Integer(value.into())))
        .collect();

    Ok(Value::Map(output))
}

pub(crate) fn hash_set_transform(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let numbers = values
        .into_iter()
        .map(|value| integer_value(&value))
        .collect::<Result<Vec<_>, _>>()?;

    let unique: HashSet<i64> = numbers.into_iter().collect();

    let mut output: Vec<i64> = unique.into_iter().collect();
    output.sort_unstable();

    Ok(Value::Array(
        output
            .into_iter()
            .map(|number| Value::Integer(number.into()))
            .collect(),
    ))
}

pub(crate) fn vec_deque_transform(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let mut queue = VecDeque::with_capacity(values.len());

    for value in values {
        queue.push_back(integer_value(&value)?);
    }

    let mut output = Vec::with_capacity(queue.len());

    while let Some(number) = queue.pop_front() {
        let transformed = number
            .checked_mul(2)
            .ok_or_else(|| "Integer overflow in vec_deque_transform".to_string())?;

        output.push(Value::Integer(transformed.into()));
    }

    Ok(Value::Array(output))
}

pub(crate) fn nested_collections(body: &Value) -> Result<Value, String> {
    let groups = array_value(body)?;
    let mut output = Vec::with_capacity(groups.len());

    for group in groups {
        let entries = text_key_map(&group)?;

        let transformed = entries
            .into_iter()
            .map(|(key, value)| {
                let number = integer_value(&value)?;
                let transformed = number
                    .checked_add(100)
                    .ok_or_else(|| "Integer overflow in nested_collections".to_string())?;

                Ok((Value::Text(key), Value::Integer(transformed.into())))
            })
            .collect::<Result<Vec<_>, String>>()?;

        output.push(Value::Map(transformed));
    }

    Ok(Value::Array(output))
}

pub(crate) fn collection_chunks(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let mut numbers = Vec::with_capacity(values.len());

    for value in values {
        numbers.push(integer_value(&value)?);
    }

    const CHUNK_SIZE: usize = 3;

    let chunks = numbers
        .chunks(CHUNK_SIZE)
        .map(|chunk| {
            Value::Array(
                chunk
                    .iter()
                    .map(|number| Value::Integer((*number).into()))
                    .collect(),
            )
        })
        .collect();

    Ok(Value::Array(chunks))
}

pub(crate) fn deterministic_map_order(body: &Value) -> Result<Value, String> {
    let entries = text_key_map(body)?;

    let ordered = entries
        .into_iter()
        .map(|(key, value)| {
            let number = integer_value(&value)?;
            Ok((key, number))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;

    Ok(Value::Array(
        ordered
            .into_iter()
            .map(|(key, value)| Value::Array(vec![Value::Text(key), Value::Integer(value.into())]))
            .collect(),
    ))
}

pub(crate) fn aggregate_numbers(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let numbers = values
        .into_iter()
        .map(|value| integer_value(&value))
        .collect::<Result<Vec<_>, _>>()?;

    let count = numbers.len() as i64;

    let sum = numbers.iter().try_fold(0i64, |sum, number| {
        sum.checked_add(*number)
            .ok_or_else(|| "Integer overflow in aggregate_numbers".to_string())
    })?;

    let average = if numbers.is_empty() {
        Value::Null
    } else {
        Value::Float((sum as f64) / (numbers.len() as f64))
    };

    Ok(Value::Map(vec![
        (Value::Text("count".into()), Value::Integer(count.into())),
        (Value::Text("sum".into()), Value::Integer(sum.into())),
        (Value::Text("average".into()), average),
    ]))
}

pub(crate) fn struct_transform(body: &Value) -> Result<Value, String> {
    let entries = map_value(body)?;

    let mut name = None;
    let mut age = None;

    for (key, value) in entries {
        let key = text_value(&key)?;

        match key.as_str() {
            "name" => {
                name = Some(text_value(&value)?);
            }
            "age" => {
                age = Some(integer_value(&value)?);
            }
            _ => {}
        }
    }

    let name = name.ok_or_else(|| "struct_transform requires 'name'".to_string())?;

    let age = age.ok_or_else(|| "struct_transform requires 'age'".to_string())?;

    let age = age
        .checked_add(1)
        .ok_or_else(|| "Integer overflow in struct_transform".to_string())?;

    Ok(Value::Map(vec![
        (Value::Text("name".into()), Value::Text(name.to_uppercase())),
        (Value::Text("age".into()), Value::Integer(age.into())),
        (Value::Text("active".into()), Value::Bool(true)),
    ]))
}

pub(crate) fn large_collection(body: &Value) -> Result<Value, String> {
    let values = array_value(body)?;

    let transformed = values
        .into_iter()
        .map(|value| {
            let number = integer_value(&value)?;

            Ok(Value::Integer(number.wrapping_mul(2).into()))
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(Value::Array(transformed))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numbers(values: &[i64]) -> Value {
        Value::Array(
            values
                .iter()
                .map(|value| Value::Integer((*value).into()))
                .collect(),
        )
    }

    fn map(entries: &[(&str, i64)]) -> Value {
        Value::Map(
            entries
                .iter()
                .map(|(key, value)| {
                    (
                        Value::Text((*key).to_string()),
                        Value::Integer((*value).into()),
                    )
                })
                .collect(),
        )
    }

    fn map_entries(value: &Value) -> Vec<(String, i64)> {
        let Value::Map(entries) = value else {
            panic!("expected CBOR map");
        };

        entries
            .iter()
            .map(|(key, value)| {
                let key = match key {
                    Value::Text(key) => key.clone(),
                    _ => panic!("expected text map key"),
                };

                let value = match value {
                    Value::Integer(value) => (*value).try_into().unwrap(),
                    _ => panic!("expected integer map value"),
                };

                (key, value)
            })
            .collect()
    }

    #[test]
    fn test_vec_transform() {
        let result = vec_transform(&numbers(&[1, 2, 3])).unwrap();

        assert_eq!(result, numbers(&[2, 4, 6]));
    }

    #[test]
    fn test_vec_filter_map() {
        let result = vec_filter_map(&numbers(&[1, 2, 3, 4, 5, 6])).unwrap();

        assert_eq!(result, numbers(&[20, 40, 60]));
    }

    #[test]
    fn test_hashmap_transform() {
        let result = hashmap_transform(&map(&[("b", 2), ("a", 1)])).unwrap();

        let mut actual = map_entries(&result);
        actual.sort();

        assert_eq!(actual, vec![("a".to_string(), 2), ("b".to_string(), 4),]);
    }

    #[test]
    fn test_btree_map_transform_is_deterministic() {
        let result = btree_map_transform(&map(&[("z", 3), ("a", 1), ("m", 2)])).unwrap();

        assert_eq!(
            result,
            Value::Map(vec![
                (Value::Text("a".into()), Value::Integer(2.into())),
                (Value::Text("m".into()), Value::Integer(4.into())),
                (Value::Text("z".into()), Value::Integer(6.into())),
            ])
        );
    }

    #[test]
    fn test_hash_set_transform_removes_duplicates_and_sorts() {
        let result = hash_set_transform(&numbers(&[3, 1, 3, 2, 1])).unwrap();

        assert_eq!(result, numbers(&[1, 2, 3]));
    }

    #[test]
    fn test_vec_deque_transform() {
        let result = vec_deque_transform(&numbers(&[1, 2, 3])).unwrap();

        assert_eq!(result, numbers(&[2, 4, 6]));
    }

    #[test]
    fn test_nested_collections() {
        let body = Value::Array(vec![map(&[("value", 1)]), map(&[("value", 2)])]);

        let result = nested_collections(&body).unwrap();

        assert_eq!(
            result,
            Value::Array(vec![map(&[("value", 101)]), map(&[("value", 102)]),])
        );
    }

    #[test]
    fn test_collection_chunks() {
        let result = collection_chunks(&numbers(&[1, 2, 3, 4, 5, 6, 7])).unwrap();

        assert_eq!(
            result,
            Value::Array(vec![
                numbers(&[1, 2, 3]),
                numbers(&[4, 5, 6]),
                numbers(&[7]),
            ])
        );
    }

    #[test]
    fn test_deterministic_map_order() {
        let result = deterministic_map_order(&map(&[("z", 3), ("a", 1), ("m", 2)])).unwrap();

        assert_eq!(
            result,
            Value::Array(vec![
                Value::Array(vec![Value::Text("a".into()), Value::Integer(1.into()),]),
                Value::Array(vec![Value::Text("m".into()), Value::Integer(2.into()),]),
                Value::Array(vec![Value::Text("z".into()), Value::Integer(3.into()),]),
            ])
        );
    }

    #[test]
    fn test_aggregate_numbers() {
        let result = aggregate_numbers(&numbers(&[10, 20, 30])).unwrap();

        assert_eq!(
            result,
            Value::Map(vec![
                (Value::Text("count".into()), Value::Integer(3.into())),
                (Value::Text("sum".into()), Value::Integer(60.into())),
                (Value::Text("average".into()), Value::Float(20.0)),
            ])
        );
    }

    #[test]
    fn test_aggregate_empty_numbers() {
        let result = aggregate_numbers(&numbers(&[])).unwrap();

        assert_eq!(
            result,
            Value::Map(vec![
                (Value::Text("count".into()), Value::Integer(0.into())),
                (Value::Text("sum".into()), Value::Integer(0.into())),
                (Value::Text("average".into()), Value::Null),
            ])
        );
    }

    #[test]
    fn test_struct_transform() {
        let body = Value::Map(vec![
            (Value::Text("name".into()), Value::Text("alice".into())),
            (Value::Text("age".into()), Value::Integer(30.into())),
        ]);

        let result = struct_transform(&body).unwrap();

        assert_eq!(
            result,
            Value::Map(vec![
                (Value::Text("name".into()), Value::Text("ALICE".into())),
                (Value::Text("age".into()), Value::Integer(31.into())),
                (Value::Text("active".into()), Value::Bool(true)),
            ])
        );
    }

    #[test]
    fn test_large_collection() {
        let result = large_collection(&numbers(&[1, 2, 3, 4])).unwrap();

        assert_eq!(result, numbers(&[2, 4, 6, 8]));
    }

    #[test]
    fn test_unicode_collection_keys() {
        let body = Value::Map(vec![
            (Value::Text("東京".into()), Value::Integer(10.into())),
            (Value::Text("café".into()), Value::Integer(20.into())),
        ]);

        let result = deterministic_map_order(&body).unwrap();

        assert_eq!(
            result,
            Value::Array(vec![
                Value::Array(vec![Value::Text("café".into()), Value::Integer(20.into()),]),
                Value::Array(vec![Value::Text("東京".into()), Value::Integer(10.into()),]),
            ])
        );
    }

    #[test]
    fn test_invalid_collection_input() {
        let result = vec_transform(&Value::Text("not-an-array".into()));

        assert_eq!(result, Err("Expected a CBOR array value".to_string()));
    }

    #[test]
    fn test_vec_transform_detects_overflow() {
        let result = vec_transform(&numbers(&[i64::MAX]));

        assert_eq!(result, Err("Integer overflow in vec_transform".to_string()));
    }

    #[test]
    fn test_struct_transform_detects_overflow() {
        let body = Value::Map(vec![
            (Value::Text("name".into()), Value::Text("alice".into())),
            (Value::Text("age".into()), Value::Integer(i64::MAX.into())),
        ]);

        let result = struct_transform(&body);

        assert_eq!(
            result,
            Err("Integer overflow in struct_transform".to_string())
        );
    }
}
