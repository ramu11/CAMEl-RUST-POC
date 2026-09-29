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

pub(crate) mod collections;
pub(crate) mod concurrency;
pub(crate) mod strings;
pub(crate) mod structs;

pub(crate) fn text_value(value: &Value) -> Result<String, String> {
    match value {
        Value::Text(value) => Ok(value.clone()),
        _ => Err("Expected a CBOR text value".to_string()),
    }
}

pub(crate) fn integer_value(value: &Value) -> Result<i64, String> {
    match value {
        Value::Integer(value) => (*value)
            .try_into()
            .map_err(|_| "Expected a signed integer value".to_string()),
        _ => Err("Expected a CBOR integer value".to_string()),
    }
}

pub(crate) fn array_value(value: &Value) -> Result<Vec<Value>, String> {
    match value {
        Value::Array(value) => Ok(value.clone()),
        _ => Err("Expected a CBOR array value".to_string()),
    }
}

pub(crate) fn map_value(value: &Value) -> Result<Vec<(Value, Value)>, String> {
    match value {
        Value::Map(value) => Ok(value.clone()),
        _ => Err("Expected a CBOR map value".to_string()),
    }
}

pub(crate) fn text_key_map(value: &Value) -> Result<Vec<(String, Value)>, String> {
    map_value(value)?
        .into_iter()
        .map(|(key, value)| Ok((text_value(&key)?, value)))
        .collect()
}

pub(crate) fn map_get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    let Value::Map(entries) = value else {
        return None;
    };

    entries
        .iter()
        .find_map(|(entry_key, value)| match entry_key {
            Value::Text(entry_key) if entry_key == key => Some(value),
            _ => None,
        })
}

pub(crate) fn dispatch_operation(operation: &str, body: &Value) -> Result<Value, String> {
    match operation {
        "echo" => Ok(body.clone()),

        "struct_transform" => collections::struct_transform(body),

        "vec_transform" => collections::vec_transform(body),

        "vec_filter_map" => collections::vec_filter_map(body),

        "hashmap_transform" => collections::hashmap_transform(body),

        "btree_map_transform" => collections::btree_map_transform(body),

        "hash_set_transform" => collections::hash_set_transform(body),

        "vec_deque_transform" => collections::vec_deque_transform(body),

        "nested_collections" => collections::nested_collections(body),

        "collection_chunks" => collections::collection_chunks(body),

        "deterministic_map_order" => collections::deterministic_map_order(body),

        "aggregate_numbers" => collections::aggregate_numbers(body),

        "string_processing" => strings::string_processing(body),

        "option_result" => strings::option_result(body),

        "tuple_transform" => structs::tuple_transform(body),

        "parallel_transform" => concurrency::parallel_transform(body),

        "shared_counter" => concurrency::shared_counter(body),

        "channel_pipeline" => concurrency::channel_pipeline(body),

        "large_collection" => collections::large_collection(body),

        _ => Err(format!("Rust operation is not implemented: {}", operation)),
    }
}
