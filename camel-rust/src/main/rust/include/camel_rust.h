/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef CAMEL_RUST_H
#define CAMEL_RUST_H

#include <stddef.h>
#include <stdint.h>

#define CAMEL_RUST_OK 0
#define CAMEL_RUST_INVALID_RUNTIME 1
#define CAMEL_RUST_INVALID_REQUEST 2
#define CAMEL_RUST_INVALID_CALLBACK 3

uint64_t camel_rust_runtime_create(void);

int32_t camel_rust_execute(
        uint64_t runtime_handle,
        uint64_t invocation_id,
        const uint8_t* request,
        size_t request_len,
        void (*completion_callback)(
                uint64_t invocation_id,
                const uint8_t* response,
                size_t response_len,
                void* user_data),
        void* user_data);

int32_t camel_rust_cancel(
        uint64_t runtime_handle,
        uint64_t invocation_id);

void camel_rust_runtime_destroy(uint64_t runtime_handle);

#endif
