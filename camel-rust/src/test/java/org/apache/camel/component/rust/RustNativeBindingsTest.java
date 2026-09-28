/*
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

package org.apache.camel.component.rust;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RustNativeBindingsTest {

    private static final Path NATIVE_LIBRARY = Path.of(
            "src/main/rust/target/release/libcamel_rust.so");

    @Test
    void shouldRoundTripThroughNativeRustCallback() throws Exception {
        assertTrue(
                NATIVE_LIBRARY.toFile().isFile(),
                "Native Rust library must exist: " + NATIVE_LIBRARY.toAbsolutePath());

        try (RustNativeBindings bindings = new RustNativeBindings(NATIVE_LIBRARY.toAbsolutePath().toString())) {

            long runtimeHandle = bindings.create();

            try {
                RustInvocationRequest request = new RustInvocationRequest(
                        "native-test-invocation",
                        "hello",
                        java.util.Map.of(),
                        java.util.Map.of());

                byte[] encodedRequest = new CborRustPayloadCodec().encode(request);

                CountDownLatch completion = new CountDownLatch(1);
                AtomicReference<RustInvocationResponse> response = new AtomicReference<>();

                bindings.execute(
                        runtimeHandle,
                        request.invocationId(),
                        encodedRequest,
                        result -> {
                            response.set(result);
                            completion.countDown();
                        });

                assertTrue(
                        completion.await(5, TimeUnit.SECONDS),
                        "Native Rust completion callback was not invoked");

                RustInvocationResponse actual = response.get();

                assertNotNull(actual);
                assertEquals(
                        RustInvocationResponse.Status.SUCCESS,
                        actual.status());
                assertEquals("hello", actual.body());
                assertNotNull(actual.headers());
                assertNull(actual.error());
            } finally {
                bindings.destroy(runtimeHandle);
            }
        }
    }

    @Test
    void shouldCancelNativeInvocation() throws Exception {
        assertTrue(
                NATIVE_LIBRARY.toFile().isFile(),
                "Native Rust library must exist: " + NATIVE_LIBRARY.toAbsolutePath());

        try (RustNativeBindings bindings = new RustNativeBindings(NATIVE_LIBRARY.toAbsolutePath().toString())) {

            long runtimeHandle = bindings.create();

            try {
                RustInvocationRequest request = new RustInvocationRequest(
                        "native-cancel-invocation",
                        "hello",
                        java.util.Map.of(),
                        java.util.Map.of());

                byte[] encodedRequest = new CborRustPayloadCodec().encode(request);

                CountDownLatch completion = new CountDownLatch(1);
                AtomicReference<RustInvocationResponse> response = new AtomicReference<>();
                java.util.concurrent.atomic.AtomicInteger callbackCount = new java.util.concurrent.atomic.AtomicInteger();

                bindings.execute(
                        runtimeHandle,
                        request.invocationId(),
                        encodedRequest,
                        result -> {
                            callbackCount.incrementAndGet();
                            response.set(result);
                            completion.countDown();
                        });

                bindings.cancel(runtimeHandle, request.invocationId());

                assertTrue(
                        completion.await(5, TimeUnit.SECONDS),
                        "Native Rust cancellation callback was not invoked");

                RustInvocationResponse actual = response.get();

                assertNotNull(actual);
                assertEquals(
                        RustInvocationResponse.Status.FAILURE,
                        actual.status());
                assertNotNull(actual.error());
                assertEquals(1, callbackCount.get());
            } finally {
                bindings.destroy(runtimeHandle);
            }
        }
    }

    @Test
    void shouldSafelyDestroyRuntimeWithInFlightInvocation() throws Exception {
        assertTrue(
                NATIVE_LIBRARY.toFile().isFile(),
                "Native Rust library must exist: " + NATIVE_LIBRARY.toAbsolutePath());

        try (RustNativeBindings bindings = new RustNativeBindings(NATIVE_LIBRARY.toAbsolutePath().toString())) {

            long runtimeHandle = bindings.create();

            RustInvocationRequest request = new RustInvocationRequest(
                    "native-destroy-in-flight",
                    "hello",
                    java.util.Map.of(),
                    java.util.Map.of());

            byte[] encodedRequest = new CborRustPayloadCodec().encode(request);

            CountDownLatch completion = new CountDownLatch(1);
            AtomicReference<RustInvocationResponse> response = new AtomicReference<>();
            java.util.concurrent.atomic.AtomicInteger callbackCount = new java.util.concurrent.atomic.AtomicInteger();

            bindings.execute(
                    runtimeHandle,
                    request.invocationId(),
                    encodedRequest,
                    result -> {
                        callbackCount.incrementAndGet();
                        response.set(result);
                        completion.countDown();
                    });

            bindings.destroy(runtimeHandle);

            assertTrue(
                    completion.await(5, TimeUnit.SECONDS),
                    "In-flight native invocation did not complete after runtime destruction");

            RustInvocationResponse actual = response.get();

            assertNotNull(actual);
            assertEquals(1, callbackCount.get());
        }
    }

    @Test
    void shouldRejectExecutionAfterRuntimeDestroy() throws Exception {
        assertTrue(
                NATIVE_LIBRARY.toFile().isFile(),
                "Native Rust library must exist: " + NATIVE_LIBRARY.toAbsolutePath());

        try (RustNativeBindings bindings = new RustNativeBindings(NATIVE_LIBRARY.toAbsolutePath().toString())) {

            long runtimeHandle = bindings.create();

            bindings.destroy(runtimeHandle);

            RustInvocationRequest request = new RustInvocationRequest(
                    "native-execute-after-destroy",
                    "hello",
                    java.util.Map.of(),
                    java.util.Map.of());

            byte[] encodedRequest = new CborRustPayloadCodec().encode(request);

            IllegalStateException exception = assertThrows(
                    IllegalStateException.class,
                    () -> bindings.execute(
                            runtimeHandle,
                            request.invocationId(),
                            encodedRequest,
                            result -> {
                            }));

            assertTrue(exception.getMessage().contains("already destroyed"));
        }
    }

    @Test
    void shouldRejectCancellationAfterRuntimeDestroy() throws Exception {
        assertTrue(
                NATIVE_LIBRARY.toFile().isFile(),
                "Native Rust library must exist: " + NATIVE_LIBRARY.toAbsolutePath());

        try (RustNativeBindings bindings = new RustNativeBindings(NATIVE_LIBRARY.toAbsolutePath().toString())) {

            long runtimeHandle = bindings.create();

            bindings.destroy(runtimeHandle);

            IllegalStateException exception = assertThrows(
                    IllegalStateException.class,
                    () -> bindings.cancel(
                            runtimeHandle,
                            "native-cancel-after-destroy"));

            assertTrue(exception.getMessage().contains("already destroyed"));
        }
    }

    @Test
    void shouldAllowDestroyingRuntimeOnlyOnce() throws Exception {
        assertTrue(
                NATIVE_LIBRARY.toFile().isFile(),
                "Native Rust library must exist: " + NATIVE_LIBRARY.toAbsolutePath());

        try (RustNativeBindings bindings = new RustNativeBindings(NATIVE_LIBRARY.toAbsolutePath().toString())) {

            long runtimeHandle = bindings.create();

            bindings.destroy(runtimeHandle);
            bindings.destroy(runtimeHandle);
        }
    }
}
