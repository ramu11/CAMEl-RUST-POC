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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultRustNativeRuntimeTest {

    private static final String NATIVE_LIBRARY = "camel_rust";

    @Test
    void testExecuteCompletesInvocationAndUpdatesExchange() throws Exception {
        RustNativeBindings nativeRuntime = new RustNativeBindings(NATIVE_LIBRARY);

        DefaultCamelContext camelContext = new DefaultCamelContext();
        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody("hello");

        CountDownLatch completion = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();

        RustInvocation invocation = new RustInvocation(
                "test-invocation",
                "test-operation",
                null,
                exchange,
                done -> {
                    callbackCount.incrementAndGet();
                    completion.countDown();
                });

        DefaultRustNativeRuntime runtime = new DefaultRustNativeRuntime(
                nativeRuntime,
                new CborRustPayloadCodec());

        try {
            runtime.start();

            runtime.execute(invocation);

            assertTrue(
                    completion.await(5, TimeUnit.SECONDS),
                    "Native Rust invocation did not complete");

            assertEquals(1, callbackCount.get());
            assertEquals("hello", exchange.getIn().getBody());
            assertEquals(
                    RustInvocation.LifecycleState.COMPLETED,
                    invocation.getState());
        } finally {
            runtime.stop();
            nativeRuntime.close();
        }
    }

    @Test
    void testExecuteRejectsWhenRuntimeIsNotStarted() throws Exception {
        RustNativeBindings nativeRuntime = new RustNativeBindings(NATIVE_LIBRARY);

        DefaultCamelContext camelContext = new DefaultCamelContext();
        Exchange exchange = new DefaultExchange(camelContext);

        RustInvocation invocation = new RustInvocation(
                "test-not-started",
                "test-operation",
                null,
                exchange,
                done -> {
                });

        DefaultRustNativeRuntime runtime = new DefaultRustNativeRuntime(
                nativeRuntime,
                new CborRustPayloadCodec());

        try {
            IllegalStateException exception = org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> runtime.execute(invocation));

            assertTrue(exception.getMessage().contains("RUST_RUNTIME_NOT_STARTED"));
        } finally {
            nativeRuntime.close();
        }
    }
}
