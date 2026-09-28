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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustAsyncExecutionTest {

    private CamelContext context;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.start();
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void testAsyncRuntimeCompletesLater() throws Exception {
        AsyncRuntime asyncRuntime = new AsyncRuntime(100);
        asyncRuntime.start();

        RustProcessor processor = new RustProcessor("asyncProc", asyncRuntime);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello async");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean doneSyncValue = new AtomicBoolean(true);
        AtomicInteger callbackCount = new AtomicInteger(0);

        boolean syncResult = processor.process(exchange, doneSync -> {
            doneSyncValue.set(doneSync);
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        // process() must return false to signify async processing
        assertFalse(syncResult, "process() must return false for async offloaded work");
        assertEquals(0, callbackCount.get(), "Callback must not be invoked synchronously");

        boolean completedInTime = latch.await(2, TimeUnit.SECONDS);
        assertTrue(completedInTime, "Async work must complete within timeout");

        assertEquals(1, callbackCount.get(), "AsyncCallback must be invoked exactly once");
        assertFalse(doneSyncValue.get(), "AsyncCallback doneSync parameter must be false");
        assertEquals("HELLO ASYNC", exchange.getIn().getBody(), "Exchange body must be updated by async worker");

        asyncRuntime.stop();
    }

    @Test
    void testAsyncExecutionFailure() throws Exception {
        AsyncRuntime asyncRuntime = new AsyncRuntime(50, true);
        asyncRuntime.start();

        RustProcessor processor = new RustProcessor("faultyAsyncProc", asyncRuntime);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello fail");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        boolean syncResult = processor.process(exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        assertFalse(syncResult);
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        assertEquals(1, callbackCount.get());

        // D9.1: asynchronous runtime failures are normalized at the
        // Camel/Rust execution boundary.
        RustExecutionException exception = assertInstanceOf(RustExecutionException.class, exchange.getException());

        assertEquals(
                "Rust execution failed for invocation " + exception.getInvocationId(),
                exception.getMessage());

        assertInstanceOf(IllegalArgumentException.class, exception.getCause());
        assertEquals("Simulated async native error", exception.getCause().getMessage());

        asyncRuntime.stop();
    }

    @Test
    void testAsyncCompletionRaceArbitration() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        RustInvocation invocation = new RustInvocation("inv-async-race", exchange, doneSync -> {
            callbackCount.incrementAndGet();
            callbackLatch.countDown();
        });

        // Simulate 2 background threads racing to complete the invocation
        Thread workerA = new Thread(() -> invocation.complete(false));
        Thread workerB = new Thread(() -> invocation.complete(false));

        workerA.start();
        workerB.start();

        workerA.join();
        workerB.join();

        assertTrue(callbackLatch.await(1, TimeUnit.SECONDS));
        assertEquals(1, callbackCount.get(), "Only single winning thread triggers callback");
        assertTrue(invocation.isCompleted());
        assertFalse(invocation.getContext().isActive(), "Exchange access revoked after completion");
    }
}
