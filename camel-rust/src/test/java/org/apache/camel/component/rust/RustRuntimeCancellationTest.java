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

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustRuntimeCancellationTest {

    private CamelContext context;
    private PendingInvocationRegistry registry;
    private AsyncRuntime asyncRuntime;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.start();

        registry = new PendingInvocationRegistry();
        registry.start();

        asyncRuntime = new AsyncRuntime(500); // 500ms delay to allow explicit cancellation testing
        asyncRuntime.start();
    }

    @AfterEach
    void tearDown() {
        if (asyncRuntime != null) {
            asyncRuntime.stop();
        }
        if (registry != null) {
            registry.stop();
        }
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void testCancellationBeforeScheduledExecution() throws Exception {
        RustProcessor processor = new RustProcessor("async", asyncRuntime, registry);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello cancellation");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        boolean sync = processor.process(exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        assertFalse(sync, "Async execution must return false from process()");
        assertEquals(1, registry.size(), "Invocation must be tracked in registry");

        // Lookup pending invocation from registry
        String invocationId = registry.snapshot().iterator().next().getInvocationId();
        RustInvocation invocation = registry.get(invocationId).orElseThrow();

        // Perform cancellation via runtime
        asyncRuntime.cancel(invocation);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Callback must be invoked upon cancellation completion");
        assertEquals(1, callbackCount.get(), "Callback must fire exactly once");
        assertEquals(0, registry.size(), "Invocation must be automatically removed from registry upon completion");
        assertEquals("hello cancellation", exchange.getIn().getBody(), "Worker payload transformation must NOT have run");
    }

    @Test
    void testCancellationIsIdempotent() throws Exception {
        RustProcessor processor = new RustProcessor("async", asyncRuntime, registry);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("idempotent test");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        processor.process(exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        RustInvocation invocation = registry.snapshot().iterator().next();

        // Issue multiple cancels
        asyncRuntime.cancel(invocation);
        asyncRuntime.cancel(invocation);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(1, callbackCount.get(), "Multiple cancel calls must only trigger one callback");
        assertEquals(0, registry.size());
    }

    @Test
    void testCancellationRacingWithCompletionProducesSingleCallback() throws Exception {
        // Short delay to create a tight race condition
        AsyncRuntime fastRuntime = new AsyncRuntime(10);
        fastRuntime.start();

        RustProcessor processor = new RustProcessor("fastAsync", fastRuntime, registry);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("race test");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        processor.process(exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        RustInvocation invocation = registry.snapshot().iterator().next();

        // Race thread cancel against worker execution
        Thread cancelThread = new Thread(() -> {
            try {
                fastRuntime.cancel(invocation);
            } catch (Exception ignored) {
            }
        });

        cancelThread.start();
        cancelThread.join();

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(1, callbackCount.get(),
                "Race condition between worker completion and cancel must yield exactly 1 callback");
        assertEquals(0, registry.size(), "Registry must be empty post-race");

        fastRuntime.stop();
    }
}
