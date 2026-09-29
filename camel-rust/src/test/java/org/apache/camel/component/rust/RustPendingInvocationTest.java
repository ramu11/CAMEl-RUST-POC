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
import org.apache.camel.support.service.ServiceSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustPendingInvocationTest {

    private CamelContext context;
    private PendingInvocationRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.start();

        registry = new PendingInvocationRegistry();
        registry.start();
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.stop();
        }
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void testRegistrationCompletionAndRemovalLifecycle() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch latch = new CountDownLatch(1);

        RustInvocation invocation = new RustInvocation(
                "inv-d7-1",
                exchange,
                doneSync -> latch.countDown());

        registry.register(invocation);

        assertEquals(1, registry.size(), "Registry must contain 1 pending invocation");
        assertTrue(registry.get("inv-d7-1").isPresent());

        boolean completed = invocation.complete(false);

        assertTrue(completed);

        registry.unregister("inv-d7-1");

        assertTrue(registry.isEmpty(), "Registry must be empty after completion");
        assertFalse(registry.get("inv-d7-1").isPresent());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    void testRuntimeCompletionRemovesPendingInvocation() throws Exception {
        CountDownLatch executionStarted = new CountDownLatch(1);
        CountDownLatch executionRelease = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(1);

        TestRustRuntime runtime = new TestRustRuntime(
                executionStarted,
                executionRelease);

        Exchange exchange = new DefaultExchange(context);

        String invocationId = "inv-runtime-async";

        RustInvocation invocation = new RustInvocation(
                invocationId,
                "test-operation",
                runtime,
                exchange,
                doneSync -> {
                    registry.unregister(invocationId);
                    completionLatch.countDown();
                });

        registry.register(invocation);

        runtime.start();
        try {
            runtime.execute(invocation);

            assertTrue(
                    executionStarted.await(1, TimeUnit.SECONDS),
                    "Rust runtime must begin asynchronous execution");

            assertEquals(
                    1,
                    registry.size(),
                    "Registry must retain invocation while Rust execution is pending");

            executionRelease.countDown();

            assertTrue(
                    completionLatch.await(2, TimeUnit.SECONDS),
                    "Rust invocation must complete");

            assertEquals(
                    0,
                    registry.size(),
                    "Completed invocation must be removed from the pending registry");
        } finally {
            runtime.stop();
        }
    }

    @Test
    void testCompletionDoesNotLeaveEntry() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch latch = new CountDownLatch(1);

        RustInvocation invocation = new RustInvocation(
                "inv-sync-d7",
                exchange,
                doneSync -> latch.countDown());

        registry.register(invocation);

        assertEquals(
                1,
                registry.size(),
                "Registry must contain pending invocation before completion");

        boolean completed = invocation.complete(true);

        assertTrue(completed);
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        registry.unregister(invocation.getInvocationId());

        assertEquals(
                0,
                registry.size(),
                "Completed invocation must not remain in registry");
    }

    @Test
    void testCompletionRaceRemovesExactlyOnce() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        String invocationId = "inv-race-d7";

        RustInvocation invocation = new RustInvocation(
                invocationId,
                exchange,
                doneSync -> {
                    registry.unregister(invocationId);
                    callbackCount.incrementAndGet();
                    callbackLatch.countDown();
                });

        registry.register(invocation);

        assertEquals(1, registry.size());

        Thread threadA = new Thread(() -> invocation.complete(false));
        Thread threadB = new Thread(() -> invocation.complete(false));

        threadA.start();
        threadB.start();

        threadA.join();
        threadB.join();

        assertTrue(callbackLatch.await(1, TimeUnit.SECONDS));
        assertEquals(
                1,
                callbackCount.get(),
                "Completion callback must execute exactly once");
        assertEquals(
                0,
                registry.size(),
                "Completion race must result in clean unregistration");
    }

    @Test
    void testDuplicateRegistrationIsRejected() {
        RustInvocation first = new RustInvocation(
                "same-id",
                null,
                null,
                null);

        RustInvocation second = new RustInvocation(
                "same-id",
                null,
                null,
                null);

        registry.register(first);

        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> registry.register(second));

        assertTrue(ex.getMessage().contains("RUST_INVOCATION_DUPLICATE"));
        assertEquals(1, registry.size());
        assertSame(first, registry.get("same-id").orElseThrow());
    }

    @Test
    void testCompletedInvocationIsNotRegistered() {
        RustInvocation invocation = new RustInvocation(
                "completed-id",
                null,
                null,
                null);

        invocation.complete(true);
        registry.register(invocation);

        assertEquals(0, registry.size());
        assertTrue(registry.get("completed-id").isEmpty());
    }

    private static final class TestRustRuntime extends ServiceSupport implements RustRuntime {

        private final CountDownLatch executionStarted;
        private final CountDownLatch executionRelease;

        private TestRustRuntime(
                                CountDownLatch executionStarted,
                                CountDownLatch executionRelease) {

            this.executionStarted = executionStarted;
            this.executionRelease = executionRelease;
        }

        @Override
        public void execute(RustInvocation invocation) {
            Thread worker = new Thread(() -> {
                executionStarted.countDown();

                try {
                    executionRelease.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    invocation.setException(e);
                }

                invocation.complete(false);
            });

            worker.start();
        }

        @Override
        public void cancel(RustInvocation invocation) {
            if (invocation != null) {
                invocation.requestCancellation();
                invocation.completeSynchronously();
            }
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }
    }
}
