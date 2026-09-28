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

        RustInvocation invocation = new RustInvocation("inv-d7-1", exchange, doneSync -> latch.countDown());

        // 1. Register invocation
        registry.register(invocation);
        assertEquals(1, registry.size(), "Registry must contain 1 pending invocation");
        assertTrue(registry.get("inv-d7-1").isPresent());

        // 2. Complete invocation and unregister
        boolean completed = invocation.complete(false);
        assertTrue(completed);

        registry.unregister("inv-d7-1");

        // 3. Assert safe de-registration and no orphaned invocations
        assertTrue(registry.isEmpty(), "Registry must be empty post-completion removal");
        assertFalse(registry.get("inv-d7-1").isPresent());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    void testAsyncCompletionAutomaticallyRemovesPendingInvocation() throws Exception {
        AsyncRuntime runtime = new AsyncRuntime(100);
        runtime.start();

        RustProcessor processor = new RustProcessor("async", runtime, registry);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello");

        CountDownLatch latch = new CountDownLatch(1);

        boolean sync = processor.process(exchange, doneSync -> latch.countDown());

        assertFalse(sync, "Async process() call must return false");
        assertEquals(1, registry.size(), "Registry must hold pending invocation while executing");

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Async work must complete within timeout");
        assertEquals(0, registry.size(), "Async completion must automatically remove pending invocation");

        runtime.stop();
    }

    @Test
    void testSynchronousCompletionDoesNotLeaveEntry() throws Exception {
        InProcessRuntime runtime = new InProcessRuntime("uppercaseProcessor");
        runtime.start();

        RustProcessor processor = new RustProcessor("sync", runtime, registry);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello sync");

        CountDownLatch latch = new CountDownLatch(1);

        boolean sync = processor.process(exchange, doneSync -> latch.countDown());

        assertTrue(sync, "Synchronous execution must return true");
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(0, registry.size(), "Synchronous completion must leave registry empty");

        runtime.stop();
    }

    @Test
    void testCompletionRaceRemovesExactlyOnce() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        String invocationId = "inv-race-d7";

        RustInvocation invocation = new RustInvocation(invocationId, exchange, doneSync -> {
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
        assertEquals(1, callbackCount.get(), "Callback must trigger exactly once");
        assertEquals(0, registry.size(), "Race condition must result in single clean unregistration");
    }

    @Test
    void testDuplicateRegistrationIsRejected() {
        RustInvocation first = new RustInvocation("same-id", null, null, null);
        RustInvocation second = new RustInvocation("same-id", null, null, null);

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
        RustInvocation invocation = new RustInvocation("completed-id", null, null, null);

        invocation.complete(true);
        registry.register(invocation);

        assertEquals(0, registry.size());
        assertTrue(registry.get("completed-id").isEmpty());
    }
}
