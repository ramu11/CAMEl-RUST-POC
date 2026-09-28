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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustShutdownDrainingTest {

    private CamelContext context;
    private PendingInvocationRegistry registry;
    private AsyncRuntime asyncRuntime;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.start();

        registry = new PendingInvocationRegistry();
        registry.start();

        asyncRuntime = new AsyncRuntime(1000);
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
    void testShutdownCancelsPendingInvocationsAndEmptiesRegistry() throws Exception {
        RustProcessor processor = new RustProcessor("async", asyncRuntime, registry);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("pending shutdown test");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        boolean sync = processor.process(exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        assertFalse(sync);
        assertEquals(1, registry.size(), "Registry must hold 1 pending invocation");

        RustInvocation pendingInvocation = registry.snapshot().iterator().next();

        asyncRuntime.stop();

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Callback must be notified on shutdown cancellation");
        assertEquals(1, callbackCount.get(), "Callback must execute exactly once");
        assertEquals(0, registry.size(), "Registry must be empty post-shutdown");

        assertThrows(IllegalStateException.class, () -> pendingInvocation.getContext().readBody(),
                "RustInvocationContext capability should be revoked after cancellation");
    }

    @Test
    void testComponentShutdownTriggersRuntimeCancellation() throws Exception {
        RustComponent component = new RustComponent(name -> asyncRuntime);
        component.setCamelContext(context);
        component.start();

        RustProcessor processor = new RustProcessor("asyncProc", asyncRuntime, component.getRegistry());

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("component stop test");

        CountDownLatch latch = new CountDownLatch(1);
        boolean sync = processor.process(exchange, doneSync -> latch.countDown());

        assertFalse(sync);
        assertEquals(1, component.getRegistry().size(), "Component registry must track active invocation");

        RustInvocation pendingInvocation = component.getRegistry().snapshot().iterator().next();

        component.stop();

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(0, component.getRegistry().size(), "Component registry must be fully drained");
        assertFalse(asyncRuntime.hasScheduledTask(pendingInvocation.getInvocationId()),
                "Runtime worker task must be cancelled during component stop");
    }

    @Test
    void testNewExecutionRejectedPostShutdown() throws Exception {
        asyncRuntime.stop();

        RustProcessor processor = new RustProcessor("async", asyncRuntime, registry);
        Exchange exchange = new DefaultExchange(context);

        assertThrows(IllegalStateException.class, () -> processor.process(exchange, doneSync -> {
        }),
                "Executing against stopped runtime must throw IllegalStateException");
    }

    @Test
    void testShutdownDrainsMultiplePendingInvocations() throws Exception {
        RustProcessor processor = new RustProcessor("async", asyncRuntime, registry);

        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger callbackCount = new AtomicInteger();

        for (int i = 0; i < 2; i++) {
            Exchange exchange = new DefaultExchange(context);
            exchange.getIn().setBody("pending-" + i);

            boolean sync = processor.process(exchange, doneSync -> {
                callbackCount.incrementAndGet();
                latch.countDown();
            });

            assertFalse(sync);
        }

        assertEquals(2, registry.size(), "Registry must hold both pending invocations");

        asyncRuntime.stop();

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Both callbacks must be notified on shutdown cancellation");
        assertEquals(2, callbackCount.get(), "Both callbacks must execute exactly once");
        assertEquals(0, registry.size(), "Registry must be empty post-shutdown");
    }
}
