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
import java.util.concurrent.TimeoutException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustTimeoutTest {

    private CamelContext context;
    private AsyncRuntime runtime;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (runtime != null) {
            runtime.stop();
        }
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void testTimeoutRequestsCancellationAndSetsException() throws Exception {
        runtime = new AsyncRuntime(1000);
        runtime.start();

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("timeout payload");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        String invocationId = "inv-timeout-1";
        RustInvocation invocation = new RustInvocation(invocationId, runtime, exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        runtime.execute(invocation, 100);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "Callback must be notified on timeout");
        assertEquals(1, callbackCount.get(), "Callback must fire exactly once");
        assertFalse(runtime.hasScheduledTask(invocationId), "Worker task must be removed from runtime");
        assertFalse(runtime.hasActiveInvocation(invocationId),
                "Active invocation reference must be removed from runtime");

        assertThrows(IllegalStateException.class, () -> invocation.getContext().readBody(),
                "RustInvocationContext capability must be revoked after timeout cancellation");

        assertInstanceOf(TimeoutException.class, exchange.getException(),
                "Exchange must reflect TimeoutException when timeout wins the cancellation race");
    }

    @Test
    void testWorkerCompletesBeforeTimeout() throws Exception {
        runtime = new AsyncRuntime(10);
        runtime.start();

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("worker payload");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        String invocationId = "inv-worker-before-timeout";
        RustInvocation invocation = new RustInvocation(invocationId, runtime, exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        runtime.execute(invocation, 1000);

        assertTrue(latch.await(2, TimeUnit.SECONDS),
                "Callback must be notified when worker completes");

        assertEquals(1, callbackCount.get(),
                "Callback must fire exactly once");

        assertEquals(RustInvocation.LifecycleState.COMPLETED, invocation.getState());

        assertEquals("WORKER PAYLOAD", exchange.getIn().getBody(),
                "Worker must transform the payload before the timeout");

        assertEquals(null, exchange.getException(),
                "Successful worker completion must not set an Exchange exception");

        assertFalse(runtime.hasScheduledTask(invocationId),
                "Runtime task must be cleared after completion");

        assertFalse(runtime.hasActiveInvocation(invocationId),
                "Active invocation reference must be cleared after completion");

        assertThrows(IllegalStateException.class, () -> invocation.getContext().readBody(),
                "Invocation context must be revoked after completion");
    }
}
