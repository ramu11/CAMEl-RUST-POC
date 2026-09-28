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

public class RustCancellationTest {

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
    void testRequestCancellationIsIdempotentAndDoesNotTriggerCallback() {
        Exchange exchange = new DefaultExchange(context);
        AtomicInteger callbackCount = new AtomicInteger(0);

        RustInvocation invocation = new RustInvocation("inv-d8-1", exchange, doneSync -> callbackCount.incrementAndGet());

        // Initial state
        assertEquals(RustInvocation.LifecycleState.ACTIVE, invocation.getState());

        // First cancellation request
        boolean requestedFirst = invocation.requestCancellation();
        assertTrue(requestedFirst);
        assertTrue(invocation.isCancellationRequested());
        assertEquals(RustInvocation.LifecycleState.CANCELLATION_REQUESTED, invocation.getState());
        assertEquals(0, callbackCount.get(), "requestCancellation() MUST NOT execute completion callback");

        // Second cancellation request (idempotency check)
        boolean requestedSecond = invocation.requestCancellation();
        assertFalse(requestedSecond);
        assertEquals(0, callbackCount.get(), "Duplicate cancellation requests MUST NOT execute callback");

        // Context revocation check
        assertThrows(IllegalStateException.class, () -> invocation.getContext().readBody());
    }

    @Test
    void testCompletionAfterCancellationIsExactlyOnce() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        RustInvocation invocation = new RustInvocation("inv-d8-2", exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        assertTrue(invocation.requestCancellation());

        // First completion call after cancellation
        boolean completedFirst = invocation.complete(false);
        assertTrue(completedFirst);
        assertTrue(invocation.isCompleted());
        assertEquals(RustInvocation.LifecycleState.COMPLETED, invocation.getState());

        // Second completion call
        boolean completedSecond = invocation.complete(false);
        assertFalse(completedSecond);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(1, callbackCount.get(), "Callback must execute exactly once even after cancellation");
    }

    @Test
    void testCancellationAndCompletionRace() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        RustInvocation invocation = new RustInvocation("inv-d8-race", exchange, doneSync -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        Thread cancelThread = new Thread(invocation::requestCancellation);
        Thread completeThread = new Thread(() -> invocation.complete(false));

        cancelThread.start();
        completeThread.start();

        cancelThread.join();
        completeThread.join();

        assertTrue(invocation.complete(false) == false || invocation.isCompleted());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(1, callbackCount.get(), "Callback must be invoked exactly once during racing conditions");
    }
}
