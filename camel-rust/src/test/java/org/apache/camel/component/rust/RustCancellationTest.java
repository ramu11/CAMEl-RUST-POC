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
    void testRequestCancellationIsIdempotentAndRevokesContext() {
        Exchange exchange = new DefaultExchange(context);
        AtomicInteger callbackCount = new AtomicInteger();

        RustInvocation invocation = new RustInvocation(
                "inv-d8-1",
                exchange,
                doneSync -> callbackCount.incrementAndGet());

        assertEquals(
                RustInvocation.LifecycleState.ACTIVE,
                invocation.getState());

        assertTrue(invocation.requestCancellation());

        assertTrue(invocation.isCancellationRequested());
        assertEquals(
                RustInvocation.LifecycleState.CANCELLATION_REQUESTED,
                invocation.getState());

        assertEquals(
                0,
                callbackCount.get(),
                "Cancellation request must not complete the invocation");

        assertFalse(
                invocation.requestCancellation(),
                "Cancellation request must be idempotent");

        assertEquals(
                0,
                callbackCount.get(),
                "Repeated cancellation must not execute the completion callback");

        assertThrows(
                IllegalStateException.class,
                () -> invocation.getContext().readBody(),
                "Invocation context must be revoked after cancellation");
    }

    @Test
    void testCompletionAfterCancellationIsExactlyOnce() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch completionLatch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();

        RustInvocation invocation = new RustInvocation(
                "inv-d8-2",
                exchange,
                doneSync -> {
                    callbackCount.incrementAndGet();
                    completionLatch.countDown();
                });

        assertTrue(invocation.requestCancellation());

        assertTrue(invocation.complete(false));
        assertTrue(invocation.isCompleted());
        assertEquals(
                RustInvocation.LifecycleState.COMPLETED,
                invocation.getState());

        assertFalse(
                invocation.complete(false),
                "A completed invocation must reject subsequent completion");

        assertTrue(
                completionLatch.await(1, TimeUnit.SECONDS),
                "Completion callback must be invoked");

        assertEquals(
                1,
                callbackCount.get(),
                "Completion callback must execute exactly once");
    }

    @Test
    void testCancellationAndCompletionRaceCompletesExactlyOnce() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        CountDownLatch completionLatch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();

        RustInvocation invocation = new RustInvocation(
                "inv-d8-race",
                exchange,
                doneSync -> {
                    callbackCount.incrementAndGet();
                    completionLatch.countDown();
                });

        Thread cancellationThread = new Thread(invocation::requestCancellation);
        Thread completionThread = new Thread(() -> invocation.complete(false));

        cancellationThread.start();
        completionThread.start();

        cancellationThread.join();
        completionThread.join();

        assertTrue(
                completionLatch.await(1, TimeUnit.SECONDS),
                "The invocation must eventually complete");

        assertTrue(
                invocation.isCompleted(),
                "The invocation must reach COMPLETED state");

        assertEquals(
                RustInvocation.LifecycleState.COMPLETED,
                invocation.getState());

        assertEquals(
                1,
                callbackCount.get(),
                "Completion callback must execute exactly once");

        assertFalse(
                invocation.complete(false),
                "Completion after the race must remain idempotent");
    }
}
