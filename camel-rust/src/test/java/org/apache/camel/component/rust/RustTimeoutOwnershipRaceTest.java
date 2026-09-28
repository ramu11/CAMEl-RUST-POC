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

import java.util.concurrent.TimeoutException;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustTimeoutOwnershipRaceTest {

    private DefaultCamelContext context;
    private AsyncRuntime runtime;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.start();
        runtime = new AsyncRuntime(10);
        runtime.start();
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
    void workerWinningCompletionPreventsTimeoutExchangeMutation() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello");

        RustInvocation invocation = new RustInvocation(
                "inv-worker-win", runtime, exchange, null, null);

        // Worker wins terminal completion.
        invocation.complete(false);

        assertTrue(invocation.isCompleted());
        assertEquals(RustInvocation.LifecycleState.COMPLETED, invocation.getState());

        // A later timeout attempt cannot claim cancellation ownership.
        boolean cancelled = invocation.requestCancellation();
        assertFalse(cancelled,
                "Timeout/cancel MUST NOT claim cancellation if invocation already completed");

        // Timeout must therefore not mutate the Exchange.
        if (cancelled) {
            exchange.setException(new TimeoutException("Timed out"));
        }

        assertNull(exchange.getException(),
                "Exchange exception MUST remain null if worker won completion");
    }

    @Test
    void timeoutWinningCancellationPreventsWorkerExchangeMutation() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello");

        RustInvocation invocation = new RustInvocation(
                "inv-timeout-win", runtime, exchange, null, null);

        // Timeout fires first and wins cancellation ownership.
        boolean cancelled = invocation.requestCancellation();
        assertTrue(cancelled, "Timeout MUST win cancellation ownership");

        if (cancelled) {
            exchange.setException(new TimeoutException("Rust execution timed out"));
            invocation.complete(false);
        }

        assertTrue(invocation.isCompleted());
        assertEquals(RustInvocation.LifecycleState.COMPLETED, invocation.getState());

        // Worker attempts to process afterwards but must observe cancellation
        // and must not overwrite the timeout exception.
        if (invocation.isCancellationRequested()) {
            // Worker observes cancellation/completion and exits without writing an error.
        } else {
            exchange.setException(
                    new RustExecutionException("Late worker error", "inv-timeout-win"));
        }

        assertInstanceOf(TimeoutException.class, exchange.getException(),
                "Exchange exception MUST retain TimeoutException and not be overwritten by worker error");
        assertEquals("Rust execution timed out", exchange.getException().getMessage());
    }
}
