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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustErrorAndRedeliveryTest {

    private CamelContext context;
    private PendingInvocationRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
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
    void testAsyncRustFailureReachesCamelErrorHandler() throws Exception {
        AsyncRuntime failingRuntime = new AsyncRuntime(10, true);
        failingRuntime.start();

        RustProcessor processor = new RustProcessor("failingProc", failingRuntime, registry);

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("fail async payload");

        CountDownLatch latch = new CountDownLatch(1);
        boolean sync = processor.process(exchange, doneSync -> latch.countDown());

        assertFalse(sync);
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        assertInstanceOf(RustExecutionException.class, exchange.getException(),
                "Async runtime failure must propagate as RustExecutionException");
        assertEquals(0, registry.size(), "Pending registry must be empty post-failure");

        failingRuntime.stop();
    }

    @Test
    void testProductionProcessorCamelRedeliverySequenceAndIsolation() throws Exception {
        AsyncRuntime failingRuntime = new AsyncRuntime(10, true);
        failingRuntime.start();

        List<String> eventLog = Collections.synchronizedList(new ArrayList<>());
        List<RustInvocation> capturedInvocations = Collections.synchronizedList(new ArrayList<>());
        List<Exchange> capturedExchanges = Collections.synchronizedList(new ArrayList<>());

        RustProcessor productionProcessor = new RustProcessor("failingProc", failingRuntime, registry);
        productionProcessor.setListener(new RustInvocationListener() {
            @Override
            public void onInvocationCreated(RustInvocation invocation) {
                capturedInvocations.add(invocation);
                capturedExchanges.add(invocation.getExchange());
                eventLog.add("CREATED:" + invocation.getInvocationId());
            }

            @Override
            public void onInvocationCompleted(RustInvocation invocation) {
                boolean revoked = false;
                try {
                    invocation.getContext().readBody();
                } catch (IllegalStateException expected) {
                    revoked = true;
                }
                eventLog.add("COMPLETED:" + invocation.getInvocationId() + ":state=" + invocation.getState() + ":revoked="
                             + revoked);
            }
        });

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(defaultErrorHandler()
                        .maximumRedeliveries(2)
                        .redeliveryDelay(10));

                from("direct:redeliveryRoute")
                        .process(productionProcessor);
            }
        });
        context.start();

        Exchange resultExchange = context.createProducerTemplate().send("direct:redeliveryRoute", exchange -> {
            exchange.getIn().setBody("redelivery test body");
        });

        // 1. Assert exactly 6 events logged (3 CREATED + 3 COMPLETED)
        assertEquals(6, eventLog.size(), "Must record 6 total interleaved lifecycle events");

        RustInvocation attempt1 = capturedInvocations.get(0);
        RustInvocation attempt2 = capturedInvocations.get(1);
        RustInvocation attempt3 = capturedInvocations.get(2);

        // 2. Assert strict ordering and revocation state at the moment of event emission
        assertEquals("CREATED:" + attempt1.getInvocationId(), eventLog.get(0));
        assertEquals("COMPLETED:" + attempt1.getInvocationId() + ":state=COMPLETED:revoked=true", eventLog.get(1));

        assertEquals("CREATED:" + attempt2.getInvocationId(), eventLog.get(2));
        assertEquals("COMPLETED:" + attempt2.getInvocationId() + ":state=COMPLETED:revoked=true", eventLog.get(3));

        assertEquals("CREATED:" + attempt3.getInvocationId(), eventLog.get(4));
        assertEquals("COMPLETED:" + attempt3.getInvocationId() + ":state=COMPLETED:revoked=true", eventLog.get(5));

        // 3. Assert distinct invocation instances & IDs per attempt
        assertNotEquals(attempt1.getInvocationId(), attempt2.getInvocationId());
        assertNotEquals(attempt2.getInvocationId(), attempt3.getInvocationId());

        // 4. Assert Exchange identity remains preserved across all attempts
        assertSame(capturedExchanges.get(0), capturedExchanges.get(1),
                "Exchange identity must be preserved across attempt 1 and 2");
        assertSame(capturedExchanges.get(1), capturedExchanges.get(2),
                "Exchange identity must be preserved across attempt 2 and 3");

        // 5. Assert total registry cleanup
        assertEquals(0, registry.size(), "Registry must be empty post redeliveries");

        // 6. Assert final unhandled failure remains on the Exchange
        assertNotNull(resultExchange.getException(), "Final unhandled exception must remain on Exchange");
        assertInstanceOf(RustExecutionException.class, resultExchange.getException());

        failingRuntime.stop();
    }

    @Test
    void testOnExceptionHandledPreventsCamelRedeliveryAndLeavesRegistryEmpty() throws Exception {
        AsyncRuntime failingRuntime = new AsyncRuntime(10, true);
        failingRuntime.start();

        RustProcessor productionProcessor = new RustProcessor("failingProc", failingRuntime, registry);

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                onException(RustExecutionException.class)
                        .handled(true)
                        .transform().constant("HANDLED_BY_CAMEL");

                from("direct:onExceptionRoute")
                        .process(productionProcessor);
            }
        });
        context.start();

        Exchange result = context.createProducerTemplate().send("direct:onExceptionRoute", exchange -> {
            exchange.getIn().setBody("trigger fallback");
        });

        assertNull(result.getException(), "Handled exception must be cleared from Exchange by Camel");
        assertEquals("HANDLED_BY_CAMEL", result.getMessage().getBody(), "Fallback payload set by onException route");
        assertEquals(0, registry.size(), "Registry must be empty after handled error");

        failingRuntime.stop();
    }
}
