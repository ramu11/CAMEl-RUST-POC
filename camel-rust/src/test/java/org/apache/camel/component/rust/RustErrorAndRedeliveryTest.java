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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.service.ServiceSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RustErrorAndRedeliveryTest {

    private CamelContext context;
    private RustComponent rustComponent;
    private FailingRustRuntime runtime;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();

        runtime = new FailingRustRuntime();
        rustComponent = new RustComponent(operation -> runtime);

        context.addComponent("rust", rustComponent);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void testRustFailureReachesCamelErrorHandler() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:failure")
                        .to("rust:failing-operation");
            }
        });

        context.start();

        Exchange exchange = context.createProducerTemplate()
                .request("direct:failure", e -> e.getIn().setBody("input"));

        assertNotNull(exchange.getException());
        assertTrue(exchange.getException() instanceof RustExecutionException);
        assertEquals(1, runtime.getInvocationCount());
        assertTrue(rustComponent.getRegistry().isEmpty());
    }

    @Test
    void testCamelRedeliveryCreatesDistinctRustInvocationsOnSameExchange() throws Exception {
        List<String> invocationIds = new ArrayList<>();
        List<Exchange> exchanges = new ArrayList<>();

        runtime.setObserver(invocation -> {
            invocationIds.add(invocation.getInvocationId());
            exchanges.add(invocation.getExchange());
        });

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(defaultErrorHandler()
                        .maximumRedeliveries(2)
                        .redeliveryDelay(0));

                from("direct:redelivery")
                        .to("rust:failing-operation");
            }
        });

        context.start();

        Exchange exchange = context.createProducerTemplate()
                .request("direct:redelivery", e -> e.getIn().setBody("input"));

        assertNotNull(exchange.getException());
        assertTrue(exchange.getException() instanceof RustExecutionException);

        assertEquals(3, runtime.getInvocationCount());
        assertEquals(3, invocationIds.size());
        assertEquals(3, invocationIds.stream().distinct().count());

        assertEquals(3, exchanges.size());
        assertSame(exchange, exchanges.get(0));
        assertSame(exchange, exchanges.get(1));
        assertSame(exchange, exchanges.get(2));

        assertTrue(rustComponent.getRegistry().isEmpty());
    }

    @Test
    void testOnExceptionHandledPreventsRedeliveryAndClearsFailure() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                onException(RustExecutionException.class)
                        .handled(true)
                        .transform()
                        .constant("handled");

                from("direct:handled")
                        .to("rust:failing-operation");
            }
        });

        context.start();

        Exchange exchange = context.createProducerTemplate()
                .request("direct:handled", e -> e.getIn().setBody("input"));

        assertNull(exchange.getException());
        assertEquals("handled", exchange.getIn().getBody());
        assertEquals(1, runtime.getInvocationCount());
        assertTrue(rustComponent.getRegistry().isEmpty());
    }

    private static final class FailingRustRuntime extends ServiceSupport implements RustRuntime {

        private final AtomicInteger invocationCount = new AtomicInteger();
        private volatile java.util.function.Consumer<RustInvocation> observer;

        @Override
        public void execute(RustInvocation invocation) {
            invocationCount.incrementAndGet();

            java.util.function.Consumer<RustInvocation> currentObserver = observer;
            if (currentObserver != null) {
                currentObserver.accept(invocation);
            }

            invocation.setException(
                    new RustExecutionException(
                            "Rust operation failed",
                            invocation.getInvocationId()));

            invocation.complete(true);
        }

        @Override
        public void cancel(RustInvocation invocation) {
            if (invocation != null) {
                invocation.requestCancellation();
                invocation.completeSynchronously();
            }
        }

        void setObserver(java.util.function.Consumer<RustInvocation> observer) {
            this.observer = observer;
        }

        int getInvocationCount() {
            return invocationCount.get();
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }
    }
}
