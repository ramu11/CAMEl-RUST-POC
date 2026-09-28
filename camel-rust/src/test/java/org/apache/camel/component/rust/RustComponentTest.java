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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustComponentTest extends CamelTestSupport {

    private final InProcessRuntime testRuntime = new InProcessRuntime();

    private static final class FailingRuntime extends InProcessRuntime {

        @Override
        public void execute(RustInvocation invocation) throws Exception {
            throw new IllegalArgumentException("Simulated Rust execution failure");
        }
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();

        RustComponent component = new RustComponent(processorName -> {
            if ("faultyProcessor".equals(processorName)) {
                return new FailingRuntime();
            }
            return new InProcessRuntime();
        });

        context.addComponent("rust", component);
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start")
                        .to("rust:uppercaseProcessor");

                from("direct:error")
                        .to("rust:faultyProcessor");
            }
        };
    }

    private Exchange createExchange(Object body) {
        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody(body);
        return exchange;
    }

    @Test
    void testRustProcessorExecution() {
        String result = template.requestBody("direct:start", "hello rust", String.class);
        assertEquals("HELLO RUST", result);
    }

    @Test
    void testInvocationContextRevocation() {
        Exchange exchange = createExchange("test");
        RustInvocation invocation = new RustInvocation("inv-1", exchange, doneSync -> {
        });

        RustInvocationContext context = invocation.getContext();
        assertTrue(context.isActive());

        invocation.completeSynchronously();

        assertFalse(context.isActive());
        IllegalStateException ex = assertThrows(IllegalStateException.class, context::readBody);
        assertTrue(ex.getMessage().contains("EXCHANGE_ACCESS_REVOKED"));

        assertThrows(IllegalStateException.class, context::getExchange);
        assertThrows(IllegalStateException.class, () -> context.getHeader("foo"));
        assertThrows(IllegalStateException.class, () -> context.setHeader("foo", "bar"));
        assertThrows(IllegalStateException.class, () -> context.getProperty("foo"));
        assertThrows(IllegalStateException.class, () -> context.setProperty("foo", "bar"));
        assertThrows(IllegalStateException.class, () -> context.writeBody("blocked"));
    }

    @Test
    void testSingleWinnerCompletion() {
        Exchange exchange = createExchange("test");
        ConcurrentLinkedQueue<Boolean> callbackResults = new ConcurrentLinkedQueue<>();

        RustInvocation invocation = new RustInvocation("inv-2", exchange, callbackResults::add);

        boolean firstCall = invocation.completeSynchronously();
        boolean secondCall = invocation.completeSynchronously();

        assertTrue(firstCall);
        assertFalse(secondCall);
        assertEquals(1, callbackResults.size());
        assertTrue(invocation.isCompleted());
    }

    @Test
    void testConcurrentCompletionArbitration() throws InterruptedException {
        Exchange exchange = createExchange("test");
        ConcurrentLinkedQueue<Boolean> winners = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        RustInvocation invocation = new RustInvocation("inv-3", exchange, doneSync -> {
        });

        int threadCount = 16;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    if (invocation.completeSynchronously()) {
                        winners.add(true);
                    }
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertTrue(errors.isEmpty(), "No thread execution errors should occur");
        assertEquals(1, winners.size(), "Exactly one thread must win the CAS completion");
        assertTrue(invocation.isCompleted());
    }

    @Test
    void testTerminalStatePreventsReopening() {
        Exchange exchange = createExchange("test");
        RustInvocation invocation = new RustInvocation("inv-4", exchange, doneSync -> {
        });

        invocation.completeSynchronously();
        assertTrue(invocation.isCompleted());

        boolean reComplete = invocation.complete(false);
        assertFalse(reComplete);
        assertFalse(invocation.getContext().isActive());
    }

    @Test
    void testErrorPropagation() {
        Exchange exchange = createExchange("faulty");
        Exchange response = template.send("direct:error", exchange);

        RustExecutionException exception = assertInstanceOf(RustExecutionException.class, response.getException());

        assertEquals(
                "Rust execution failed for invocation " + exception.getInvocationId(),
                exception.getMessage());

        assertInstanceOf(IllegalArgumentException.class, exception.getCause());
        assertEquals("Simulated Rust execution failure", exception.getCause().getMessage());
    }
}
