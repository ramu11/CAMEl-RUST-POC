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

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustRuntimeLifecycleTest {

    private CamelContext context;
    private InProcessRuntime testRuntime;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
        testRuntime = new InProcessRuntime();

        RustComponent component = new RustComponent(processorName -> testRuntime);
        context.addComponent("rust", component);

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start")
                        .to("rust:uppercaseProcessor");
            }
        });
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void testRuntimeStartsAndStopsWithContext() throws Exception {
        context.start();
        assertTrue(testRuntime.isStarted(), "RustRuntime should start automatically when CamelContext starts");

        context.stop();
        assertTrue(testRuntime.isStopped(), "RustRuntime should stop automatically when CamelContext stops");
    }

    @Test
    void testExecutionRejectedWhenStopped() {
        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello");
        RustInvocation invocation = new RustInvocation("inv-lifecycle-1", exchange, doneSync -> {
        });

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            testRuntime.execute(invocation);
        });

        assertTrue(exception.getMessage().contains("RUST_RUNTIME_NOT_STARTED"),
                "Executing against a non-started runtime must throw IllegalStateException");
    }

    @Test
    void testRuntimeServiceRegistrationAndExecution() throws Exception {
        context.start();

        RustEndpoint endpoint = context.getEndpoint("rust:uppercaseProcessor", RustEndpoint.class);
        assertNotNull(endpoint, "Endpoint should be resolved");
        assertNotNull(endpoint.getRuntime(), "Runtime should be attached to endpoint");
        assertTrue(endpoint.getRuntime().isStarted(), "Attached runtime should be in STARTED state");

        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("hello rust");
        RustInvocation invocation = new RustInvocation("inv-lifecycle-2", exchange, doneSync -> {
        });

        endpoint.getRuntime().execute(invocation);
        assertEquals("HELLO RUST", exchange.getIn().getBody(), "Started runtime should execute payload transformation");
    }
}
