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

import java.nio.file.Path;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustRuntimeLifecycleTest {

    private static final Path NATIVE_LIBRARY = Path.of("src/main/rust/target/release/libcamel_rust.so");

    private CamelContext context;
    private DefaultRustNativeRuntime testRuntime;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();

        RustNativeBindings nativeRuntime = new RustNativeBindings(NATIVE_LIBRARY.toAbsolutePath().toString());

        testRuntime = new DefaultRustNativeRuntime(
                nativeRuntime,
                new CborRustPayloadCodec());

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

        assertTrue(
                testRuntime.isStarted(),
                "RustRuntime should start automatically when CamelContext starts");

        context.stop();

        assertTrue(
                testRuntime.isStopped(),
                "RustRuntime should stop automatically when CamelContext stops");
    }

    @Test
    void testRuntimeServiceRegistration() throws Exception {
        context.start();

        RustEndpoint endpoint = context.getEndpoint(
                "rust:uppercaseProcessor",
                RustEndpoint.class);

        assertNotNull(endpoint, "Endpoint should be resolved");
        assertNotNull(
                endpoint.getRuntime(),
                "Runtime should be attached to endpoint");
        assertTrue(
                endpoint.getRuntime().isStarted(),
                "Attached runtime should be in STARTED state");
    }
}
