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

import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RustNativeIntegrationTest extends CamelTestSupport {

    @Test
    public void shouldExecuteRealRustEchoFromCamelRoute() throws Exception {
        MockEndpoint result = getMockEndpoint("mock:result");
        result.expectedMessageCount(1);
        result.expectedBodiesReceived("hello-rust");

        template.sendBody("direct:start", "hello-rust");

        result.assertIsSatisfied();

        Exchange exchange = result.getReceivedExchanges().get(0);

        assertNotNull(exchange);
        assertEquals("hello-rust", exchange.getIn().getBody(String.class));
        assertNull(exchange.getException());
    }

    @Test
    public void shouldExecuteRealRustStructTransformFromCamelRoute() throws Exception {
        MockEndpoint result = getMockEndpoint("mock:struct-result");
        result.expectedMessageCount(1);

        Map<String, Object> input = Map.of(
                "name", "alice",
                "age", 30);

        Exchange exchange = template.request("direct:struct", inputExchange -> {
            inputExchange.getIn().setBody(input);
        });

        result.assertIsSatisfied();

        assertNotNull(exchange);
        assertNull(exchange.getException());

        Object body = exchange.getIn().getBody();
        assertNotNull(body);

        assertEquals(
                Map.of(
                        "name", "ALICE",
                        "age", 31,
                        "active", true),
                body);
    }

    @Test
    public void shouldExecuteRealRustNestedCollectionsFromCamelRoute() throws Exception {
        MockEndpoint result = getMockEndpoint("mock:nested-result");
        result.expectedMessageCount(1);

        List<Map<String, Object>> input = List.of(
                Map.of("value", 1),
                Map.of("value", 2));

        Exchange exchange = template.request("direct:nested", inputExchange -> {
            inputExchange.getIn().setBody(input);
        });

        result.assertIsSatisfied();

        assertNotNull(exchange);
        assertNull(exchange.getException());

        Object body = exchange.getIn().getBody();
        assertNotNull(body);

        assertEquals(
                List.of(
                        Map.of("value", 101),
                        Map.of("value", 102)),
                body);
    }

    @Test
    public void shouldPropagateUnknownRustOperationAsCamelException() throws Exception {
        MockEndpoint result = getMockEndpoint("mock:failure-result");
        result.expectedMessageCount(0);

        Exchange exchange = template.request("direct:failure", input -> {
            input.getIn().setBody("should-fail");
        });

        assertNotNull(exchange);
        assertNotNull(exchange.getException());
        assertTrue(exchange.getException() instanceof RustExecutionException);
        assertTrue(exchange.getException().getMessage().contains(
                "Rust operation is not implemented: does_not_exist"));

        result.assertIsSatisfied();
    }

    @Test
    public void shouldExecuteMultipleRealRustInvocationsSequentially() throws Exception {
        MockEndpoint result = getMockEndpoint("mock:result");
        result.expectedMessageCount(3);
        result.expectedBodiesReceivedInAnyOrder(
                "first",
                "second",
                "third");

        template.sendBody("direct:start", "first");
        template.sendBody("direct:start", "second");
        template.sendBody("direct:start", "third");

        result.assertIsSatisfied();

        assertEquals(3, result.getReceivedExchanges().size());

        for (Exchange exchange : result.getReceivedExchanges()) {
            assertNotNull(exchange);
            assertNull(exchange.getException());
        }
    }

    @Test
    public void shouldTransformJavaListThroughRust() throws Exception {
        MockEndpoint result = getMockEndpoint("mock:list-transform");
        result.expectedMessageCount(1);

        List<Integer> input = List.of(1, 2, 3, 4);

        template.sendBody("direct:list-transform", input);

        result.assertIsSatisfied();

        Exchange exchange = result.getReceivedExchanges().get(0);
        assertEquals(List.of(2, 4, 6, 8), exchange.getIn().getBody());
        assertEquals(null, exchange.getException());
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start")
                        .to("rust:echo")
                        .to("mock:result");

                from("direct:struct")
                        .to("rust:struct_transform")
                        .to("mock:struct-result");

                from("direct:nested")
                        .to("rust:nested_collections")
                        .to("mock:nested-result");

                from("direct:failure")
                        .to("rust:does_not_exist")
                        .to("mock:failure-result");

                from("direct:list-transform")
                        .to("rust:vec_transform")
                        .to("mock:list-transform");
            }
        };
    }
}
