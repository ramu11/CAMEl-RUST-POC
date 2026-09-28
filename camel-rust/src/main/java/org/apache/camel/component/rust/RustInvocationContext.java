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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.Exchange;

/**
 * Encapsulates the execution context for a Rust invocation, providing controlled access to the Exchange and creating
 * immutable invocation-boundary snapshots.
 */
public class RustInvocationContext {

    private final Exchange exchange;
    private final AtomicBoolean active = new AtomicBoolean(true);

    public RustInvocationContext(Exchange exchange) {
        this.exchange = exchange;
    }

    Exchange getExchange() {
        ensureActive();
        return exchange;
    }

    public boolean isActive() {
        return active.get();
    }

    public void revoke() {
        active.set(false);
    }

    public Object readBody() {
        ensureActive();
        return exchange != null ? exchange.getIn().getBody() : null;
    }

    public void writeBody(Object body) {
        ensureActive();
        if (exchange != null) {
            exchange.getIn().setBody(body);
        }
    }

    public Object getHeader(String name) {
        ensureActive();
        return exchange != null ? exchange.getIn().getHeader(name) : null;
    }

    public void setHeader(String name, Object value) {
        ensureActive();
        if (exchange != null) {
            exchange.getIn().setHeader(name, value);
        }
    }

    public Object getProperty(String name) {
        ensureActive();
        return exchange != null ? exchange.getProperty(name) : null;
    }

    public void setProperty(String name, Object value) {
        ensureActive();
        if (exchange != null) {
            exchange.setProperty(name, value);
        }
    }

    /**
     * Creates a snapshot of the data exposed to the Rust execution boundary.
     *
     * <p>
     * The returned maps are independent snapshots and must not be used to access the Exchange after invocation
     * completion.
     * </p>
     *
     * @param  invocationId unique invocation identifier
     * @return              invocation request snapshot
     */
    public RustInvocationRequest createRequest(String invocationId) {
        ensureActive();

        Object body = readBody();

        Map<String, Object> headers = exchange != null
                ? new HashMap<>(exchange.getIn().getHeaders())
                : new HashMap<>();

        Map<String, Object> properties = exchange != null
                ? new HashMap<>(exchange.getProperties())
                : new HashMap<>();

        return new RustInvocationRequest(invocationId, body, headers, properties);
    }

    private void ensureActive() {
        if (!active.get()) {
            throw new IllegalStateException("EXCHANGE_ACCESS_REVOKED: Invocation context is no longer active");
        }
    }
}
