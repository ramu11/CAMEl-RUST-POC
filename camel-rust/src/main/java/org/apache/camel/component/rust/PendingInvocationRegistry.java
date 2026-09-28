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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.spi.Metadata;
import org.apache.camel.support.service.ServiceSupport;

/**
 * Thread-safe lifecycle registry tracking active, non-blocking {@link RustInvocation} instances.
 */
@Metadata(label = "core,management")
public class PendingInvocationRegistry extends ServiceSupport {

    private final Map<String, RustInvocation> pending = new ConcurrentHashMap<>();

    @Override
    protected void doStart() throws Exception {
        // Ready to accept registrations
    }

    @Override
    protected void doStop() throws Exception {
        // Shutdown draining is managed explicitly by the owning Component/Runtime prior to stopping this service
    }

    public void register(RustInvocation invocation) {
        if (invocation != null && !invocation.isCompleted()) {
            RustInvocation existing = pending.putIfAbsent(
                    invocation.getInvocationId(), invocation);

            if (existing != null) {
                throw new IllegalStateException(
                        "RUST_INVOCATION_DUPLICATE: Invocation ID already registered: "
                                                + invocation.getInvocationId());
            }
        }
    }

    public Optional<RustInvocation> unregister(String invocationId) {
        if (invocationId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(pending.remove(invocationId));
    }

    public Optional<RustInvocation> get(String invocationId) {
        if (invocationId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(pending.get(invocationId));
    }

    /**
     * Returns an immutable snapshot of currently active pending invocations.
     */
    public Collection<RustInvocation> snapshot() {
        return List.copyOf(pending.values());
    }

    public int size() {
        return pending.size();
    }

    public boolean isEmpty() {
        return pending.isEmpty();
    }
}
