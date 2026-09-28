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

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.support.AsyncProcessorSupport;

/**
 * Non-blocking processing boundary delegating execution to a configured {@link RustRuntime}.
 */
public class RustProcessor extends AsyncProcessorSupport {

    private final String processorName;
    private final RustRuntime runtime;
    private final PendingInvocationRegistry registry;
    private RustInvocationListener listener;

    public RustProcessor(String processorName, RustRuntime runtime) {
        this(processorName, runtime, null, null);
    }

    public RustProcessor(String processorName, RustRuntime runtime, PendingInvocationRegistry registry) {
        this(processorName, runtime, registry, null);
    }

    public RustProcessor(String processorName, RustRuntime runtime, PendingInvocationRegistry registry,
                         RustInvocationListener listener) {
        this.processorName = processorName;
        this.runtime = runtime;
        this.registry = registry;
        this.listener = listener;
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        String invocationId = "inv-" + System.nanoTime();

        RustInvocation invocation = new RustInvocation(
                invocationId,
                runtime,
                exchange,
                doneSync -> {
                    if (registry != null) {
                        registry.unregister(invocationId);
                    }
                    if (callback != null) {
                        callback.done(doneSync);
                    }
                },
                listener);

        try {
            if (registry != null) {
                registry.register(invocation);
            }

            if (listener != null) {
                listener.onInvocationCreated(invocation);
            }

            runtime.execute(invocation);

            return invocation.isCompleted();
        } catch (Exception e) {
            if (e instanceof IllegalStateException illegalStateException) {
                invocation.complete(true);
                throw illegalStateException;
            }

            exchange.setException(normalizeException(e, invocationId));
            invocation.complete(true);
            return true;
        }
    }

    private Exception normalizeException(Exception e, String invocationId) {
        if (e instanceof RustExecutionException) {
            return e;
        }

        return new RustExecutionException(
                "Rust execution failed for invocation " + invocationId,
                e,
                invocationId);
    }

    public String getProcessorName() {
        return processorName;
    }

    public RustRuntime getRuntime() {
        return runtime;
    }

    public PendingInvocationRegistry getRegistry() {
        return registry;
    }

    public RustInvocationListener getListener() {
        return listener;
    }

    public void setListener(RustInvocationListener listener) {
        this.listener = listener;
    }
}
