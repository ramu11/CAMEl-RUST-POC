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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;

/**
 * Fundamental semantic root for a single Camel-Rust execution lifetime.
 *
 * <p>
 * A RustInvocation represents one execution requested by a Camel exchange. It owns the invocation lifecycle,
 * cancellation state, timeout task and completion callback coordination.
 * </p>
 */
public class RustInvocation {

    public enum LifecycleState {
        ACTIVE,
        CANCELLATION_REQUESTED,
        COMPLETED
    }

    private final String invocationId;
    private final String operation;
    private final RustRuntime runtime;
    private final RustInvocationContext context;
    private final AsyncCallback callback;
    private final AtomicReference<LifecycleState> state = new AtomicReference<>(LifecycleState.ACTIVE);
    private final AtomicReference<ScheduledFuture<?>> timeoutFuture = new AtomicReference<>();
    private final AtomicBoolean completedOnce = new AtomicBoolean(false);

    public RustInvocation(String invocationId, Exchange exchange, AsyncCallback callback) {
        this(invocationId, null, null, exchange, callback);
    }

    public RustInvocation(
                          String invocationId,
                          RustRuntime runtime,
                          Exchange exchange,
                          AsyncCallback callback) {
        this(invocationId, null, runtime, exchange, callback);
    }

    public RustInvocation(
                          String invocationId,
                          String operation,
                          RustRuntime runtime,
                          Exchange exchange,
                          AsyncCallback callback) {
        this.invocationId = invocationId;
        this.operation = operation;
        this.runtime = runtime;
        this.context = new RustInvocationContext(exchange);
        this.callback = callback;
    }

    public String getOperation() {
        return operation;
    }

    public String getInvocationId() {
        return invocationId;
    }

    public RustRuntime getRuntime() {
        return runtime;
    }

    public RustInvocationContext getContext() {
        return context;
    }

    Exchange getExchange() {
        return context != null ? context.getExchange() : null;
    }

    void setException(Throwable exception) {
        Exchange exchange = getExchange();
        if (exchange != null) {
            exchange.setException(exception);
        }
    }

    public LifecycleState getState() {
        return state.get();
    }

    public boolean isCompleted() {
        return state.get() == LifecycleState.COMPLETED;
    }

    public boolean isCancellationRequested() {
        return state.get() == LifecycleState.CANCELLATION_REQUESTED
                || state.get() == LifecycleState.COMPLETED;
    }

    void setTimeoutFuture(ScheduledFuture<?> future) {
        this.timeoutFuture.set(future);
    }

    public boolean requestCancellation() {
        return requestCancellation(null);
    }

    public boolean requestCancellation(Throwable exception) {
        if (state.compareAndSet(LifecycleState.ACTIVE, LifecycleState.CANCELLATION_REQUESTED)) {
            if (exception != null) {
                setException(exception);
            }
            cancelTimeoutTask();
            if (context != null) {
                context.revoke();
            }
            return true;
        }
        return false;
    }

    boolean completeSynchronously() {
        return complete(true);
    }

    public boolean complete(boolean doneSync) {
        LifecycleState previous = state.getAndSet(LifecycleState.COMPLETED);
        if (previous != LifecycleState.COMPLETED) {
            cancelTimeoutTask();
            if (context != null) {
                context.revoke();
            }
            triggerCompletionCallbacks(doneSync);
            return true;
        }
        return false;
    }

    private void triggerCompletionCallbacks(boolean doneSync) {
        if (completedOnce.compareAndSet(false, true)) {
            if (callback != null) {
                callback.done(doneSync);
            }
        }
    }

    private void cancelTimeoutTask() {
        ScheduledFuture<?> future = timeoutFuture.getAndSet(null);
        if (future != null) {
            future.cancel(false);
        }
    }
}
