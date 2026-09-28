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

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.support.service.ServiceSupport;

/**
 * Pure Java asynchronous runtime supporting non-blocking execution, worker cancellation, deadline-driven timeouts, and
 * shutdown draining.
 */
public class AsyncRuntime extends ServiceSupport implements RustRuntime {

    private final long delayMs;
    private final boolean failAsync;
    private ScheduledExecutorService executor;
    private final Map<String, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();
    private final Map<String, RustInvocation> activeInvocations = new ConcurrentHashMap<>();

    public AsyncRuntime(long delayMs) {
        this(delayMs, false);
    }

    public AsyncRuntime(long delayMs, boolean failAsync) {
        this.delayMs = delayMs;
        this.failAsync = failAsync;
    }

    @Override
    protected void doStart() throws Exception {
        executor = Executors.newScheduledThreadPool(4);
        scheduledTasks.clear();
        activeInvocations.clear();
    }

    @Override
    protected void doStop() throws Exception {
        for (RustInvocation invocation : activeInvocations.values()) {
            try {
                cancel(invocation);
            } catch (Exception ignored) {
            }
        }

        if (executor != null) {
            executor.shutdownNow();
        }

        scheduledTasks.clear();
        activeInvocations.clear();
    }

    @Override
    public void execute(RustInvocation invocation) throws Exception {
        execute(invocation, 0);
    }

    public void execute(RustInvocation invocation, long timeoutMs) throws Exception {
        if (!isStarted()) {
            throw new IllegalStateException(
                    "RUST_RUNTIME_NOT_STARTED: Cannot execute invocation while runtime state is " + getStatus());
        }

        String invocationId = invocation.getInvocationId();
        activeInvocations.put(invocationId, invocation);

        if (timeoutMs > 0) {
            ScheduledFuture<?> timeoutTask = executor.schedule(() -> {
                handleTimeout(invocation, timeoutMs);
            }, timeoutMs, TimeUnit.MILLISECONDS);
            invocation.setTimeoutFuture(timeoutTask);
        }

        ScheduledFuture<?> workerTask = executor.schedule(() -> {
            try {
                if (invocation.isCancellationRequested()) {
                    return;
                }

                RustInvocationContext context = invocation.getContext();
                if (failAsync) {
                    throw new IllegalArgumentException("Simulated async native error");
                }

                Object body = context.readBody();
                if (body instanceof String str) {
                    context.writeBody(str.toUpperCase(Locale.ROOT));
                }
            } catch (Exception e) {
                if (e instanceof RustExecutionException ree) {
                    invocation.setException(ree);
                } else {
                    invocation.setException(
                            new RustExecutionException(
                                    "Rust execution failed for invocation " + invocationId,
                                    e,
                                    invocationId));
                }
            } finally {
                scheduledTasks.remove(invocationId);
                activeInvocations.remove(invocationId);
                invocation.complete(false);
            }
        }, delayMs, TimeUnit.MILLISECONDS);

        scheduledTasks.put(invocationId, workerTask);
    }

    private void handleTimeout(RustInvocation invocation, long timeoutMs) {
        if (invocation.requestCancellation(
                new TimeoutException("Rust execution timed out after " + timeoutMs + " ms"))) {
            cleanupAndCompleteCancellation(invocation);
        }
    }

    @Override
    public void cancel(RustInvocation invocation) throws Exception {
        if (invocation == null) {
            return;
        }

        if (invocation.requestCancellation()) {
            cleanupAndCompleteCancellation(invocation);
        }
    }

    private void cleanupAndCompleteCancellation(RustInvocation invocation) {
        String invocationId = invocation.getInvocationId();

        ScheduledFuture<?> future = scheduledTasks.remove(invocationId);
        if (future != null) {
            future.cancel(true);
        }

        activeInvocations.remove(invocationId);
        invocation.complete(false);
    }

    public boolean hasScheduledTask(String invocationId) {
        return scheduledTasks.containsKey(invocationId);
    }

    public boolean hasActiveInvocation(String invocationId) {
        return activeInvocations.containsKey(invocationId);
    }
}
