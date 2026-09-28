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

import org.apache.camel.support.service.ServiceSupport;

/**
 * In-process synchronous execution runtime.
 */
public class InProcessRuntime extends ServiceSupport implements RustRuntime {

    private final String name;
    private final RustExecutor executor;

    public InProcessRuntime() {
        this("defaultInProcess");
    }

    public InProcessRuntime(String name) {
        this(name, new InProcessExecutor());
    }

    InProcessRuntime(String name, RustExecutor executor) {
        this.name = name;
        this.executor = executor;
    }

    @Override
    public void execute(RustInvocation invocation) throws Exception {
        if (!isStarted()) {
            throw new IllegalStateException(
                    "RUST_RUNTIME_NOT_STARTED: Cannot execute invocation while runtime state is " + getStatus());
        }

        RustInvocationContext context = invocation.getContext();
        RustInvocationRequest request = context.createRequest(invocation.getInvocationId());

        RustCancellationToken cancellationToken = invocation::isCancellationRequested;

        executor.execute(request, cancellationToken, result -> applyResult(invocation, result));
    }

    @Override
    public void cancel(RustInvocation invocation) throws Exception {
        if (invocation != null) {
            invocation.requestCancellation();
            invocation.completeSynchronously();
        }
    }

    public String getName() {
        return name;
    }

    private void applyResult(RustInvocation invocation, RustInvocationResponse result) {
        if (result == null) {
            invocation.setException(
                    new RustExecutionException(
                            "Rust executor returned a null result",
                            null,
                            invocation.getInvocationId()));
            invocation.complete(true);
            return;
        }

        if (result.status() == RustInvocationResponse.Status.FAILURE) {
            RustError error = result.error();

            Throwable cause = error != null
                    ? new IllegalStateException(error.message())
                    : null;

            invocation.setException(
                    new RustExecutionException(
                            "Rust executor returned a null result",
                            null,
                            invocation.getInvocationId()));
            invocation.complete(true);
            return;
        }

        RustInvocationContext context = invocation.getContext();

        if (result.body() != null) {
            context.writeBody(result.body());
        }

        if (result.headers() != null) {
            result.headers().forEach(context::setHeader);
        }

        invocation.completeSynchronously();
    }

    /**
     * Temporary Java implementation used to prove the boundary contract.
     */
    public static class InProcessExecutor implements RustExecutor {

        @Override
        public void execute(
                RustInvocationRequest request,
                RustCancellationToken cancellationToken,
                RustCompletionHandler completionHandler) {

            if (cancellationToken.isCancellationRequested()) {
                completionHandler.complete(
                        new RustInvocationResponse(
                                RustInvocationResponse.Status.FAILURE,
                                null,
                                null,
                                new RustError(
                                        RustError.RUST_CANCELLED,
                                        "Rust execution was cancelled",
                                        null)));
                return;
            }

            Object body = request.body();

            Object resultBody = body instanceof String str
                    ? str.toUpperCase(Locale.ROOT)
                    : body;

            completionHandler.complete(
                    new RustInvocationResponse(
                            RustInvocationResponse.Status.SUCCESS,
                            resultBody,
                            null,
                            null));
        }
    }

    @Override
    protected void doStart() throws Exception {
        // no-op
    }

    @Override
    protected void doStop() throws Exception {
        // no-op
    }
}
