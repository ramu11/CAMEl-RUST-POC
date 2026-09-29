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

import org.apache.camel.support.service.ServiceSupport;

public class DefaultRustNativeRuntime extends ServiceSupport implements RustRuntime {

    private final RustNativeBindings nativeRuntime;
    private final RustPayloadCodec codec;

    private volatile long runtimeHandle;

    public DefaultRustNativeRuntime() {
        this(new RustNativeBindings(), new CborRustPayloadCodec());
    }

    DefaultRustNativeRuntime(
                             RustNativeBindings nativeRuntime,
                             RustPayloadCodec codec) {

        this.nativeRuntime = nativeRuntime;
        this.codec = codec;
    }

    @Override
    public void execute(RustInvocation invocation) throws Exception {
        if (!isStarted()) {
            throw new IllegalStateException(
                    "RUST_RUNTIME_NOT_STARTED: Cannot execute invocation while runtime state is " + getStatus());
        }

        RustInvocationRequest request = invocation.getContext().createRequest(invocation.getInvocationId());

        byte[] payload = codec.encode(request);

        nativeRuntime.execute(
                runtimeHandle,
                invocation.getInvocationId(),
                payload,
                result -> applyResult(invocation, result));
    }

    @Override
    public void cancel(RustInvocation invocation) throws Exception {
        if (invocation != null) {
            invocation.requestCancellation();
            nativeRuntime.cancel(
                    runtimeHandle,
                    invocation.getInvocationId());
            invocation.completeSynchronously();
        }
    }

    private void applyResult(
            RustInvocation invocation,
            RustInvocationResponse result) {

        if (result == null) {
            invocation.setException(
                    new RustExecutionException(
                            "Rust native runtime returned a null result",
                            invocation.getInvocationId()));
            invocation.complete(true);
            return;
        }

        if (result.status() == RustInvocationResponse.Status.FAILURE) {
            RustError error = result.error();

            invocation.setException(
                    new RustExecutionException(
                            error != null ? error.message() : "Rust execution failed",
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

    @Override
    protected void doStart() throws Exception {
        runtimeHandle = nativeRuntime.create();

        if (runtimeHandle == 0) {
            throw new IllegalStateException(
                    "RUST_RUNTIME_CREATE_FAILED: Native runtime handle is zero");
        }
    }

    @Override
    protected void doStop() throws Exception {
        long handle = runtimeHandle;
        runtimeHandle = 0;

        if (handle != 0) {
            nativeRuntime.destroy(handle);
        }
    }
}
