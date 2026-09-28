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
/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* ```
   http://www.apache.org/licenses/LICENSE-2.0
  ```
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.camel.component.rust;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * Java 25 Foreign Function and Memory API bindings for the native Rust runtime.
 *
 * <p>
 * This class owns the native ABI boundary. It is intentionally separate from {@link DefaultRustNativeRuntime}, which
 * owns Camel runtime semantics.
 * </p>
 *
 */
public final class RustNativeBindings implements AutoCloseable {

    private static final String NATIVE_LIBRARY_NAME = "camel-rust";

    private static final int CAMEL_RUST_OK = 0;
    private static final int CAMEL_RUST_INVALID_RUNTIME = 1;
    private static final int CAMEL_RUST_INVALID_REQUEST = 2;
    private static final int CAMEL_RUST_INVALID_CALLBACK = 3;

    private final RustPayloadCodec payloadCodec;

    private static final FunctionDescriptor RUNTIME_CREATE_DESCRIPTOR = FunctionDescriptor.of(ValueLayout.JAVA_LONG);

    private static final FunctionDescriptor EXECUTE_DESCRIPTOR = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_LONG,
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS);

    private static final FunctionDescriptor CANCEL_DESCRIPTOR = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_LONG,
            ValueLayout.JAVA_LONG);

    private static final FunctionDescriptor RUNTIME_DESTROY_DESCRIPTOR = FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG);

    private static final FunctionDescriptor COMPLETION_CALLBACK_DESCRIPTOR = FunctionDescriptor.ofVoid(
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS);

    private final Arena arena;
    private final MethodHandle runtimeCreate;
    private final MethodHandle execute;
    private final MethodHandle cancel;
    private final MethodHandle runtimeDestroy;
    private final MemorySegment completionCallback;

    private final AtomicLong nativeInvocationIds = new AtomicLong(1);

    private final ConcurrentMap<Long, PendingCompletion> pendingInvocations = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Long> invocationIds = new ConcurrentHashMap<>();

    private final Set<Long> destroyedRuntimeHandles = ConcurrentHashMap.newKeySet();

    private final Object runtimeLifecycleLock = new Object();

    private volatile boolean closed;

    public RustNativeBindings() {
        this(NATIVE_LIBRARY_NAME);
    }

    /**
     * Creates bindings for the specified native library.
     *
     * @param libraryName native library name understood by the platform loader
     */
    public RustNativeBindings(String libraryName) {
        this.payloadCodec = new CborRustPayloadCodec();
        this.arena = Arena.ofShared();

        try {
            SymbolLookup lookup = SymbolLookup.libraryLookup(libraryName, arena);
            Linker linker = Linker.nativeLinker();

            this.runtimeCreate = linker.downcallHandle(
                    lookup.findOrThrow("camel_rust_runtime_create"),
                    RUNTIME_CREATE_DESCRIPTOR);

            this.execute = linker.downcallHandle(
                    lookup.findOrThrow("camel_rust_execute"),
                    EXECUTE_DESCRIPTOR);

            this.cancel = linker.downcallHandle(
                    lookup.findOrThrow("camel_rust_cancel"),
                    CANCEL_DESCRIPTOR);

            this.runtimeDestroy = linker.downcallHandle(
                    lookup.findOrThrow("camel_rust_runtime_destroy"),
                    RUNTIME_DESTROY_DESCRIPTOR);

            MethodHandle completionMethod = MethodHandles.lookup()
                    .findStatic(
                            RustNativeBindings.class,
                            "complete",
                            MethodType.methodType(
                                    void.class,
                                    long.class,
                                    MemorySegment.class,
                                    long.class,
                                    MemorySegment.class));

            this.completionCallback = linker.upcallStub(
                    completionMethod,
                    COMPLETION_CALLBACK_DESCRIPTOR,
                    arena);
        } catch (Throwable e) {
            try {
                arena.close();
            } catch (Exception ignored) {
                // Preserve the original initialization failure.
            }

            throw new IllegalStateException(
                    "Unable to initialize native Rust FFM bindings for library: "
                                            + libraryName,
                    e);
        }
    }

    /**
     * Creates a native Rust runtime instance.
     *
     * @return native runtime handle
     */
    public long create() {
        ensureOpen();

        try {
            long runtimeHandle = (long) runtimeCreate.invokeExact();

            if (runtimeHandle == 0) {
                throw new RustExecutionException(
                        "Native Rust runtime creation returned a null handle",
                        null);
            }

            destroyedRuntimeHandles.remove(runtimeHandle);

            return runtimeHandle;
        } catch (RustExecutionException e) {
            throw e;
        } catch (Throwable e) {
            throw new RustExecutionException(
                    "Unable to create native Rust runtime",
                    e,
                    null);
        }
    }

    /**
     * Executes a native Rust invocation.
     *
     * @param runtimeHandle     native runtime handle
     * @param invocationId      Camel invocation identifier
     * @param request           serialized request payload
     * @param completionHandler completion callback
     */
    public void execute(
            long runtimeHandle,
            String invocationId,
            byte[] request,
            RustCompletionHandler completionHandler) {

        ensureOpen();

        if (runtimeHandle == 0) {
            throw new IllegalArgumentException("runtimeHandle must not be zero");
        }

        if (invocationId == null || invocationId.isBlank()) {
            throw new IllegalArgumentException(
                    "invocationId must not be null or blank");
        }

        if (request == null || request.length == 0) {
            throw new IllegalArgumentException(
                    "request must not be null or empty");
        }

        if (completionHandler == null) {
            throw new IllegalArgumentException(
                    "completionHandler must not be null");
        }

        synchronized (runtimeLifecycleLock) {
            ensureRuntimeActive(runtimeHandle);

            long nativeInvocationId = nextNativeInvocationId();

            Long previous = invocationIds.putIfAbsent(
                    invocationId,
                    nativeInvocationId);

            if (previous != null) {
                throw new IllegalStateException(
                        "Duplicate native Rust invocation ID: " + invocationId);
            }

            pendingInvocations.put(
                    nativeInvocationId,
                    new PendingCompletion(
                            invocationId,
                            completionHandler));

            /*
             * Rust may invoke the completion callback synchronously from
             * camel_rust_execute(). Therefore the binding must be registered
             * before entering the native call.
             */
            registerBinding(nativeInvocationId);

            try (Arena requestArena = Arena.ofConfined()) {
                MemorySegment requestSegment = requestArena.allocateFrom(
                        ValueLayout.JAVA_BYTE,
                        request);

                int status = (int) execute.invokeExact(
                        runtimeHandle,
                        nativeInvocationId,
                        requestSegment,
                        (long) request.length,
                        completionCallback,
                        MemorySegment.NULL);

                if (status != CAMEL_RUST_OK) {
                    removePending(nativeInvocationId);

                    throw new RustExecutionException(
                            "Native Rust execution failed with status "
                                                     + status
                                                     + " ("
                                                     + statusDescription(status)
                                                     + ")",
                            invocationId);
                }
            } catch (RustExecutionException e) {
                throw e;
            } catch (Throwable e) {
                removePending(nativeInvocationId);

                throw new RustExecutionException(
                        "Unable to invoke native Rust runtime",
                        e,
                        invocationId);
            }
        }
    }

    /**
     * Requests cancellation of a native Rust invocation.
     *
     * @param runtimeHandle native runtime handle
     * @param invocationId  Camel invocation identifier
     */
    public void cancel(long runtimeHandle, String invocationId) {
        ensureOpen();

        if (runtimeHandle == 0) {
            throw new IllegalArgumentException(
                    "runtimeHandle must not be zero");
        }

        if (invocationId == null || invocationId.isBlank()) {
            throw new IllegalArgumentException(
                    "invocationId must not be null or blank");
        }

        synchronized (runtimeLifecycleLock) {
            ensureRuntimeActive(runtimeHandle);

            Long nativeInvocationId = invocationIds.get(invocationId);

            if (nativeInvocationId == null) {
                return;
            }

            try {
                int status = (int) cancel.invokeExact(
                        runtimeHandle,
                        nativeInvocationId.longValue());

                if (status != CAMEL_RUST_OK) {
                    throw new RustExecutionException(
                            "Native Rust cancellation failed with status "
                                                     + status
                                                     + " ("
                                                     + statusDescription(status)
                                                     + ")",
                            invocationId);
                }
            } catch (RustExecutionException e) {
                throw e;
            } catch (Throwable e) {
                throw new RustExecutionException(
                        "Unable to cancel native Rust invocation",
                        e,
                        invocationId);
            }
        }
    }

    /**
     * Destroys a native Rust runtime instance.
     *
     * @param runtimeHandle native runtime handle
     */
    public void destroy(long runtimeHandle) {
        ensureOpen();

        if (runtimeHandle == 0) {
            return;
        }

        synchronized (runtimeLifecycleLock) {
            if (!destroyedRuntimeHandles.add(runtimeHandle)) {
                return;
            }

            try {
                runtimeDestroy.invokeExact(runtimeHandle);
            } catch (Throwable e) {
                destroyedRuntimeHandles.remove(runtimeHandle);

                throw new RustExecutionException(
                        "Unable to destroy native Rust runtime",
                        e,
                        null);
            }
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;

            for (Long nativeInvocationId : pendingInvocations.keySet()) {
                unregisterBinding(nativeInvocationId);
            }

            pendingInvocations.clear();
            invocationIds.clear();
            destroyedRuntimeHandles.clear();

            arena.close();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(
                    "Native Rust bindings are already closed");
        }
    }

    private void ensureRuntimeActive(long runtimeHandle) {
        if (destroyedRuntimeHandles.contains(runtimeHandle)) {
            throw new IllegalStateException(
                    "Native Rust runtime is already destroyed: "
                                            + runtimeHandle);
        }
    }

    private long nextNativeInvocationId() {
        long id = nativeInvocationIds.getAndIncrement();

        if (id == 0) {
            id = nativeInvocationIds.getAndIncrement();
        }

        return id;
    }

    private void removePending(long nativeInvocationId) {
        PendingCompletion pending = pendingInvocations.remove(nativeInvocationId);

        unregisterBinding(nativeInvocationId);

        if (pending != null) {
            invocationIds.remove(
                    pending.invocationId(),
                    nativeInvocationId);
        }
    }

    private void handleCompletion(
            long nativeInvocationId,
            MemorySegment response,
            long responseLength) {

        /*
         * Remove the pending invocation before doing any decoding or
         * completion work. This provides the exactly-once guard.
         */
        PendingCompletion pending = pendingInvocations.remove(nativeInvocationId);

        if (pending == null) {
            return;
        }

        invocationIds.remove(
                pending.invocationId(),
                nativeInvocationId);

        unregisterBinding(nativeInvocationId);

        RustInvocationResponse result;

        try {
            if (response == null
                    || response.equals(MemorySegment.NULL)) {
                result = failureResponse(
                        "Native Rust runtime returned a null response",
                        null);
            } else if (responseLength <= 0
                    || responseLength > Integer.MAX_VALUE) {
                result = failureResponse(
                        "Native Rust runtime returned an invalid response length",
                        Long.toString(responseLength));
            } else {
                byte[] responseBytes = response
                        .reinterpret(responseLength)
                        .toArray(ValueLayout.JAVA_BYTE);

                result = payloadCodec.decode(responseBytes);
            }
        } catch (Throwable e) {
            result = failureResponse(
                    "Unable to process native Rust completion",
                    e.toString());
        }

        /*
         * The completion handler is invoked exactly once. If user code
         * throws here, do not attempt a second completion.
         */
        pending.completionHandler().complete(result);
    }

    private RustInvocationResponse failureResponse(
            String message,
            String detail) {

        return new RustInvocationResponse(
                RustInvocationResponse.Status.FAILURE,
                null,
                null,
                new RustError(
                        RustError.RUST_EXECUTION_FAILED,
                        message,
                        detail));
    }

    private static void complete(
            long nativeInvocationId,
            MemorySegment response,
            long responseLength,
            MemorySegment userData) {

        /*
         * The callback itself is an instance-independent FFM upcall.
         * Native correlation is provided by nativeInvocationId, so the
         * user-data pointer is intentionally unused for now.
         */
        RustNativeBindings bindings = BindingRegistry.find(nativeInvocationId);

        if (bindings != null) {
            bindings.handleCompletion(
                    nativeInvocationId,
                    response,
                    responseLength);
        }
    }

    private void registerBinding(long nativeInvocationId) {
        BindingRegistry.register(nativeInvocationId, this);
    }

    private void unregisterBinding(long nativeInvocationId) {
        BindingRegistry.unregister(nativeInvocationId, this);
    }

    private String statusDescription(int status) {
        return switch (status) {
            case CAMEL_RUST_OK -> "OK";
            case CAMEL_RUST_INVALID_RUNTIME -> "INVALID_RUNTIME";
            case CAMEL_RUST_INVALID_REQUEST -> "INVALID_REQUEST";
            case CAMEL_RUST_INVALID_CALLBACK -> "INVALID_CALLBACK";
            default -> "UNKNOWN";
        };
    }

    private record PendingCompletion(
            String invocationId,
            RustCompletionHandler completionHandler) {
    }

    /**
     * Associates native invocation IDs with the binding instance which owns their callback lifecycle.
     */
    private static final class BindingRegistry {

        private static final ConcurrentMap<Long, RustNativeBindings> BINDINGS = new ConcurrentHashMap<>();

        private BindingRegistry() {
        }

        static void register(
                long nativeInvocationId,
                RustNativeBindings bindings) {

            BINDINGS.put(nativeInvocationId, bindings);
        }

        static RustNativeBindings find(long nativeInvocationId) {
            return BINDINGS.get(nativeInvocationId);
        }

        static void unregister(
                long nativeInvocationId,
                RustNativeBindings bindings) {

            BINDINGS.remove(nativeInvocationId, bindings);
        }
    }
}
