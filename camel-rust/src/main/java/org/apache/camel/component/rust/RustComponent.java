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

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.annotations.Component;
import org.apache.camel.support.DefaultComponent;

@Component("rust")
@Metadata(label = "core,native")
public class RustComponent extends DefaultComponent {

    @Metadata(description = "Factory to create or look up RustRuntime instances")
    private RustRuntimeFactory runtimeFactory;

    private final PendingInvocationRegistry registry = new PendingInvocationRegistry();

    public RustComponent() {
        this(processorName -> new DefaultRustNativeRuntime());
    }

    public RustComponent(RustRuntimeFactory runtimeFactory) {
        this.runtimeFactory = runtimeFactory;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        registry.start();
    }

    @Override
    protected void doStop() throws Exception {
        // Drain pending invocations by delegating to their owning runtime's cancel contract
        for (RustInvocation invocation : registry.snapshot()) {
            RustRuntime runtime = invocation.getRuntime();

            if (runtime != null) {
                try {
                    runtime.cancel(invocation);
                } catch (Exception ignored) {
                    // Runtime cancellation failed; force lifecycle completion so the
                    // invocation is not left pending during component shutdown.
                    invocation.requestCancellation();
                    invocation.complete(false);
                }
            } else {
                invocation.requestCancellation();
                invocation.complete(false);
            }
        }

        registry.stop();
        super.doStop();
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        RustRuntime runtime = runtimeFactory != null ? runtimeFactory.createRuntime(remaining) : new InProcessRuntime();
        RustEndpoint endpoint = new RustEndpoint(uri, this, remaining, runtime);
        setProperties(endpoint, parameters);
        return endpoint;
    }

    public RustRuntimeFactory getRuntimeFactory() {
        return runtimeFactory;
    }

    public void setRuntimeFactory(RustRuntimeFactory runtimeFactory) {
        this.runtimeFactory = runtimeFactory;
    }

    public PendingInvocationRegistry getRegistry() {
        return registry;
    }
}
