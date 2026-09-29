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

import org.apache.camel.Category;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;

@UriEndpoint(
             firstVersion = "2.23.0",
             scheme = "rust",
             title = "Rust",
             syntax = "rust:operation",
             producerOnly = true,
             category = { Category.CORE })
public class RustEndpoint extends DefaultEndpoint {

    @UriPath(description = "Name of the Rust operation")
    @Metadata(required = true, description = "Name of the Rust operation")
    private String operation;

    private final RustRuntime runtime;

    public RustEndpoint(
                        String endpointUri,
                        RustComponent component,
                        String operation,
                        RustRuntime runtime) {
        super(endpointUri, component);
        this.operation = operation;
        this.runtime = runtime;
    }

    @Override
    public RustComponent getComponent() {
        return (RustComponent) super.getComponent();
    }

    @Override
    protected void doInit() throws Exception {
        super.doInit();

        if (runtime != null) {
            getCamelContext().addService(runtime, true);
        }
    }

    @Override
    public Producer createProducer() throws Exception {
        PendingInvocationRegistry registry = getComponent() != null ? getComponent().getRegistry() : null;

        return new RustProducer(
                this,
                operation,
                runtime,
                registry);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException(
                "Rust component does not support consumer endpoints");
    }

    /**
     * Indicates that Camel may reuse this Endpoint instance within a CamelContext.
     *
     * Endpoint singleton lifecycle in Camel does not define Rust runtime thread-safety, state sharing, execution
     * serialization, thread affinity, or native resource allocation. Those concerns belong to the Rust runtime
     * implementation.
     */
    @Override
    public boolean isSingleton() {
        return true;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public RustRuntime getRuntime() {
        return runtime;
    }
}
