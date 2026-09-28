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

@UriEndpoint(firstVersion = "2.23.0", scheme = "rust", title = "Rust", syntax = "rust:processorName", producerOnly = true,
             category = { Category.CORE })
public class RustEndpoint extends DefaultEndpoint {

    @UriPath(description = "Name of the target Rust processor or execution function")
    @Metadata(required = true, description = "Name of the target Rust processor or execution function")
    private String processorName;

    private final RustRuntime runtime;

    public RustEndpoint(String endpointUri, RustComponent component, String processorName, RustRuntime runtime) {
        super(endpointUri, component);
        this.processorName = processorName;
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
        RustProcessor processor = new RustProcessor(processorName, runtime, registry);
        return new RustProducer(this, processor);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("Rust component does not support consumer endpoints");
    }

    /**
     * Indicates that Camel may reuse this Endpoint instance within a CamelContext.
     *
     * ARCHITECTURAL NOTE: Endpoint singleton lifecycle in Camel does NOT define Rust runtime thread-safety, state
     * sharing, execution serialization, thread affinity, or native resource allocation. The Rust execution model
     * concurrency and isolation policy remain open/deferred.
     */
    @Override
    public boolean isSingleton() {
        return true;
    }

    public String getProcessorName() {
        return processorName;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    public RustRuntime getRuntime() {
        return runtime;
    }
}
