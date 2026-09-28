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
import org.apache.camel.support.DefaultAsyncProducer;

/**
 * Producer extending {@link DefaultAsyncProducer} to natively integrate with Camel's async engine.
 */
public class RustProducer extends DefaultAsyncProducer {

    private final RustProcessor processor;

    public RustProducer(RustEndpoint endpoint, RustProcessor processor) {
        super(endpoint);
        this.processor = processor;
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        return processor.process(exchange, callback);
    }

    public RustProcessor getProcessor() {
        return processor;
    }
}
