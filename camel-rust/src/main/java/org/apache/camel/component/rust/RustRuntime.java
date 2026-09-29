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

import org.apache.camel.StatefulService;

/**
 * Service Provider Interface (SPI) decoupling the Camel component boundary from concrete Rust execution engines.
 *
 * <p>
 * The runtime owns execution of a {@link RustInvocation}. It does not expose a Camel processor programming model.
 * </p>
 */
public interface RustRuntime extends StatefulService {

    /**
     * Executes the given invocation asynchronously or synchronously.
     *
     * @param  invocation the execution wrapper holding the Exchange and completion callback
     * @throws Exception  if execution fails prior to asynchronous handoff
     */
    void execute(RustInvocation invocation) throws Exception;

    /**
     * Requests cancellation of an active asynchronous invocation.
     *
     * @param  invocation the invocation to cancel
     * @throws Exception  if runtime cancellation fails
     */
    void cancel(RustInvocation invocation) throws Exception;
}
