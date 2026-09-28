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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

/**
 * CBOR payload codec used by the native Rust execution boundary.
 *
 * <p>
 * The codec defines the Camel-Rust wire envelope while delegating the actual CBOR encoding and decoding to Jackson's
 * CBOR implementation supplied through Camel's {@code camel-cbor} dependency.
 * </p>
 *
 * <p>
 * The wire format is intentionally represented as a CBOR map rather than serializing the Java records directly. This
 * keeps the protocol independent from Java implementation details and allows it to evolve independently.
 * </p>
 */
public final class CborRustPayloadCodec implements RustPayloadCodec {

    private static final int PROTOCOL_VERSION = 1;

    private static final String VERSION = "version";
    private static final String INVOCATION_ID = "invocationId";
    private static final String BODY = "body";
    private static final String HEADERS = "headers";
    private static final String PROPERTIES = "properties";
    private static final String STATUS = "status";
    private static final String ERROR = "error";

    private static final String ERROR_CODE = "code";
    private static final String ERROR_MESSAGE = "message";
    private static final String ERROR_DETAIL = "detail";

    private final ObjectMapper mapper;

    /**
     * Creates a CBOR codec using a Jackson CBOR mapper.
     */
    public CborRustPayloadCodec() {
        this(new ObjectMapper(new CBORFactory()));
    }

    /**
     * Creates a codec using the supplied mapper.
     *
     * <p>
     * This constructor allows callers that already have a Camel-managed Jackson configuration to supply their
     * configured mapper.
     * </p>
     *
     * @param mapper Jackson mapper configured with a CBOR factory
     */
    public CborRustPayloadCodec(ObjectMapper mapper) {
        if (mapper == null) {
            throw new IllegalArgumentException("mapper must not be null");
        }

        this.mapper = mapper;
    }

    @Override
    public byte[] encode(RustInvocationRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }

        if (request.invocationId() == null
                || request.invocationId().isBlank()) {
            throw new IllegalArgumentException(
                    "request.invocationId must not be null or blank");
        }

        Map<String, Object> envelope = new LinkedHashMap<>();

        envelope.put(VERSION, PROTOCOL_VERSION);
        envelope.put(INVOCATION_ID, request.invocationId());
        envelope.put(BODY, request.body());
        envelope.put(
                HEADERS,
                request.headers() == null
                        ? Map.of()
                        : request.headers());
        envelope.put(
                PROPERTIES,
                request.properties() == null
                        ? Map.of()
                        : request.properties());

        try {
            return mapper.writeValueAsBytes(envelope);
        } catch (JsonProcessingException e) {
            throw new RustExecutionException(
                    "Unable to encode Rust invocation as CBOR",
                    e,
                    request.invocationId());
        }
    }

    @Override
    public RustInvocationResponse decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException(
                    "payload must not be null or empty");
        }

        try {
            JsonNode root = mapper.readTree(payload);

            validateEnvelope(root);

            String invocationId = textValue(
                    root,
                    INVOCATION_ID,
                    null);

            RustInvocationResponse.Status status = parseStatus(root);

            Object body = convertBody(root.get(BODY));

            Map<String, Object> headers = convertMap(root.get(HEADERS));

            RustError error = parseError(root.get(ERROR));

            /*
             * RustInvocationResponse does not currently carry invocationId.
             * The native invocation correlation is maintained by
             * RustNativeBindings. Therefore the ID is validated here but does
             * not need to be copied into the response record.
             */
            return new RustInvocationResponse(
                    status,
                    body,
                    headers,
                    error);

        } catch (RustExecutionException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new RustExecutionException(
                    "Unable to decode native Rust CBOR response",
                    e,
                    null);
        }
    }

    private void validateEnvelope(JsonNode root) {
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException(
                    "Rust CBOR payload must contain a map/object envelope");
        }

        JsonNode version = root.get(VERSION);

        if (version == null || !version.canConvertToInt()) {
            throw new IllegalArgumentException(
                    "Rust CBOR payload is missing a valid protocol version");
        }

        int protocolVersion = version.intValue();

        if (protocolVersion != PROTOCOL_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported Rust CBOR protocol version: "
                                               + protocolVersion
                                               + ", expected "
                                               + PROTOCOL_VERSION);
        }

        JsonNode invocationId = root.get(INVOCATION_ID);

        if (invocationId == null || !invocationId.isTextual()
                || invocationId.textValue().isBlank()) {
            throw new IllegalArgumentException(
                    "Rust CBOR payload is missing a valid invocationId");
        }
    }

    private RustInvocationResponse.Status parseStatus(JsonNode root) {
        JsonNode status = root.get(STATUS);

        if (status == null || !status.isTextual()) {
            /*
             * A request-shaped envelope has no status. Treating that as
             * SUCCESS would hide a malformed native response, so fail closed.
             */
            throw new IllegalArgumentException(
                    "Rust CBOR response is missing status");
        }

        try {
            return RustInvocationResponse.Status.valueOf(
                    status.textValue());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown Rust response status: "
                                               + status.textValue(),
                    e);
        }
    }

    private RustError parseError(JsonNode errorNode) {
        if (errorNode == null || errorNode.isNull()) {
            return null;
        }

        if (!errorNode.isObject()) {
            throw new IllegalArgumentException(
                    "Rust CBOR error must be a map/object");
        }

        String code = textValue(errorNode, ERROR_CODE, null);
        String message = textValue(errorNode, ERROR_MESSAGE, null);
        String detail = textValue(errorNode, ERROR_DETAIL, null);

        return new RustError(code, message, detail);
    }

    private Object convertBody(JsonNode bodyNode) {
        if (bodyNode == null || bodyNode.isNull()) {
            return null;
        }

        /*
         * Jackson's Object.class conversion preserves the natural CBOR
         * structure:
         *
         * object -> Map
         * array  -> List
         * text   -> String
         * boolean -> Boolean
         * integral -> Integer/Long/BigInteger as appropriate
         * floating -> Double
         * binary -> byte[]
         *
         * This keeps the codec independent from application-specific POJOs.
         */
        return mapper.convertValue(bodyNode, Object.class);
    }

    private Map<String, Object> convertMap(JsonNode node) {
        if (node == null || node.isNull()) {
            return Map.of();
        }

        if (!node.isObject()) {
            throw new IllegalArgumentException(
                    "Rust CBOR headers/properties must be maps");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> result = mapper.convertValue(node, Map.class);

        return result == null ? Map.of() : result;
    }

    private String textValue(
            JsonNode node,
            String field,
            String defaultValue) {

        JsonNode value = node.get(field);

        if (value == null || value.isNull()) {
            return defaultValue;
        }

        if (!value.isTextual()) {
            throw new IllegalArgumentException(
                    "Rust CBOR field '" + field + "' must be a string");
        }

        return value.textValue();
    }
}
