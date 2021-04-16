/*******************************************************************************
 * Copyright (c) 2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *******************************************************************************/

package org.eclipse.hono.adapter.coap.lwm2m;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.eclipse.hono.util.ResourceIdentifier;

import io.vertx.core.buffer.Buffer;

/**
 * The result of mapping a message using a {@code MessageMapping} service.
 */
public final class MappedDownstreamMessage {

    private final ResourceIdentifier targetAddress;
    private final String contentType;
    private final Buffer payload;
    private final Map<String, Object> additionalProperties = new HashMap<>();

    /**
     * Creates a new mapping result.
     *
     * @param targetAddress The target address that the original message has been mapped to.
     * @param contentType The type of content contained in the payload or {@code null} if the
     *                    message has no payload.
     * @param payload The payload that the original message has been mapped to or {@code null} if the
     *                    message has no payload..
     * @throws NullPointerException if targetAddress is {@code null}.
     */
    public MappedDownstreamMessage(
            final ResourceIdentifier targetAddress,
            final String contentType,
            final Buffer payload) {
        this(targetAddress, contentType, payload, null);
    }

    /**
     * Creates a new mapping result.
     *
     * @param targetAddress The target address that the original message has been mapped to.
     * @param contentType The type of content contained in the payload or {@code null} if the
     *                    message has no payload.
     * @param payload The payload that the original message has been mapped to or {@code null} if the
     *                    message has no payload..
     * @param additionalProperties Extra properties that should be included with the mapped message.
     * @throws NullPointerException if targetAddress is {@code null}.
     */
    public MappedDownstreamMessage(
            final ResourceIdentifier targetAddress,
            final String contentType,
            final Buffer payload,
            final Map<String, Object> additionalProperties) {

        this.targetAddress = Objects.requireNonNull(targetAddress);
        this.contentType = contentType;
        this.payload = Optional.ofNullable(payload).orElseGet(Buffer::buffer);
        Optional.ofNullable(additionalProperties)
            .ifPresent(props -> this.additionalProperties.putAll(additionalProperties));
    }

    /**
     * Gets the address that the message should be forwarded to.
     *
     * @return The address.
     */
    public ResourceIdentifier getTargetAddress() {
        return targetAddress;
    }

    /**
     * Gets the type of content containe in the payload.
     *
     * @return The content type or {@code null} if the payload is empty.
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Gets the mapped payload.
     *
     * @return The payload.
     */
    public Buffer getPayload() {
        return payload;
    }

    /**
     * Gets additional properties to be included with the mapped
     * message.
     *
     * @return The properties (may be empty).
     */
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }
}
