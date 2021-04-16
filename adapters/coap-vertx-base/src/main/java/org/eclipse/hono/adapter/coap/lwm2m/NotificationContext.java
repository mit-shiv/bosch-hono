/**
 * Copyright (c) 2021 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.eclipse.hono.adapter.coap.lwm2m;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.core.coap.Response;
import org.eclipse.hono.auth.Device;
import org.eclipse.hono.util.MapBasedTelemetryExecutionContext;
import org.eclipse.hono.util.MessageHelper;
import org.eclipse.hono.util.QoS;
import org.eclipse.leshan.core.node.LwM2mNode;
import org.eclipse.leshan.core.observation.Observation;
import org.eclipse.leshan.core.response.ObserveResponse;
import org.eclipse.leshan.server.registration.Registration;

import io.micrometer.core.instrument.Timer.Sample;
import io.opentracing.Span;
import io.vertx.core.buffer.Buffer;

/**
 * A dictionary of relevant information required during the processing of an observe notification contained in
 * a CoAP response message published by a LwM2M client (device).
 *
 */
public final class NotificationContext extends MapBasedTelemetryExecutionContext {

    private final Registration registration;
    private final ObserveResponse response;
    private final Observation observation;
    private final String contentType;
    private final Sample timer;

    private NotificationContext(
            final Registration registration,
            final ObserveResponse response,
            final Device authenticatedDevice,
            final Sample timer,
            final Span span) {
        super(span, authenticatedDevice);
        this.registration = registration;
        this.response = response;
        this.observation = response.getObservation();
        this.timer = timer;
        this.contentType = MediaTypeRegistry.toString(((Response) response.getCoapResponse()).getOptions().getContentFormat());
    }

    /**
     * Creates a new context for an observe notification.
     *
     * @param registration The LwM2M registration of the device that sent the notification.
     * @param response The observe response containing the notification.
     * @param authenticatedDevice The authenticated device that the notification has been received from.
     * @param timer The object to use for measuring the time it takes to process the request.
     * @param span The <em>OpenTracing</em> root span that is used to track the processing of this context.
     * @return The context.
     * @throws NullPointerException if response, originDevice, span or timer are {@code null}.
     */
    public static NotificationContext fromResponse(
            final Registration registration,
            final ObserveResponse response,
            final Device authenticatedDevice,
            final Sample timer,
            final Span span) {

        Objects.requireNonNull(registration);
        Objects.requireNonNull(response);
        Objects.requireNonNull(authenticatedDevice);
        Objects.requireNonNull(span);
        Objects.requireNonNull(timer);

        return new NotificationContext(registration, response, authenticatedDevice, timer, span);
    }

    /**
     * Gets the tenant identifier.
     *
     * @return The tenant.
     */
    public String getTenantId() {
        return getAuthenticatedDevice().getTenantId();
    }

    /**
     * Gets the observe response.
     *
     * @return The response.
     */
    public ObserveResponse getResponse() {
        return response;
    }

    /**
     * Gets the LwM2M registration of the device that sent this notification.
     *
     * @return The registration.
     */
    public Registration getRegistration() {
        return registration;
    }

    /**
     * Gets the payload of this notification parsed into the generic LwM2M resource model.
     *
     * @return The parsed payload.
     */
    public LwM2mNode getParsedPayload() {
        return response.getContent();
    }

    /**
     * Gets the raw payload contained in the CoAP message representing this notification.
     *
     * @return The payload.
     */
    public Buffer getPayload() {
        return Optional.ofNullable(((Response) response.getCoapResponse()).getPayload())
                .map(Buffer::buffer)
                .orElse(Buffer.buffer());
    }

    /**
     * Gets the size of the payload of this notification.
     *
     * @return The size in bytes.
     */
    public int getPayloadSize() {
        return ((Response) response.getCoapResponse()).getPayloadSize();
    }

    /**
     * Gets the media type that corresponds to the <em>content-format</em> option of the CoAP response.
     *
     * @return The media type.
     */
    public String getContentTypeAsString() {
        return contentType;
    }

    /**
     * Gets the object used for measuring the time it takes to process this response.
     *
     * @return The timer or {@code null} if not set.
     */
    public Sample getTimer() {
        return timer;
    }

    /**
     * Checks if this notification has been sent using a CONfirmable message.
     *
     * @return {@code true} if the response message is CONfirmable.
     */
    public boolean isConfirmable() {
        return ((Response) response.getCoapResponse()).isConfirmable();
    }

    @Override
    public QoS getRequestedQos() {
        return isConfirmable() ? QoS.AT_LEAST_ONCE : QoS.AT_MOST_ONCE;
    }

    /**
     * {@inheritDoc}
     *
     * @return An empty optional.
     */
    @Override
    public Optional<Duration> getTimeToLive() {
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     *
     * @return The observed resource path.
     */
    @Override
    public String getOrigAddress() {
        return observation.getPath().toString();
    }

    /**
     * Gets the properties that need to be included in the message being sent
     * downstream.
     * <p>
     * This default implementation puts the following properties to the returned map:
     * <ul>
     * <li>the value returned by {@link #getOrigAddress()} under key {@value MessageHelper#APP_PROPERTY_ORIG_ADDRESS},
     * if not {@code null}</li>
     * <li>the entries returned by {@link Observation#getContext()}</li>
     * </ul>
     *
     * @return The properties.
     */
    @Override
    public Map<String, Object> getDownstreamMessageProperties() {
        final Map<String, Object> props = new HashMap<>();
        Optional.ofNullable(getOrigAddress())
            .ifPresent(address -> props.put(MessageHelper.APP_PROPERTY_ORIG_ADDRESS, address));
        props.putAll(observation.getContext());
        return props;
    }
}
