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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.eclipse.leshan.core.request.DownlinkRequest;
import org.eclipse.leshan.core.response.LwM2mResponse;

/**
 * The outcome of mapping a Hono command message to a LwM2M request.
 */
public final class MappedUpstreamMessage {

    private final List<ObservationSpec> requiredObservations;
    private final DownlinkRequest<? extends LwM2mResponse> request;

    /**
     * Creates a new mapped message.
     *
     * @param request The request that the command has been mapped to.
     * @throws NullPointerException if request is {@code null}.
     * @see #getRequiredObservations()
     */
    public MappedUpstreamMessage(final DownlinkRequest<? extends LwM2mResponse> request) {
        this(request, (ObservationSpec[]) null);
    }

    /**
     * Creates a new mapped message.
     *
     * @param request The request that the command has been mapped to.
     * @param requiredObservations The observations that need to be established before sending the request to
     *                             the device or {@code null} if no observations are required.
     * @throws NullPointerException if request is {@code null}.
     * @see #getRequiredObservations()
     */
    public MappedUpstreamMessage(
            final DownlinkRequest<? extends LwM2mResponse> request,
            final ObservationSpec... requiredObservations) {

        Objects.requireNonNull(request);
        this.requiredObservations = Optional.ofNullable(requiredObservations)
                .map(obs -> {
                    final var result = new ArrayList<ObservationSpec>(obs.length);
                    for (final ObservationSpec spec : obs) {
                        result.add(spec);
                    }
                    return result;
                })
                .orElseGet(() -> new ArrayList<>(0));
        this.request = request;
    }

    /**
     * Gets meta data about observations that need to be established before
     * the command request is being sent to the device.
     *
     * @return An unmodifiable view on the observations.
     */
    public List<ObservationSpec> getRequiredObservations() {
        return Collections.unmodifiableList(requiredObservations);
    }

    /**
     * Gets the request that the command has been mapped to.
     *
     * @return The request.
     */
    public DownlinkRequest<? extends LwM2mResponse> getRequest() {
        return request;
    }
}
