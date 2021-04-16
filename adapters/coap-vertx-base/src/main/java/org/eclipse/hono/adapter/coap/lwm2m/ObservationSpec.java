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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.eclipse.hono.service.metric.MetricsTags.EndpointType;
import org.eclipse.leshan.core.node.LwM2mPath;

/**
 * A container for defining a resource to observe and the endpoint that should be used
 * to forward notifications downstream.
 */
public final class ObservationSpec {

    private final LwM2mPath path;
    private final EndpointType notificationEndpoint;
    private final Map<String, String> observationProperties;

    private ObservationSpec(
            final LwM2mPath path,
            final EndpointType endpoint,
            final Map<String, String> props) {
        this.path = Objects.requireNonNull(path);
        this.notificationEndpoint = Objects.requireNonNull(endpoint);
        this.observationProperties = Optional.ofNullable(props).orElseGet(HashMap::new);
        this.observationProperties.put(LwM2MUtils.KEY_NOTIFICATION_ENDPOINT, endpoint.getCanonicalName());
    }

    /**
     * Creates a new container for initial observation information.
     * <p>
     * The given endpoint's canonical name will be put to the notification properties
     * under key {@value LwM2MUtils#KEY_NOTIFICATION_ENDPOINT}.
     *
     * @param resourcePath The path of the resource that should be observed.
     * @param notificationEndpoint The endpoint to forward notifications to.
     * @return The new container.
     * @throws NullPointerException if resource path or endpoint are {@code null}.
     */
    public static ObservationSpec from(
            final LwM2mPath resourcePath,
            final EndpointType notificationEndpoint) {

        return new ObservationSpec(resourcePath, notificationEndpoint, null);
    }

    /**
     * Creates a new container for initial observation information.
     * <p>
     * The given endpoint's canonical name will be put to the notification properties
     * under key {@value LwM2MUtils#KEY_NOTIFICATION_ENDPOINT}.
     *
     * @param resourcePath The path of the resource that should be observed.
     * @param notificationEndpoint The endpoint to forward notifications to.
     * @param notificationProperties Additional properties that must be included with
     *                               each notification being forwarded downstream.
     * @return The new container.
     * @throws NullPointerException if resource path or endpoint are {@code null}.
     */
    public static ObservationSpec from(
            final LwM2mPath resourcePath,
            final EndpointType notificationEndpoint,
            final Map<String, String> notificationProperties) {

        return new ObservationSpec(resourcePath, notificationEndpoint, notificationProperties);
    }

    /**
     * Gets the resource to observe.
     *
     * @return The path.
     */
    public LwM2mPath getPath() {
        return path;
    }

    /**
     * Gets the endpoint to forward notifications to.
     *
     * @return The endpoint.
     */
    public EndpointType getNotificationEndpoint() {
        return notificationEndpoint;
    }

    /**
     * Gets the additional properties to include with each notification being forwarded.
     *
     * @return The properties.
     */
    public Map<String, String> getObservationProperties() {
        return observationProperties;
    }
}
