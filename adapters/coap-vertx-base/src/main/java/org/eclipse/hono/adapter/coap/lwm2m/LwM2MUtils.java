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

import java.util.Map;
import java.util.Optional;

import org.eclipse.hono.auth.Device;
import org.eclipse.hono.tracing.TracingHelper;
import org.eclipse.leshan.server.registration.Registration;

import io.opentracing.tag.StringTag;

/**
 * Helper methods for LwM2M.
 *
 */
final class LwM2MUtils {

    static final StringTag TAG_LWM2M_REGISTRATION_ID = new StringTag("lwm2m_registration_id");
    static final StringTag TAG_LWM2M_RESOURCE = new StringTag("path");
    static final String KEY_NOTIFICATION_ENDPOINT = "hono.notification.endpoint";

    private LwM2MUtils() {
        // prevent instantiation
    }

    static <T> T getValue(final Map<String, Object> props, final String key, final Class<T> type) {
        return Optional.ofNullable(props.get(key))
                .filter(type::isInstance)
                .map(type::cast)
                .orElse(null);
    }

    /**
     * Determines the device that has created a LwM2M registration.
     * <p>
     * Uses {@link #getTenantId(Registration)} and {@link #getDevice(Registration)}
     * to determine the tenant and device ID from the registration's additional attributes.
     *
     * @param registration The LwM2M registration.
     * @return The device.
     * @throws IllegalArgumentException if the registration does not contain the required
     *                                  additional attributes to determine the device.
     */
    static Device getDevice(final Registration registration) {
        return new Device(getTenantId(registration), getDeviceId(registration));
    }

    /**
     * Gets the tenant of the device that has created a given LwM2M registration.
     *
     * @param registration The LwM2M registration.
     * @return The value of the additional registration attribute for key {@link TracingHelper#TAG_TENANT_ID}.
     */
    static String getTenantId(final Registration registration) {
        return registration.getAdditionalRegistrationAttributes().get(TracingHelper.TAG_TENANT_ID.getKey());
    }

    /**
     * Gets the identifier of the device that has created a given LwM2M registration.
     *
     * @param registration The LwM2M registration.
     * @return The value of the additional registration attribute for key {@link TracingHelper#TAG_DEVICE_ID}.
     */
    static String getDeviceId(final Registration registration) {
        return registration.getAdditionalRegistrationAttributes().get(TracingHelper.TAG_DEVICE_ID.getKey());
    }
}
