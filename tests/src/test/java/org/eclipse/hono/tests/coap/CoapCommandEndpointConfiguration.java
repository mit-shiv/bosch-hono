/**
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
 */


package org.eclipse.hono.tests.coap;

import org.eclipse.hono.tests.CommandEndpointConfiguration;
import org.eclipse.hono.util.CommandConstants;
import org.eclipse.hono.util.EventConstants;
import org.eclipse.hono.util.TelemetryConstants;


/**
 * CoAP adapter specific configuration properties for defining variants
 * how a device can interact with the CoAP adapter.
 *
 */
public final class CoapCommandEndpointConfiguration extends CommandEndpointConfiguration {

    /**
     * Creates a new configuration.
     * 
     * @param clientType The type of client connecting to the adapter.
     * @param telemetryEndpoint The endpoint to use for uploading data.
     */
    public CoapCommandEndpointConfiguration(final ClientType clientType, final TelemetryEndpoint telemetryEndpoint) {
        this(clientType, telemetryEndpoint, true);
    }

    /**
     * Creates a new configuration.
     * 
     * @param clientType The type of client connecting to the adapter.
     * @param telemetryEndpoint The endpoint to use for uploading data.
     * @param authenticated {@code true} if the device should authenticate to the adapter.
     */
    public CoapCommandEndpointConfiguration(final ClientType clientType, final TelemetryEndpoint telemetryEndpoint, final boolean authenticated) {
        super(clientType, telemetryEndpoint, authenticated);
    }

    /**
     * Gets the URI to use for uploading a response to a command.
     * 
     * @param tenantId The tenant that the device belongs to.
     * @param deviceId The identifier of the device.
     * @param reqId The command request ID.
     * @return The URI.
     */
    public String getCommandResponseUri(final String tenantId, final String deviceId, final String reqId) {
        if (isGatewayClient()) {
            return String.format("/%s/%s/%s/%s", CommandConstants.COMMAND_RESPONSE_ENDPOINT, tenantId, deviceId, reqId);
        }
        return String.format("/%s/%s", CommandConstants.COMMAND_RESPONSE_ENDPOINT, reqId);
    }

    /**
     * Gets the URI to use for uploading telemetry data.
     * 
     * @param tenantId The tenant that the device belongs to.
     * @param deviceId The identifier of the device.
     * @return The URI.
     */
    public String getTelemetryUri(final String tenantId, final String deviceId) {
        if (isGatewayClient() || !isAuthenticated()) {
            return String.format("/%s/%s/%s", TelemetryConstants.TELEMETRY_ENDPOINT, tenantId, deviceId);
        }
        return "/" + TelemetryConstants.TELEMETRY_ENDPOINT;
    }

    /**
     * Gets the URI to use for uploading events.
     * 
     * @param tenantId The tenant that the device belongs to.
     * @param deviceId The identifier of the device.
     * @return The URI.
     */
    public String getEventUri(final String tenantId, final String deviceId) {
        if (isGatewayClient() || !isAuthenticated()) {
            return String.format("/%s/%s/%s", EventConstants.EVENT_ENDPOINT, tenantId, deviceId);
        }
        return "/" + EventConstants.EVENT_ENDPOINT;
    }
}
