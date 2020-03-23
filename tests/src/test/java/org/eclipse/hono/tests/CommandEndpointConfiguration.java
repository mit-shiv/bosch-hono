/**
 * Copyright (c) 2019, 2020 Contributors to the Eclipse Foundation
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


package org.eclipse.hono.tests;

import org.eclipse.hono.util.CommandConstants;

/**
 * Configuration properties for defining variants of how clients interact
 * with a protocol adapter.
 *
 */
public class CommandEndpointConfiguration {

    private final ClientType clientType;
    private final TelemetryEndpoint telemetryEndpoint;
    private final boolean authenticated;


    /**
     * Defines the different types of clients that can connect to an adapter.
     */
    public enum ClientType {
        /**
         * A directly connected device.
         */
        DEVICE,
        /**
         * A gateway acting on behalf of all devices of a tenant.
         */
        GATEWAY_FOR_ALL_DEVICES,
        /**
         * A gateway acting on behalf of a single device.
         */
        GATEWAY_FOR_SINGLE_DEVICE
    }

    /**
     * The endpoint to use for uploading data to an adapter.
     *
     */
    public enum TelemetryEndpoint {

        TELEMETRY_AT_MOST_ONCE(DeliverySemantics.AT_MOST_ONCE),
        TELEMETRY_AT_LEAST_ONCE(DeliverySemantics.AT_LEAST_ONCE),
        EVENT(DeliverySemantics.AT_LEAST_ONCE);

        private final DeliverySemantics qos;

        TelemetryEndpoint(final DeliverySemantics qos) {
            this.qos = qos;
        }

        /**
         * Gets the delivery semantics associated with this endpoint.
         * 
         * @return The delivery semantics.
         */
        public final DeliverySemantics getDeliverySemantics() {
            return qos;
        }
    }

    /**
     * Creates a new configuration using the event endpoint for uploading data.
     * 
     * @param clientType The type of client connecting to the adapter.
     */
    public CommandEndpointConfiguration(final ClientType clientType) {
        this(clientType, TelemetryEndpoint.EVENT, true);
    }

    /**
     * Creates a new configuration using the event endpoint for uploading data.
     * 
     * @param clientType The type of client connecting to the adapter.
     * @param telemetryEndpoint The endpoint to use for uploading data.
     * @param authenticated {@code true} if the device should authenticate to the adapter.
     */
    public CommandEndpointConfiguration(
            final ClientType clientType,
            final TelemetryEndpoint telemetryEndpoint,
            final boolean authenticated) {
        this.clientType = clientType;
        this.telemetryEndpoint = telemetryEndpoint;
        this.authenticated = authenticated;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("Client [type: %s, authenticated: %b], telemetry endpoint: %s", clientType, authenticated, telemetryEndpoint);
    }

    /**
     * Checks if the device authenticates to the adapter.
     * 
     * @return {@code true} if the device authenticates to the adapter.
     */
    public final boolean isAuthenticated() {
        return authenticated;
    }

    /**
     * Gets the endpoint to use for uploading data.
     * 
     * @return The endpoint.
     */
    public final TelemetryEndpoint getTelemetryEndpoint() {
        return telemetryEndpoint;
    }

    /**
     * Gets the name of the command endpoint used by devices.
     * 
     * @return The command endpoint name.
     */
    public final String getSouthboundCommandEndpoint() {
        return CommandConstants.COMMAND_ENDPOINT;
    }

    /**
     * Gets the type of client connecting to the adapter.
     *
     * @return The type.
     */
    public ClientType getClientType() {
        return clientType;
    }

    /**
     * Checks whether the client is a gateway.
     *
     * @return {@code true} if the client is a gateway and acts on behalf of one
     *         or more other devices.
     */
    public boolean isGatewayClient() {
        return isGatewayClientForAllDevices() || isGatewayClientForSingleDevice();
    }

    /**
     * Checks if the client is a gateway acting on behalf of all of the tenant's devices.
     *
     * @return {@code true} if the client is a gateway for all devices.
     */
    public boolean isGatewayClientForAllDevices() {
        return clientType == ClientType.GATEWAY_FOR_ALL_DEVICES;
    }

    /**
     * Checks if the client is a gateway acting on behalf of a single device only.
     *
     * @return {@code true} if the client is a gateway for a single device.
     */
    public boolean isGatewayClientForSingleDevice() {
        return clientType == ClientType.GATEWAY_FOR_SINGLE_DEVICE;
    }

    /**
     * Gets the name of the endpoint that applications use for sending
     * commands to devices.
     * 
     * @return The endpoint name.
     */
    public final String getNorthboundCommandEndpoint() {
        return CommandConstants.COMMAND_ENDPOINT;
    }

    /**
     * Gets the target address to use in command messages sent to devices.
     * 
     * @param tenantId The tenant that the device belongs to.
     * @param deviceId The device identifier.
     * @return The target address.
     */
    public final String getCommandMessageAddress(final String tenantId, final String deviceId) {

        return String.format("%s/%s/%s", getNorthboundCommandEndpoint(), tenantId, deviceId);
    }

    /**
     * Gets the target address to use in a sender link for sending commands
     * to devices.
     * 
     * @param tenantId The tenant that the device belongs to.
     * @return The target address.
     */
    public final String getSenderLinkTargetAddress(final String tenantId) {

        return String.format("%s/%s", CommandConstants.COMMAND_ENDPOINT, tenantId);
    }
}
