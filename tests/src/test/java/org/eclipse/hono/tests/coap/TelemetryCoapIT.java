/*******************************************************************************
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
 *******************************************************************************/

package org.eclipse.hono.tests.coap;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.tests.CommandEndpointConfiguration.ClientType;
import org.eclipse.hono.tests.CommandEndpointConfiguration.TelemetryEndpoint;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;


/**
 * Integration tests for uploading telemetry data to the CoAP adapter.
 *
 */
@ExtendWith(VertxExtension.class)
public class TelemetryCoapIT extends CoapTestBase {

    /**
     * Creates the endpoint configuration variants for Command &amp; Control scenarios.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> commandAndControlVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_ALL_DEVICES, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE),

                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_ALL_DEVICES, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE)
                );
    }

    /**
     * Creates the endpoint configuration variants for scenarios where
     * authenticated devices upload telemetry data.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> authenticatedDeviceTelemetryVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE, true),
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE, true)
                );
    }

    /**
     * Creates the endpoint configuration variants for scenarios where
     * devices upload telemetry data.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> deviceTelemetryVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE, true),
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE, false),
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE, true),
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE, false)
                );
    }

    /**
     * Creates the endpoint configuration variants for scenarios where
     * gateways upload telemetry data on behalf of devices.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> authenticatedGatewayTelemetryVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE, true),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE, true)
                );
    }

    /**
     * Creates the endpoint configuration variants for scenarios where
     * gateways upload telemetry data on behalf of devices.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> gatewayTelemetryVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE, true),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.TELEMETRY_AT_MOST_ONCE, false),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE, true),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE, false)
                );
    }

    @Override
    protected Future<MessageConsumer> createConsumer(final String tenantId, final Consumer<Message> messageConsumer) {

        return helper.applicationClientFactory.createTelemetryConsumer(tenantId, messageConsumer, remoteClose -> {});
    }
}
