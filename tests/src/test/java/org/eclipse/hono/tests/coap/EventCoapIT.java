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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.tests.CommandEndpointConfiguration.ClientType;
import org.eclipse.hono.tests.CommandEndpointConfiguration.TelemetryEndpoint;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;


/**
 * Integration tests for uploading telemetry data to the CoAP adapter.
 *
 */
@ExtendWith(VertxExtension.class)
public class EventCoapIT extends CoapTestBase {

    /**
     * Creates the endpoint configuration variants for Command &amp; Control scenarios.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> commandAndControlVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.EVENT),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_ALL_DEVICES, TelemetryEndpoint.EVENT),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.EVENT)
                );
    }

    /**
     * Creates the endpoint configuration variants for scenarios where
     * authenticated devices upload events.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> authenticatedDeviceTelemetryVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.EVENT, true)
                );
    }

    /**
     * Creates the endpoint configuration variants for scenarios where
     * devices upload events.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> deviceTelemetryVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.EVENT, true),
                new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.EVENT, false)
                );
    }

    /**
     * Creates the endpoint configuration variants for scenarios where
     * gateways upload events on behalf of devices.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> authenticatedGatewayTelemetryVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.EVENT, true)
                );
    }

    /**
     * Creates the endpoint configuration variants for scenarios where
     * gateways upload events on behalf of devices.
     * 
     * @return The configurations.
     */
    static Stream<CoapCommandEndpointConfiguration> gatewayTelemetryVariants() {
        return Stream.of(
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.EVENT, true),
                new CoapCommandEndpointConfiguration(ClientType.GATEWAY_FOR_SINGLE_DEVICE, TelemetryEndpoint.EVENT, false)
                );
    }

    @Override
    protected Future<MessageConsumer> createConsumer(final String tenantId, final Consumer<Message> messageConsumer) {

        return helper.applicationClientFactory.createEventConsumer(tenantId, messageConsumer, remoteClose -> {});
    }

    @Override
    protected void assertAdditionalMessageProperties(final VertxTestContext ctx, final Message msg) {
        // assert that events are marked as "durable"
        ctx.verify(() -> {
            assertThat(msg.isDurable()).isTrue();
        });
    }
}
