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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Map;

import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.coap.Response;
import org.eclipse.californium.elements.AddressEndpointContext;
import org.eclipse.hono.adapter.client.command.Command;
import org.eclipse.hono.adapter.client.command.CommandContext;
import org.eclipse.hono.auth.Device;
import org.eclipse.hono.service.metric.MetricsTags.EndpointType;
import org.eclipse.hono.util.Constants;
import org.eclipse.hono.util.RegistrationAssertion;
import org.eclipse.hono.util.ResourceIdentifier;
import org.eclipse.leshan.core.LwM2mId;
import org.eclipse.leshan.core.ResponseCode;
import org.eclipse.leshan.core.node.LwM2mNode;
import org.eclipse.leshan.core.node.LwM2mObjectInstance;
import org.eclipse.leshan.core.node.LwM2mPath;
import org.eclipse.leshan.core.node.LwM2mSingleResource;
import org.eclipse.leshan.core.observation.Observation;
import org.eclipse.leshan.core.request.ExecuteRequest;
import org.eclipse.leshan.core.request.Identity;
import org.eclipse.leshan.core.request.WriteRequest;
import org.eclipse.leshan.core.response.ObserveResponse;
import org.eclipse.leshan.server.registration.Registration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.micrometer.core.instrument.Timer.Sample;
import io.opentracing.Span;
import io.opentracing.noop.NoopSpan;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;


/**
 * A DittoProtocolMessageMappingTest.
 *
 */
@ExtendWith(VertxExtension.class)
public class DittoProtocolMessageMappingTest {

    private static final String DEVICE_ID = "device";
    private static final String TENANT_ID = Constants.DEFAULT_TENANT;

    private DittoProtocolMessageMapping mapping;
    private Registration registration;
    private InetSocketAddress clientAddress = InetSocketAddress.createUnresolved("localhost", 15000);

    /**
     */
    @BeforeEach
    void setUp() {
        mapping = new DittoProtocolMessageMapping();
        registration = new Registration.Builder(
                "reg-id",
                "test-ep",
                Identity.psk(clientAddress, "device-identity")).build();

    }

    private CommandContext newCommandContext(
            final String topic,
            final String path,
            final JsonObject value) {

        final JsonObject commandPayload = new JsonObject()
                .put("topic", topic)
                .put("headers", new JsonObject())
                .put("path", path)
                .put("value", value);
        final Command command = mock(Command.class);
        when(command.getName()).thenReturn("modified");
        when(command.isOneWay()).thenReturn(Boolean.TRUE);
        when(command.getTenant()).thenReturn(TENANT_ID);
        when(command.getDeviceId()).thenReturn(DEVICE_ID);
        when(command.getContentType()).thenReturn("application/vnd.eclipse.ditto+json");
        when(command.getPayload()).thenReturn(commandPayload.toBuffer());
        final CommandContext commandContext = mock(CommandContext.class);
        when(commandContext.getCommand()).thenReturn(command);
        when(commandContext.getTracingSpan()).thenReturn(mock(Span.class));
        return commandContext;
    }

    private JsonArray newSoftwareModules(final String uri) {
        return new JsonArray().add(new JsonObject()
                .put("softwareModule", new JsonObject()
                        .put("name", "bumlux-fw")
                        .put("version", "v1.5.3"))
                .put("artifacts", new JsonArray().add(new JsonObject()
                        .put("download", new JsonObject()
                                .put("HTTPS", new JsonObject().put("url", uri))))));
    }

    @Test
    void testMapUpstreamSoftwareUpdatableDownloadModifiedEvent(final VertxTestContext ctx) {

        final var uri = "https://firmware.eclipse.org/tenant/device";
        final var commandContext = newCommandContext(
                String.format("test/%s:%s/things/twins/events/modified", TENANT_ID, DEVICE_ID),
                "features/softwareUpdateable/properties/download",
                new JsonObject()
                        .put("correlationId", "download-cor-id")
                        .put("softwareModules", newSoftwareModules(uri)));

        mapping.mapUpstreamMessage(new RegistrationAssertion(DEVICE_ID), commandContext, registration)
            .onComplete(ctx.succeeding(msg -> {
                ctx.verify(() -> {
                    assertThat(msg.getRequest()).isInstanceOfSatisfying(
                            WriteRequest.class,
                            req -> {
                                assertThat(req.getPath().getObjectId()).isEqualTo(LwM2mId.FIRMWARE);
                                assertThat(req.getPath().getResourceId()).isEqualTo(1);
                                assertThat(req.getNode()).isInstanceOfSatisfying(
                                        LwM2mSingleResource.class,
                                        node -> {
                                            assertThat(node.getValue()).isEqualTo(uri);
                                        });
                            });
                    assertRequiredObservationProperties(
                            msg.getRequiredObservations().stream().iterator().next(),
                            "/5/0",
                            EndpointType.EVENT,
                            Map.of(DittoProtocolMessageMapping.KEY_STATUS_CORRELATION_ID, "download-cor-id",
                                    DittoProtocolMessageMapping.KEY_STATUS_SOFTWARE_MODULE_NAME, "bumlux-fw",
                                    DittoProtocolMessageMapping.KEY_STATUS_SOFTWARE_MODULE_VERSION, "v1.5.3"));
                });
                ctx.completeNow();
            }));
    }

    @Test
    void testMapUpstreamSoftwareUpdatableInstallModifiedEvent(final VertxTestContext ctx) {

        final var uri = "https://firmware.eclipse.org/tenant/device";
        final var commandContext = newCommandContext(
                String.format("test/%s:%s/things/twins/events/modified", TENANT_ID, DEVICE_ID),
                "features/softwareUpdateable/properties/install",
                new JsonObject()
                        .put("correlationId", "install-cor-id")
                        .put("softwareModules", newSoftwareModules(uri)));

        mapping.mapUpstreamMessage(new RegistrationAssertion(DEVICE_ID), commandContext, registration)
            .onComplete(ctx.succeeding(msg -> {
                ctx.verify(() -> {
                    assertThat(msg.getRequest()).isInstanceOfSatisfying(
                            ExecuteRequest.class,
                            req -> {
                                assertThat(req.getPath().getObjectId()).isEqualTo(LwM2mId.FIRMWARE);
                                assertThat(req.getPath().getResourceId()).isEqualTo(2);
                            });
                    assertRequiredObservationProperties(
                            msg.getRequiredObservations().stream().iterator().next(),
                            "/5/0",
                            EndpointType.EVENT,
                            Map.of(DittoProtocolMessageMapping.KEY_STATUS_CORRELATION_ID, "install-cor-id",
                                    DittoProtocolMessageMapping.KEY_STATUS_SOFTWARE_MODULE_NAME, "bumlux-fw",
                                    DittoProtocolMessageMapping.KEY_STATUS_SOFTWARE_MODULE_VERSION, "v1.5.3"));
                });
                ctx.completeNow();
            }));
    }

    @Test
    void testMapFirmwareDownloadingNotification(final VertxTestContext ctx) {

        final var namespacedThing = String.format("test/%s:%s", TENANT_ID, DEVICE_ID);
        final var observation = new Observation(new byte[] { 0x00 }, "reg-id", new LwM2mPath("/5/0"), null, Map.of(
                DittoProtocolMessageMapping.KEY_DITTO_NAMESPACED_THING_NAME, namespacedThing,
                DittoProtocolMessageMapping.KEY_STATUS_CORRELATION_ID, "cor-id",
                DittoProtocolMessageMapping.KEY_STATUS_SOFTWARE_MODULE_NAME, "bumlux-fw",
                DittoProtocolMessageMapping.KEY_STATUS_SOFTWARE_MODULE_VERSION, "v1.5.3"));
        final LwM2mNode content = new LwM2mObjectInstance(
                0,
                LwM2mSingleResource.newIntegerResource(3, 1),
                LwM2mSingleResource.newIntegerResource(5, 0));
        final Request request = Request.newGet();
        request.setSourceContext(new AddressEndpointContext(clientAddress));
        final var coapResponse = new Response(org.eclipse.californium.core.coap.CoAP.ResponseCode.CONTENT);
        coapResponse.getOptions().setObserve(1);
        coapResponse.getOptions().setContentFormat(MediaTypeRegistry.APPLICATION_VND_OMA_LWM2M_TLV);
        final ObserveResponse observeResponse = new ObserveResponse(
                ResponseCode.CONTENT,
                content,
                null,
                observation,
                null,
                coapResponse);
        final NotificationContext notificationContext = NotificationContext.fromResponse(
                registration,
                observeResponse,
                new Device(TENANT_ID, DEVICE_ID),
                mock(Sample.class),
                NoopSpan.INSTANCE);

        mapping.mapDownstreamMessage(notificationContext, ResourceIdentifier.fromString("5/0"), new RegistrationAssertion(DEVICE_ID))
            .onComplete(ctx.succeeding(msg -> {
                ctx.verify(() -> {
                    assertThat(msg.getContentType()).isEqualTo(DittoProtocolMessageMapping.CONTENT_TYPE_DITTO);
                    final JsonObject dittoMessage = msg.getPayload().toJsonObject();
                    assertThat(dittoMessage.getString("topic"))
                        .isEqualTo(String.format("%s/things/twin/commands/merge", namespacedThing));
                    assertThat(dittoMessage.getJsonObject("headers").getMap())
                        .containsEntry("content-type", DittoProtocolMessageMapping.CONTENT_TYPE_MERGE_PATCH_JSON);
                    final var value = dittoMessage.getJsonObject("value");
                    assertThat(value.containsKey("lastFailedOperation")).isFalse();
                    assertOperationStatus(
                            value.getJsonObject("lastOperation"),
                            "DOWNLOADING",
                            "cor-id",
                            "bumlux-fw",
                            "v1.5.3");
                });
                ctx.completeNow();
            }));
    }

    private void assertOperationStatus(
            final JsonObject operationStatus,
            final String expectedStatus,
            final String expectedCorrelationId,
            final String expectedSoftwareModuleName,
            final String expectedSoftwareModuleVersion) {

        assertThat(operationStatus.getString("correlationId")).isEqualTo(expectedCorrelationId);
        assertThat(operationStatus.getString("status")).isEqualTo(expectedStatus);
        final var softwareModule = operationStatus.getJsonObject("softwareModule");
        assertThat(softwareModule.getString("name")).isEqualTo(expectedSoftwareModuleName);
        assertThat(softwareModule.getString("version")).isEqualTo(expectedSoftwareModuleVersion);
    }

    private void assertRequiredObservationProperties(
            final ObservationSpec observation,
            final String expectedPath,
            final EndpointType expectedEndpointType,
            final Map<String, String> expectedAdditionalProperties) {

        assertThat(observation.getPath().toString()).isEqualTo(expectedPath);
        assertThat(observation.getNotificationEndpoint()).isEqualTo(expectedEndpointType);
        assertThat(observation.getObservationProperties()).containsEntry(
                DittoProtocolMessageMapping.KEY_DITTO_NAMESPACED_THING_NAME,
                "test/DEFAULT_TENANT:device");
        if (expectedAdditionalProperties != null) {
            assertThat(observation.getObservationProperties()).containsAllEntriesOf(expectedAdditionalProperties);
        }
    }

}
