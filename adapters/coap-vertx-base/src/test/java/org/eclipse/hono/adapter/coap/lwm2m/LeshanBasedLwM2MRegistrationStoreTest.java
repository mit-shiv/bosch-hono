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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.eclipse.californium.core.coap.CoAP.Code;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.core.coap.MessageObserver;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.coap.Response;
import org.eclipse.californium.core.coap.Token;
import org.eclipse.californium.core.network.RandomTokenGenerator;
import org.eclipse.californium.core.network.TokenGenerator;
import org.eclipse.californium.core.network.TokenGenerator.Scope;
import org.eclipse.californium.core.network.config.NetworkConfig;
import org.eclipse.californium.core.observe.Observation;
import org.eclipse.californium.elements.AddressEndpointContext;
import org.eclipse.hono.adapter.client.command.Command;
import org.eclipse.hono.adapter.client.command.CommandContext;
import org.eclipse.hono.adapter.client.command.CommandResponse;
import org.eclipse.hono.adapter.coap.ResourceTestBase;
import org.eclipse.hono.service.metric.MetricsTags.Direction;
import org.eclipse.hono.service.metric.MetricsTags.EndpointType;
import org.eclipse.hono.service.metric.MetricsTags.ProcessingOutcome;
import org.eclipse.hono.test.VertxMockSupport;
import org.eclipse.hono.tracing.TracingHelper;
import org.eclipse.hono.util.Constants;
import org.eclipse.hono.util.QoS;
import org.eclipse.hono.util.RegistrationAssertion;
import org.eclipse.hono.util.ResourceIdentifier;
import org.eclipse.hono.util.TenantObject;
import org.eclipse.leshan.core.Link;
import org.eclipse.leshan.core.node.LwM2mPath;
import org.eclipse.leshan.core.request.BindingMode;
import org.eclipse.leshan.core.request.ContentFormat;
import org.eclipse.leshan.core.request.ExecuteRequest;
import org.eclipse.leshan.core.request.Identity;
import org.eclipse.leshan.core.request.WriteRequest;
import org.eclipse.leshan.server.californium.registration.CaliforniumRegistrationStore;
import org.eclipse.leshan.server.californium.registration.InMemoryRegistrationStore;
import org.eclipse.leshan.server.registration.Registration;
import org.eclipse.leshan.server.registration.RegistrationUpdate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import io.micrometer.core.instrument.Timer.Sample;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.noop.NoopTracerFactory;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;


/**
 * Tests verifying behavior of {@link LeshanBasedLwM2MRegistrationStore}.
 *
 */
@ExtendWith(VertxExtension.class)
@Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
public class LeshanBasedLwM2MRegistrationStoreTest extends ResourceTestBase {

    private static final String DEVICE_ID = "device";
    private static final String TENANT_ID = Constants.DEFAULT_TENANT;

    private final TokenGenerator tokenGenerator = new RandomTokenGenerator(NetworkConfig.createStandardWithoutFile());

    private LeshanBasedLwM2MRegistrationStore store;
    private Tracer tracer = NoopTracerFactory.create();
    private Identity identity;
    private CaliforniumRegistrationStore observationStore;
    private InetSocketAddress clientAddress = InetSocketAddress.createUnresolved("localhost", 15000);
    private LwM2MMessageMapping messageMapping;

    /**
     * Sets up the fixture.
     */
    @BeforeEach
    void setUp(final VertxTestContext ctx) {
        givenAnAdapter(properties);
        observationStore = new InMemoryRegistrationStore();
        messageMapping = mock(LwM2MMessageMapping.class);
        when(messageMapping.mapDownstreamMessage(any(NotificationContext.class), any(ResourceIdentifier.class), any(RegistrationAssertion.class)))
            .then(invocation -> {
                final NotificationContext context = invocation.getArgument(0);
                return Future.succeededFuture(new MappedDownstreamMessage(
                        invocation.getArgument(1),
                        context.getContentTypeAsString(),
                        context.getPayload(),
                        context.getDownstreamMessageProperties()));
            });
        store = new LeshanBasedLwM2MRegistrationStore(observationStore, adapter, tracer);
        store.setResourcesToObserve(Map.of());
        store.setMessageMapping(messageMapping);
        store.start().onComplete(ctx.completing());
        identity = Identity.psk(clientAddress, "device-identity");
    }

    private void givenSucceedingObservationOfResource(
            final String resourcePath) {
        givenSucceedingObservationOfResource(resourcePath, tokenGenerator.createToken(Scope.SHORT_TERM));
    }

    private void givenSucceedingObservationOfResource(
            final String resourcePath,
            final Token token) {

        doAnswer(invocation -> {
            final Request req = invocation.getArgument(0);
            if (req.getCode() == Code.GET && req.getOptions().hasObserve()
                    && req.getOptions().getUriPathString().equals(resourcePath)) {
                // set token on request which is required by Leshan's message observer handling the response
                req.setToken(token);

                // and let device accept the observe request
                final var observeResponse = new Response(ResponseCode.CONTENT);
                observeResponse.getOptions().setObserve(1);
                observeResponse.getOptions().setContentFormat(MediaTypeRegistry.APPLICATION_VND_OMA_LWM2M_TLV);
                req.getMessageObservers()
                    .forEach(obs -> {
                        // required to prevent NPE when processing response
                        obs.onReadyToSend();
                        obs.onResponse(observeResponse);
                    });
                // add observation to Californium CoapEndpoint's ObservationStore
                // so that it can be found (and canceled) when removing the LwM2M registration
                // in a "non-mocked" environment this would be done automatically under the hood
                // by the CoapEndpoint implementation
                observationStore.putIfAbsent(token, new Observation(
                        req,
                        new AddressEndpointContext(clientAddress)));
            }
            return null;
        }).when(secureEndpoint).sendRequest(any(Request.class));
    }

    private Registration newRegistration(
            final String registrationId,
            final String endpoint,
            final String lwm2mVersion,
            final Integer lifetime,
            final BindingMode bindingMode) {

        final Link[] objectLinks = Link.parse("</3/0>,</5/0>".getBytes(StandardCharsets.UTF_8));

        return new Registration.Builder("reg-id", endpoint, identity)
                .lwM2mVersion(lwm2mVersion)
                .lifeTimeInSec(lifetime.longValue())
                .bindingMode(bindingMode)
                .objectLinks(objectLinks)
                .additionalRegistrationAttributes(Map.of(
                        TracingHelper.TAG_TENANT_ID.getKey(), TENANT_ID,
                        TracingHelper.TAG_DEVICE_ID.getKey(), DEVICE_ID))
                .build();
    }

    private Token assertObserveRelationEstablished(final String resourcePath) {

        verify(secureEndpoint).sendRequest(argThat(r -> r.getCode() == Code.GET
                && r.getOptions().hasObserve()
                && r.getOptions().getUriPathString().equals(resourcePath)));
        return null;
    }

    private void assertCommandResponseHasBeenSentDownstream(
            final String expectedCorrelationId,
            final int expectedStatus,
            final String expectedTenantId,
            final String expectedDeviceId) {

        final var cmdResponse = ArgumentCaptor.forClass(CommandResponse.class);
        verify(commandResponseSender).sendCommandResponse(cmdResponse.capture(), any());
        assertThat(cmdResponse.getValue().getCorrelationId()).isEqualTo(expectedCorrelationId);
        assertThat(cmdResponse.getValue().getStatus()).isEqualTo(expectedStatus);
        assertThat(cmdResponse.getValue().getTenantId()).isEqualTo(expectedTenantId);
        assertThat(cmdResponse.getValue().getDeviceId()).isEqualTo(expectedDeviceId);
    }

    /**
     * Verifies that the store sends an empty notification downstream that corresponds to the
     * binding mode used by the device. Also verifies that the resources configured for the
     * device are getting observed.
     *
     * @param endpoint The endpoint name that the device uses when registering.
     * @param bindingMode The binding mode that the device uses when registering.
     * @param lifetime The lifetime that the device uses when registering.
     * @param lwm2mVersion The version of the LwM2M enabler that the device uses when registering.
     * @param notificationEndpoint The type of endpoint to use for forwarding notifications.
     * @param ctx The vert.x test context.
     */
    @ParameterizedTest
    @CsvSource(value = { "test-ep,U,84600,v1.0,TELEMETRY", "test-ep,UQ,300,v1.0.2,EVENT"})
    public void testAddRegistrationSucceeds(
            final String endpoint,
            final BindingMode bindingMode,
            final Integer lifetime,
            final String lwm2mVersion,
            final EndpointType notificationEndpoint,
            final VertxTestContext ctx) {

        // GIVEN an adapter with a downstream consumer
        givenATelemetrySenderForAnyTenant();
        givenAnEventSenderForAnyTenant();
        givenSucceedingObservationOfResource("3/0");
        store.setResourcesToObserve(Map.of("/3/0", notificationEndpoint));

        // WHEN a device registers the standard Device object
        final var registrationId = "reg-id";
        final var registration = newRegistration(registrationId, endpoint, lwm2mVersion, lifetime, bindingMode);

        store.addRegistration(registration, NoopSpan.INSTANCE.context())
            .onComplete(ctx.succeeding(ok -> {
                ctx.verify(() -> {
                    // THEN the store sends an empty notification downstream corresponding
                    // to the binding mode,
                    verify(adapter).sendTtdEvent(
                            eq(TENANT_ID),
                            eq(DEVICE_ID),
                            isNull(),
                            eq(BindingMode.U == bindingMode ? lifetime : 20),
                            any());
                });
                // opens a command consumer for the device
                ctx.verify(() -> {
                    verify(commandConsumerFactory).createCommandConsumer(
                            eq(TENANT_ID),
                            eq(DEVICE_ID),
                            VertxMockSupport.anyHandler(),
                            isNull(),
                            any());
                });
                ctx.verify(() -> {
                    // and establishes an observe relation for the Device object
                    assertObserveRelationEstablished("3/0");
                    // THEN the store forwards the observe response downstream
                    if (notificationEndpoint == EndpointType.TELEMETRY) {
                        assertTelemetryMessageHasBeenSentDownstream(
                                QoS.AT_MOST_ONCE,
                                TENANT_ID,
                                DEVICE_ID,
                                MediaTypeRegistry.toString(MediaTypeRegistry.APPLICATION_VND_OMA_LWM2M_TLV));
                    }
                    if (notificationEndpoint == EndpointType.EVENT) {
                        assertEventHasBeenSentDownstream(
                                TENANT_ID,
                                DEVICE_ID,
                                MediaTypeRegistry.toString(MediaTypeRegistry.APPLICATION_VND_OMA_LWM2M_TLV));
                    }
                });
                ctx.completeNow();
            }));
    }

    /**
     * Verifies that the store sends an empty notification downstream that corresponds to the
     * binding mode used by the device.
     *
     * @param ctx The vert.x test context.
     */
    @Test
    public void testUpdateRegistrationSucceeds(final VertxTestContext ctx) {

        // GIVEN a registered device
        givenATelemetrySenderForAnyTenant();
        givenSucceedingObservationOfResource("3/0");
        store.setResourcesToObserve(Map.of("/3/0", EndpointType.TELEMETRY));

        final var registrationId = "reg-id";
        final var originalRegistration = newRegistration(registrationId, "device-ep", "v1.0.2", 84600, BindingMode.U);

        store.addRegistration(originalRegistration, null)
            .compose(ok -> {
                ctx.verify(() -> {
                    // with an established observe relation for the Device object
                    assertObserveRelationEstablished("3/0");
                });
                // WHEN the device updates its registration with a new lifetime
                final var registrationUpdate = new RegistrationUpdate(
                        registrationId,
                        identity,
                        300L,
                        null,
                        null,
                        null,
                        null);
                return store.updateRegistration(registrationUpdate, null);
            })
            .onComplete(ctx.succeeding(ok -> {
                ctx.verify(() -> {
                    // THEN the device's command consumer is being kept open
                    verify(commandConsumer, never()).close(any());
                    // and an empty notification is being sent downstream
                    // with a TTD reflecting the updated lifetime
                    verify(adapter).sendTtdEvent(
                            eq(TENANT_ID),
                            eq(DEVICE_ID),
                            isNull(),
                            eq(300),
                            any());
                    // and the observation of the Device object is kept alive
                    verify(secureEndpoint, never()).cancelObservation(any(Token.class));
                });
                ctx.completeNow();
            }));
    }

    /**
     * Verifies that the store sends an empty notification downstream with a TTD of 0.
     * Also verifies that the command consumer for the device is being closed and that observations for
     * the device are being canceled.
     *
     * @param ctx The vert.x test context.
     */
    @Test
    public void testRemoveRegistrationSucceeds(final VertxTestContext ctx) {

        // GIVEN a registered device
        final var token = tokenGenerator.createToken(Scope.SHORT_TERM);
        givenATelemetrySenderForAnyTenant();
        givenSucceedingObservationOfResource("3/0", token);
        store.setResourcesToObserve(Map.of("/3/0", EndpointType.TELEMETRY));

        final var registrationId = "reg-id";
        final var originalRegistration = newRegistration(registrationId, "device-ep", "v1.0.2", 84600, BindingMode.U);

        store.addRegistration(originalRegistration, null)
            .onComplete(ctx.succeeding(ok -> {
                ctx.verify(() -> {
                    // with an established observe relation for the Device object
                    assertObserveRelationEstablished("3/0");
                });
                // WHEN the device deregisters
                store.removeRegistration(registrationId, null);
                ctx.verify(() -> {
                    // THEN the device's command consumer is being closed
                    verify(commandConsumer).close(any());
                    // and an empty notification is being sent downstream
                    verify(adapter).sendTtdEvent(
                            eq(TENANT_ID),
                            eq(DEVICE_ID),
                            isNull(),
                            eq(0),
                            any());
                    // and the observation of the Device object is being canceled
                    verify(secureEndpoint).cancelObservation(token);
                    // and no more observations are registered
                    assertThat(observationStore.getObservations(registrationId)).isEmpty();
                });
                ctx.completeNow();
            }));
    }

    /**
     * Verifies that a LwM2M device can be rebooted by means of sending a command to
     * the device.
     *
     * @param ctx The vert.x test context.
     */
    @Test
    public void testDeviceCanBeRebooted(final VertxTestContext ctx) {

        // GIVEN a registered device
        givenAnEventSenderForAnyTenant();
        givenATelemetrySenderForAnyTenant();
        givenACommandResponseSenderForAnyTenant();

        final var registrationId = "reg-id";
        final var originalRegistration = newRegistration(registrationId, "device-ep", "v1.0.2", 84600, BindingMode.U);

        // to which an application wants to send a reboot command
        final Command command = mock(Command.class);
        when(command.getName()).thenReturn("reboot");
        when(command.getCorrelationId()).thenReturn("cor-id");
        when(command.isOneWay()).thenReturn(Boolean.FALSE);
        when(command.getTenant()).thenReturn(TENANT_ID);
        when(command.getDeviceId()).thenReturn(DEVICE_ID);
        final CommandContext commandContext = mock(CommandContext.class);
        when(commandContext.getCommand()).thenReturn(command);
        when(commandContext.getTracingSpan()).thenReturn(mock(Span.class));

        when(messageMapping.mapUpstreamMessage(any(RegistrationAssertion.class), any(CommandContext.class), any(Registration.class)))
            .thenReturn(Future.succeededFuture(new MappedUpstreamMessage(new ExecuteRequest(3, 0, 4))));

        store.addRegistration(originalRegistration, null)
            .map(ok -> {
                final ArgumentCaptor<Handler<CommandContext>> commandHandler = VertxMockSupport.argumentCaptorHandler();
                ctx.verify(() -> {
                    verify(commandConsumerFactory).createCommandConsumer(
                            eq(TENANT_ID),
                            eq(DEVICE_ID),
                            commandHandler.capture(),
                            isNull(),
                            any());
                });
                return commandHandler.getValue();
            })
            .onSuccess(commandHandler -> {
                // WHEN an application sends a reboot command to the client
                commandHandler.handle(commandContext);
            })
            .map(commandHandler -> {
                final ArgumentCaptor<Request> request = ArgumentCaptor.forClass(Request.class);
                ctx.verify(() -> {
                    // THEN the command is forwarded to the device as a CoAP POST request
                    verify(secureEndpoint).sendRequest(request.capture());
                    assertThat(request.getValue().getCode()).isEqualTo(Code.POST);
                    assertThat(request.getValue().getOptions().getUriPathString()).isEqualTo("3/0/4");
                });
                return request.getValue();
            })
            .onSuccess(request -> {
                // and WHEN the device acknowledges the request
                final List<MessageObserver> observers = request.getMessageObservers();
                observers.forEach(obs -> {
                    // this is a hack to prevent NPE in message observer when handling the response
                    // in org.eclipse.leshan.core.californium.CoapAsyncRequestObserver#onResponse(Response)
                    obs.onReadyToSend();
                    obs.onAcknowledgement();
                    });
                ctx.verify(() -> {
                    // THEN the command request has been marked as accepted 
                    verify(commandContext).accept();
                    // and corresponding metrics have been reported
                    verify(metrics).reportCommand(
                            eq(Direction.REQUEST),
                            eq(TENANT_ID),
                            any(TenantObject.class),
                            eq(ProcessingOutcome.FORWARDED),
                            eq(0),
                            any(Sample.class));
                });
                // and WHEN the device returns its response to the adapter
                final var response = new Response(ResponseCode.CHANGED);
                observers.forEach(obs -> obs.onResponse(response));
            })
            .onComplete(ctx.succeeding(ok -> {
                ctx.verify(() -> {
                    // the device's response is returned back to the downstream application
                    assertCommandResponseHasBeenSentDownstream(
                            "cor-id",
                            HttpURLConnection.HTTP_NO_CONTENT,
                            TENANT_ID,
                            DEVICE_ID);
                    // and command response metrics have been reported
                    verify(metrics).reportCommand(
                            eq(Direction.RESPONSE),
                            eq(TENANT_ID),
                            any(TenantObject.class),
                            eq(ProcessingOutcome.FORWARDED),
                            eq(0),
                            any(Sample.class));
                });
                ctx.completeNow();
            }));
    }

    /**
     * Verifies that a LwM2M device's Firmware update URI can be set by means of sending a command to
     * the device.
     *
     * @param ctx The vert.x test context.
     */
    @Test
    public void testFirmwareUriCanBeSetOnDevice(final VertxTestContext ctx) {

        // GIVEN a registered device
        givenAnEventSenderForAnyTenant();
        givenACommandResponseSenderForAnyTenant();
        givenSucceedingObservationOfResource("5/0");

        final var uri = "https://firmware.eclipse.org/tenant/device";
        final var registrationId = "reg-id";
        final var originalRegistration = newRegistration(registrationId, "device-ep", "v1.0.2", 84600, BindingMode.U);

        // for which an application wants to set the URI to download firmware from
        final Command command = mock(Command.class);
        when(command.getName()).thenReturn("set package URI");
        when(command.isOneWay()).thenReturn(Boolean.TRUE);
        when(command.getTenant()).thenReturn(TENANT_ID);
        when(command.getDeviceId()).thenReturn(DEVICE_ID);
        when(command.getContentType()).thenReturn("text/plain");
        when(command.getPayload()).thenReturn(Buffer.buffer(uri));
        final CommandContext commandContext = mock(CommandContext.class);
        when(commandContext.getCommand()).thenReturn(command);
        when(commandContext.getTracingSpan()).thenReturn(mock(Span.class));

        when(messageMapping.mapUpstreamMessage(any(RegistrationAssertion.class), any(CommandContext.class), any(Registration.class)))
            .thenReturn(Future.succeededFuture(new MappedUpstreamMessage(
                    new WriteRequest(ContentFormat.TEXT, 5, 0, 1, uri),
                    ObservationSpec.from(new LwM2mPath(5, 0), EndpointType.EVENT))));

        store.addRegistration(originalRegistration, null)
            .map(ok -> {
                final ArgumentCaptor<Handler<CommandContext>> commandHandler = VertxMockSupport.argumentCaptorHandler();
                ctx.verify(() -> {
                    verify(commandConsumerFactory).createCommandConsumer(
                            eq(TENANT_ID),
                            eq(DEVICE_ID),
                            commandHandler.capture(),
                            isNull(),
                            any());
                });
                return commandHandler.getValue();
            })
            .onSuccess(commandHandler -> {
                // WHEN an application sends the command to the client
                commandHandler.handle(commandContext);
            })
            .onComplete(ctx.succeeding(ok -> {
                final var request = ArgumentCaptor.forClass(Request.class);
                ctx.verify(() -> {

                    // THEN an observe relation for the Firmware object has been established
                    assertObserveRelationEstablished("5/0");
                    // and the command is forwarded to the device as a CoAP PUT request
                    verify(secureEndpoint, times(2)).sendRequest(request.capture());
                    final var commandRequest = request.getAllValues().get(1);
                    assertThat(commandRequest.getCode()).isEqualTo(Code.PUT);
                    assertThat(commandRequest.getOptions().getUriPathString()).isEqualTo("5/0/1");
                    assertThat(commandRequest.getPayloadString()).isEqualTo(uri);

                    // and WHEN the device acknowledges the request
                    final List<MessageObserver> observers = commandRequest.getMessageObservers();
                    observers.forEach(obs -> {
                        // this is a hack to prevent NPE in message observer when handling the response
                        // in org.eclipse.leshan.core.californium.CoapAsyncRequestObserver#onResponse(Response)
                        obs.onReadyToSend();
                        obs.onAcknowledgement();
                        });

                    // THEN the command request has been marked as accepted 
                    verify(commandContext).accept();
                    // and corresponding metrics have been reported
                    verify(metrics).reportCommand(
                            eq(Direction.ONE_WAY),
                            eq(TENANT_ID),
                            any(TenantObject.class),
                            eq(ProcessingOutcome.FORWARDED),
                            anyInt(),
                            any(Sample.class));

                    // and WHEN the device returns its response to the adapter
                    final var response = new Response(ResponseCode.CHANGED);
                    observers.forEach(obs -> obs.onResponse(response));
                    // THEN the device's response is not sent downstream
                    assertNoCommandResponseHasBeenSentDownstream();
                    // and no corresponding metrics have been reported
                    verify(metrics, never()).reportCommand(
                            eq(Direction.RESPONSE),
                            anyString(),
                            any(TenantObject.class),
                            any(ProcessingOutcome.class),
                            anyInt(),
                            any(Sample.class));
                });
                ctx.completeNow();
            }));
    }

}
