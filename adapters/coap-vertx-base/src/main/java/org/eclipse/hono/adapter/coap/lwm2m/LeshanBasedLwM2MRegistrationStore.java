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

import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.core.coap.MessageObserverAdapter;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.coap.Response;
import org.eclipse.hono.adapter.client.command.Command;
import org.eclipse.hono.adapter.client.command.CommandConsumer;
import org.eclipse.hono.adapter.client.command.CommandContext;
import org.eclipse.hono.adapter.client.command.CommandResponse;
import org.eclipse.hono.adapter.client.command.Commands;
import org.eclipse.hono.adapter.coap.CoapConstants;
import org.eclipse.hono.adapter.coap.CoapProtocolAdapter;
import org.eclipse.hono.auth.Device;
import org.eclipse.hono.client.ClientErrorException;
import org.eclipse.hono.client.StatusCodeMapper;
import org.eclipse.hono.service.metric.MetricsTags.Direction;
import org.eclipse.hono.service.metric.MetricsTags.EndpointType;
import org.eclipse.hono.service.metric.MetricsTags.ProcessingOutcome;
import org.eclipse.hono.service.metric.MetricsTags.QoS;
import org.eclipse.hono.service.metric.MetricsTags.TtdStatus;
import org.eclipse.hono.tracing.TracingHelper;
import org.eclipse.hono.util.CommandConstants;
import org.eclipse.hono.util.Constants;
import org.eclipse.hono.util.Lifecycle;
import org.eclipse.hono.util.MessageHelper;
import org.eclipse.hono.util.RegistrationAssertion;
import org.eclipse.hono.util.ResourceIdentifier;
import org.eclipse.hono.util.TelemetryConstants;
import org.eclipse.hono.util.TenantObject;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeDecoder;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeEncoder;
import org.eclipse.leshan.core.node.codec.LwM2mNodeDecoder;
import org.eclipse.leshan.core.observation.Observation;
import org.eclipse.leshan.core.request.BindingMode;
import org.eclipse.leshan.core.request.ObserveRequest;
import org.eclipse.leshan.core.response.ObserveResponse;
import org.eclipse.leshan.server.californium.observation.ObservationServiceImpl;
import org.eclipse.leshan.server.californium.registration.CaliforniumRegistrationStore;
import org.eclipse.leshan.server.californium.request.CaliforniumLwM2mRequestSender;
import org.eclipse.leshan.server.model.LwM2mModelProvider;
import org.eclipse.leshan.server.model.StandardModelProvider;
import org.eclipse.leshan.server.observation.ObservationListener;
import org.eclipse.leshan.server.observation.ObservationService;
import org.eclipse.leshan.server.registration.ExpirationListener;
import org.eclipse.leshan.server.registration.Registration;
import org.eclipse.leshan.server.registration.RegistrationUpdate;
import org.eclipse.leshan.server.request.LwM2mRequestSender2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Timer.Sample;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;


/**
 * An Eclipse Leshan based registration store.
 *
 */
public class LeshanBasedLwM2MRegistrationStore implements LwM2MRegistrationStore, Lifecycle, ExpirationListener {

    private static final Logger LOG = LoggerFactory.getLogger(LeshanBasedLwM2MRegistrationStore.class);

    private final Map<String, CommandConsumer> commandConsumers = new ConcurrentHashMap<>();
    private final CoapProtocolAdapter adapter;
    private final Tracer tracer;
    private final CaliforniumRegistrationStore registrationStore;
    private final LwM2mNodeDecoder nodeDecoder = new DefaultLwM2mNodeDecoder();
    private final LwM2mModelProvider modelProvider = new StandardModelProvider();

    private LwM2mRequestSender2 lwm2mRequestSender;
    private ObservationService observationService;
    private Map<String, EndpointType> resourcesToObserve = new ConcurrentHashMap<>();
    private LwM2MMessageMapping messageMapping = new DittoProtocolMessageMapping();

    /**
     * Creates a new store.
     *
     * @param registrationStore The component to use for storing LwM2M registration information.
     * @param adapter The protocol adapter to use for interacting with Hono's service components
     *                and the messaging infrastructure.
     * @param tracer Open Tracing tracer to use for tracking the processing of requests.
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    public LeshanBasedLwM2MRegistrationStore(
            final CaliforniumRegistrationStore registrationStore,
            final CoapProtocolAdapter adapter,
            final Tracer tracer) {
        this.registrationStore = Objects.requireNonNull(registrationStore);
        this.adapter = Objects.requireNonNull(adapter);
        this.tracer = Objects.requireNonNull(tracer);
        // observe Device by default
        resourcesToObserve.put("/3/0", EndpointType.TELEMETRY);
    }

    /**
     * Sets the component to use for mapping upstream/downstream messages.
     * <p>
     * If not set, no mapping will be applied.
     *
     * @param mapping The mapping component.
     * @throws NullPointerException if mapping is {@code null}.
     */
    public void setMessageMapping(final LwM2MMessageMapping mapping) {
        this.messageMapping = Objects.requireNonNull(mapping);
    }

    @Override
    public Future<Void> start() {
        registrationStore.setExpirationListener(this);

        final var observationServiceImpl = new ObservationServiceImpl(
                registrationStore,
                modelProvider,
                nodeDecoder);
        Optional.ofNullable(adapter.getSecureEndpoint()).ifPresent(ep -> {
            ep.addNotificationListener(observationServiceImpl);
            observationServiceImpl.setNonSecureEndpoint(ep);
        });
        Optional.ofNullable(adapter.getInsecureEndpoint()).ifPresent(ep -> {
            ep.addNotificationListener(observationServiceImpl);
            observationServiceImpl.setNonSecureEndpoint(ep);
        });

        observationServiceImpl.addListener(new ObservationListener() {

            @Override
            public void onResponse(
                    final Observation observation,
                    final Registration registration,
                    final ObserveResponse response) {

                adapter.runOnContext(go -> {
                    onNotification(registration, response);
                });
            }

            @Override
            public void onError(
                    final Observation observation,
                    final Registration registration,
                    final Exception error) {
            }

            @Override
            public void newObservation(final Observation observation, final Registration registration) {
                LOG.debug("established new {} for {}", observation, registration);
            }

            @Override
            public void cancelled(final Observation observation) {
            }
        });

        lwm2mRequestSender = new CaliforniumLwM2mRequestSender(
                adapter.getSecureEndpoint(),
                adapter.getInsecureEndpoint(),
                observationServiceImpl,
                modelProvider,
                new DefaultLwM2mNodeEncoder(),
                nodeDecoder);

        observationService = observationServiceImpl;
        registrationStore.start();

        return Future.succeededFuture();
    }

    @Override
    public Future<Void> stop() {
        if (registrationStore != null) {
            registrationStore.stop();
        }
        return Future.succeededFuture();
    }

    private Future<?> sendLifetimeAsTtdEvent(
            final Device originDevice,
            final Registration registration,
            final SpanContext tracingContext) {

        final int lifetime = registration.getBindingMode() == BindingMode.U
                ? registration.getLifeTimeInSec().intValue()
                : 20;
        return adapter.sendTtdEvent(
                originDevice.getTenantId(),
                originDevice.getDeviceId(),
                null,
                lifetime,
                tracingContext);
    }

    void setResourcesToObserve(final Map<String, EndpointType> mappings) {
        resourcesToObserve.clear();
        if (mappings != null) {
            resourcesToObserve.putAll(mappings);
        }
    }

    private Future<Map<String, EndpointType>> getResourcesToObserve(
            final String tenantId,
            final String deviceId,
            final SpanContext tracingContext) {

        // TODO determine resources to observe from device registration info
        return Future.succeededFuture(resourcesToObserve);
    }

    private void observeConfiguredResources(
            final Registration registration,
            final SpanContext tracingContext) {

        final String tenantId = LwM2MUtils.getTenantId(registration);
        final String deviceId = LwM2MUtils.getDeviceId(registration);

        final var span = tracer.buildSpan("observe configured resources")
                .addReference(References.FOLLOWS_FROM, tracingContext)
                .withTag(TracingHelper.TAG_TENANT_ID, tenantId)
                .withTag(TracingHelper.TAG_DEVICE_ID, deviceId)
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CLIENT)
                .withTag(LwM2MUtils.TAG_LWM2M_REGISTRATION_ID, registration.getId())
                .start();

        getResourcesToObserve(tenantId, deviceId, span.context())
            .onSuccess(resourcesToObserve -> {
                resourcesToObserve.entrySet()
                    .forEach(entry -> observeResource(
                            registration,
                            tenantId,
                            deviceId,
                            entry.getKey(),
                            Map.of(LwM2MUtils.KEY_NOTIFICATION_ENDPOINT, entry.getValue().getCanonicalName()),
                            tracingContext));
            })
            .onComplete(r -> span.finish());
    }

    private boolean isResourceSupported(final Registration registration, final String resource) {
        return Stream.of(registration.getObjectLinks())
                .anyMatch(link -> resource.startsWith(link.getUrl()));
    }

    private Future<Void> observeResource(
            final Registration registration,
            final String tenantId,
            final String deviceId,
            final String resource,
            final Map<String, String> additionalProperties,
            final SpanContext tracingContext) {

        final var span = tracer.buildSpan("observe resource")
                .addReference(References.CHILD_OF, tracingContext)
                .withTag(TracingHelper.TAG_TENANT_ID, tenantId)
                .withTag(TracingHelper.TAG_DEVICE_ID, deviceId)
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CLIENT)
                .withTag(LwM2MUtils.TAG_LWM2M_REGISTRATION_ID, registration.getId())
                .withTag(LwM2MUtils.TAG_LWM2M_RESOURCE, resource)
                .start();

        if (!isResourceSupported(registration, resource)) {
            TracingHelper.logError(span, "device does not support resource");
            span.finish();
            return Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_NOT_FOUND));
        }

        if (!additionalProperties.containsKey(LwM2MUtils.KEY_NOTIFICATION_ENDPOINT)) {
            additionalProperties.put(LwM2MUtils.KEY_NOTIFICATION_ENDPOINT, EndpointType.TELEMETRY.getCanonicalName());
        }

        Optional.ofNullable(additionalProperties.get("status.correlationId"))
            .ifPresent(cid -> {
                observationService.getObservations(registration)
                    .stream()
                    .filter(obs -> obs.getPath().toString().equals(resource))
                    .forEach(existingObservation -> {
                        // check if the existing observation uses a different correlation ID
                        Optional.ofNullable(existingObservation.getContext().get("status.correlationId"))
                            .filter(id -> !id.equals(cid))
                            .ifPresent(existingCorrelationId -> {
                                // we need to replace the existing observation with a new one for the new correlation id
                                LOG.debug("canceling existing {}", existingObservation);
                                observationService.cancelObservation(existingObservation);
                            });
                    });
            });

        final Promise<Void> result = Promise.promise();

        lwm2mRequestSender.send(
                registration,
                new ObserveRequest(null, resource, additionalProperties),
                req -> {
                    ((Request) req).setConfirmable(true);
                },
                30_000L, // we require the device to answer quickly
                response -> {
                    if (response.isFailure()) {
                        TracingHelper.logError(span, "failed to establish observation");
                        result.fail(StatusCodeMapper.from(response.getCode().getCode(), response.getErrorMessage()));
                    } else {
                        result.complete();
                        adapter.runOnContext(go -> {
                            onNotification(registration, response);
                        });
                    }
                    final String responseCode = ((Response) response.getCoapResponse()).getCode().text;
                    CoapConstants.TAG_COAP_RESPONSE_CODE.set(span, responseCode);
                    span.finish();
                },
                t -> {
                    TracingHelper.logError(span, Map.of(
                            Fields.MESSAGE, "failed to send observe request",
                            Fields.ERROR_KIND, "Exception",
                            Fields.ERROR_OBJECT, t));
                    result.fail(StatusCodeMapper.toServerError(t));
                    span.finish();
                });
        return result.future();
    }

    /**
     * Forwards a notification received from a device to downstream consumers.
     *
     * @param registration The device's LwM2M registration information.
     * @param response The response message that contains the notification.
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    void onNotification(
            final Registration registration,
            final ObserveResponse response) {

        Objects.requireNonNull(registration);
        Objects.requireNonNull(response);

        final var observation = response.getObservation();
        LOG.debug("handling notification for {}", observation);
        final Device authenticatedDevice = LwM2MUtils.getDevice(registration);
        final EndpointType endpoint = Optional.ofNullable(observation.getContext().get(LwM2MUtils.KEY_NOTIFICATION_ENDPOINT))
                .map(EndpointType::fromString)
                .orElse(EndpointType.TELEMETRY);

        final Span currentSpan = tracer.buildSpan("process notification")
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER)
                .withTag(Tags.COMPONENT, adapter.getTypeName())
                .withTag(TracingHelper.TAG_TENANT_ID, authenticatedDevice.getTenantId())
                .withTag(TracingHelper.TAG_DEVICE_ID, authenticatedDevice.getDeviceId())
                .withTag(LwM2MUtils.TAG_LWM2M_REGISTRATION_ID, registration.getId())
                .withTag(LwM2MUtils.TAG_LWM2M_RESOURCE, observation.getPath().toString())
                .start();

        final var ctx = NotificationContext.fromResponse(
                registration,
                response,
                authenticatedDevice,
                adapter.getMetrics().startTimer(),
                currentSpan);

        final Future<TenantObject> tenantTracker = getTenantConfiguration(ctx.getTenantId(), ctx.getPayloadSize(), currentSpan.context());
        final Future<RegistrationAssertion> tokenTracker = adapter.getRegistrationAssertion(
                ctx.getTenantId(),
                ctx.getAuthenticatedDevice().getDeviceId(),
                null,
                currentSpan.context());
        final Future<MappedDownstreamMessage> mappedMessage = tokenTracker
                .compose(registrationInfo -> messageMapping.mapDownstreamMessage(
                        ctx,
                        ResourceIdentifier.from(
                                TelemetryConstants.TELEMETRY_ENDPOINT,
                                ctx.getTenantId(),
                                ctx.getAuthenticatedDevice().getDeviceId()),
                        registrationInfo));

        CompositeFuture.all(tenantTracker, mappedMessage)
            .compose(ok -> {
                final Map<String, Object> props = mappedMessage.result().getAdditionalProperties();
                props.put(MessageHelper.APP_PROPERTY_ORIG_ADAPTER, adapter.getTypeName());

                if (EndpointType.EVENT == endpoint) {
                    LOG.debug("forwarding observe response as event");
                    return adapter.getEventSender(tenantTracker.result()).sendEvent(
                            tenantTracker.result(),
                            tokenTracker.result(),
                            mappedMessage.result().getContentType(),
                            mappedMessage.result().getPayload(),
                            props,
                            currentSpan.context());
                } else {
                    LOG.debug("forwarding observe response as telemetry message");
                    return adapter.getTelemetrySender(tenantTracker.result()).sendTelemetry(
                            tenantTracker.result(),
                            tokenTracker.result(),
                            ctx.getRequestedQos(),
                            mappedMessage.result().getContentType(),
                            mappedMessage.result().getPayload(),
                            props,
                            currentSpan.context());
                }
            })
            .onComplete(r -> {
                final ProcessingOutcome outcome;
                if (r.succeeded()) {
                    outcome = ProcessingOutcome.FORWARDED;
                } else {
                    outcome = ProcessingOutcome.from(r.cause());
                    TracingHelper.logError(currentSpan, r.cause());
                }
                adapter.getMetrics().reportTelemetry(
                        endpoint,
                        authenticatedDevice.getTenantId(),
                        tenantTracker.result(),
                        outcome,
                        EndpointType.EVENT == endpoint ? QoS.AT_LEAST_ONCE : QoS.AT_MOST_ONCE,
                        ctx.getPayloadSize(),
                        TtdStatus.NONE,
                        ctx.getTimer());
                currentSpan.finish();
            });
    }

    void onCommand(
            final Registration registration,
            final CommandContext commandContext) {

        final Sample timer = adapter.getMetrics().startTimer();
        final Span requestSpan = TracingHelper.buildChildSpan(
                tracer,
                commandContext.getTracingContext(),
                "send command request",
                adapter.getTypeName())
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CLIENT)
                .start();

        final Command command = commandContext.getCommand();

        final Future<TenantObject> tenantTracker = getTenantConfiguration(
                command.getTenant(),
                command.getPayloadSize(),
                requestSpan.context());

        final Future<MappedUpstreamMessage> mappedMessageTracker = adapter.getRegistrationAssertion(
                command.getTenant(),
                command.getDeviceId(),
                null,
                requestSpan.context())
            .compose(registrationInfo -> messageMapping.mapUpstreamMessage(registrationInfo, commandContext, registration));

        CompositeFuture.all(tenantTracker, mappedMessageTracker)
            .compose(ok -> mappedMessageTracker)
            .compose(mappedMessage -> {

                @SuppressWarnings("rawtypes")
                final List<Future> requiredObservationsEstablished = new LinkedList<>();
                mappedMessage.getRequiredObservations().stream()
                    .forEach(requiredObservation -> {
                            LOG.debug("command requires observation of {}", requiredObservation.getPath());
                            requiredObservationsEstablished.add(observeResource(
                                    registration,
                                    command.getTenant(),
                                    command.getDeviceId(),
                                    requiredObservation.getPath().toString(),
                                    requiredObservation.getObservationProperties(),
                                    requestSpan.context()));
                    });
                return CompositeFuture.all(requiredObservationsEstablished).map(mappedMessage.getRequest());
            })
            .onSuccess(request -> {
                LOG.debug("forwarding command to device in new Leshan request [type: {}, resource path: {}]",
                        request.getClass().getSimpleName(), request.getPath().toString());
                requestSpan.log(Map.of(
                        Fields.MESSAGE, "forwarding command to device in new Leshan request",
                        "type", request.getClass().getSimpleName()));
                LwM2MUtils.TAG_LWM2M_RESOURCE.set(requestSpan, request.getPath().toString());

                lwm2mRequestSender.send(
                        registration,
                        request,
                        req -> {
                            ((Request) req).setConfirmable(true);
                            ((Request) req).addMessageObserver(new MessageObserverAdapter() {
                                /**
                                 * {@inheritDoc}
                                 * <p>
                                 * Marks the command as having been successfully sent to the device.
                                 */
                                @Override
                                public void onAcknowledgement() {
                                    adapter.runOnContext(go -> {
                                        commandContext.accept();
                                        adapter.getMetrics().reportCommand(
                                                command.isOneWay() ? Direction.ONE_WAY : Direction.REQUEST,
                                                command.getTenant(),
                                                tenantTracker.result(),
                                                ProcessingOutcome.FORWARDED,
                                                command.getPayloadSize(),
                                                timer);
                                    });
                                }
                            });
                        },
                        30_000L, // we require the device to answer quickly
                        response -> {
                            adapter.runOnContext(go -> {
                                final var coapResponse = (Response) response.getCoapResponse();
                                CoapConstants.TAG_COAP_RESPONSE_CODE.set(requestSpan, coapResponse.getCode().toString());
                                requestSpan.finish();
                                if (command.isOneWay()) {
                                    // nothing more to do
                                } else {
                                    onCommandResponse(registration, command, coapResponse, requestSpan.context());
                                }
                            });
                        },
                        t -> {
                            handleRequestSendingFailure(t, requestSpan, commandContext, command, tenantTracker.result(), timer);
                        });
            })
            .onFailure(t -> {
                handleRequestSendingFailure(t, requestSpan, commandContext, command, tenantTracker.result(), timer);
            });

    }

    private void handleRequestSendingFailure(
            final Throwable error,
            final Span requestSpan,
            final CommandContext commandContext,
            final Command command,
            final TenantObject tenant,
            final Sample timer) {

        LOG.debug("failed to send command request", error);
        TracingHelper.logError(requestSpan, Map.of(
                Fields.MESSAGE, "failed to send command request",
                Fields.ERROR_KIND, "Exception",
                Fields.ERROR_OBJECT, error));
        requestSpan.finish();
        commandContext.release();
        adapter.getMetrics().reportCommand(
                command.isOneWay() ? Direction.ONE_WAY : Direction.REQUEST,
                command.getTenant(),
                tenant,
                ProcessingOutcome.from(error),
                command.getPayloadSize(),
                timer);
    }

    void onCommandResponse(
            final Registration registration,
            final Command command,
            final Response response,
            final SpanContext requestContext) {

        final Sample timer = adapter.getMetrics().startTimer();
        final Device device = LwM2MUtils.getDevice(registration);
        final String correlationId = command.getCorrelationId();
        final Integer status = response.getCode().codeClass * 100 + response.getCode().codeDetail;

        final Span currentSpan = tracer.buildSpan("upload Command response")
                .addReference(References.FOLLOWS_FROM, requestContext)
                .withTag(Tags.COMPONENT, adapter.getTypeName())
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                .withTag(TracingHelper.TAG_TENANT_ID, device.getTenantId())
                .withTag(TracingHelper.TAG_DEVICE_ID, device.getDeviceId())
                .withTag(Constants.HEADER_COMMAND_RESPONSE_STATUS, status)
                .withTag(TracingHelper.TAG_CORRELATION_ID, correlationId)
                .start();

        LOG.debug("processing response to command [tenant-id: {}, device-id: {}, correlation-id: {}, status code: {}]",
                device.getTenantId(), device.getDeviceId(), correlationId, status);

        final String replyToAddress = String.format("%s/%s/%s",
                CommandConstants.COMMAND_RESPONSE_ENDPOINT,
                device.getTenantId(),
                Commands.getDeviceFacingReplyToId(command.getReplyToId(), device.getDeviceId()));

        // TODO invoke downstream message mapper for command response

        final Buffer payload = Optional.ofNullable(response.getPayload())
                .map(Buffer::buffer)
                .orElse(Buffer.buffer());
        final CommandResponse commandResponse = CommandResponse.fromCorrelationId(
                correlationId,
                replyToAddress,
                payload,
                MediaTypeRegistry.toString(response.getOptions().getContentFormat()),
                status);

        final Future<TenantObject> tenantTracker = getTenantConfiguration(
                device.getTenantId(),
                response.getPayloadSize(),
                currentSpan.context());

        tenantTracker
            .compose(tenant -> adapter.getCommandResponseSender(tenant)
                    .sendCommandResponse(commandResponse, currentSpan.context()))
            .onSuccess(ok -> {
                LOG.trace("forwarded command response [correlation-id: {}] to downstream application",
                        correlationId);
                currentSpan.log("forwarded command response to application");
                adapter.getMetrics().reportCommand(
                        Direction.RESPONSE,
                        device.getTenantId(),
                        tenantTracker.result(),
                        ProcessingOutcome.FORWARDED,
                        payload.length(),
                        timer);
            })
            .onFailure(t -> {
                LOG.debug("could not send command response [correlation-id: {}] to application",
                        correlationId, t);
                TracingHelper.logError(currentSpan, t);
                adapter.getMetrics().reportCommand(
                        Direction.RESPONSE,
                        device.getTenantId(),
                        tenantTracker.result(),
                        ProcessingOutcome.from(t),
                        payload.length(),
                        timer);
            })
            .onComplete(r -> currentSpan.finish());
    }

    private Future<TenantObject> getTenantConfiguration(
            final String tenantId,
            final SpanContext tracingContext) {

        return adapter.getTenantClient().get(tenantId, tracingContext)
                .compose(adapter::isAdapterEnabled);
    }

    private Future<TenantObject> getTenantConfiguration(
            final String tenantId,
            final long payloadSize,
            final SpanContext tracingContext) {

        return getTenantConfiguration(tenantId, tracingContext)
                .compose(tenant -> {
                    // we only need to check message limit here because
                    // 1. the connection to the client is already established and
                    // 2. even if the tenant has been disabled between sending out the request and
                    // receiving the response, we should still be able to safely process the (pending) response.
                    return adapter.checkMessageLimit(tenant, payloadSize, tracingContext).map(tenant);
                });
    }

    @Override
    public void registrationExpired(final Registration registration, final Collection<Observation> observations) {
        Optional.ofNullable(commandConsumers.remove(registration.getId()))
            .ifPresent(consumer -> {
                adapter.runOnContext(go -> {
                    final Device device = LwM2MUtils.getDevice(registration);
                    consumer.close(null)
                        .onComplete(r -> observations.forEach(obs -> {
                            LOG.debug("canceling {} of {}", obs, device);
                            observationService.cancelObservation(obs);
                        }));
                });
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> addRegistration(final Registration registration, final SpanContext tracingContext) {

        final var device = LwM2MUtils.getDevice(registration);
        final var existingRegistration = registrationStore.addRegistration(registration);

        if (existingRegistration == null) {
            LOG.debug("added {} for {}", registration, device);
        } else {
            LOG.debug("replaced existing {} for {} with new {}", existingRegistration, device, registration);
        }

        return adapter.getCommandConsumerFactory()
                .createCommandConsumer(
                        device.getTenantId(),
                        device.getDeviceId(),
                        commandContext -> {
                            onCommand(registration, commandContext);
                        },
                        null,
                        tracingContext)
            .compose(commandConsumer -> {
                commandConsumers.put(registration.getId(), commandConsumer);
                return sendLifetimeAsTtdEvent(device, registration, tracingContext);
            })
            .onSuccess(ok -> observeConfiguredResources(registration, tracingContext))
            .mapEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> updateRegistration(final RegistrationUpdate registrationUpdate, final SpanContext tracingContext) {

        final var updatedRegistration = registrationStore.updateRegistration(registrationUpdate);
        final var device = LwM2MUtils.getDevice(updatedRegistration.getPreviousRegistration());

        return sendLifetimeAsTtdEvent(device, updatedRegistration.getUpdatedRegistration(), tracingContext)
                .mapEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> removeRegistration(final String registrationId, final SpanContext tracingContext) {

        // this also removes all observations associated with the registration
        final var deregistration = registrationStore.removeRegistration(registrationId);
        final var device = LwM2MUtils.getDevice(deregistration.getRegistration());

        registrationExpired(deregistration.getRegistration(), deregistration.getObservations());
        return adapter.sendTtdEvent(
                device.getTenantId(),
                device.getDeviceId(),
                null,
                0,
                tracingContext)
                .mapEmpty();
    }
}
