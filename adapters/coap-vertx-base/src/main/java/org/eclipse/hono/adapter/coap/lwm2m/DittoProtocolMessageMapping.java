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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.eclipse.hono.adapter.client.command.Command;
import org.eclipse.hono.adapter.client.command.CommandContext;
import org.eclipse.hono.client.ClientErrorException;
import org.eclipse.hono.service.metric.MetricsTags.EndpointType;
import org.eclipse.hono.util.RegistrationAssertion;
import org.eclipse.hono.util.ResourceIdentifier;
import org.eclipse.leshan.core.LwM2mId;
import org.eclipse.leshan.core.node.LwM2mObjectInstance;
import org.eclipse.leshan.core.node.LwM2mPath;
import org.eclipse.leshan.core.node.LwM2mSingleResource;
import org.eclipse.leshan.core.request.ContentFormat;
import org.eclipse.leshan.core.request.ExecuteRequest;
import org.eclipse.leshan.core.request.WriteRequest;
import org.eclipse.leshan.core.request.WriteRequest.Mode;
import org.eclipse.leshan.server.registration.Registration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.pointer.JsonPointer;


/**
 * A DittoProtocolMessageMapping.
 *
 */
public class DittoProtocolMessageMapping extends GenericLwM2MMessageMapping {

    static final String CONTENT_TYPE_MERGE_PATCH_JSON = "application/merge-patch+json";
    static final String CONTENT_TYPE_DITTO = "application/vnd.eclipse.ditto+json";
    static final String KEY_DITTO_NAMESPACED_THING_NAME = "ditto.namespaced.thingname";
    static final String KEY_STATUS_SOFTWARE_MODULE_VERSION = "status.softwareModule.version";
    static final String KEY_STATUS_SOFTWARE_MODULE_NAME = "status.softwareModule.name";
    static final String KEY_STATUS_CORRELATION_ID = "status.correlationId";

    private static final Logger LOG = LoggerFactory.getLogger(DittoProtocolMessageMapping.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<MappedDownstreamMessage> mapDownstreamMessage(
            final NotificationContext ctx,
            final ResourceIdentifier targetAddress,
            final RegistrationAssertion registrationInfo) {

        final var path = new LwM2mPath(ctx.getOrigAddress());

        if (path.getObjectId() == LwM2mId.FIRMWARE && path.isObjectInstance()) {
            final var props = ctx.getDownstreamMessageProperties();
            final var payload = ctx.getParsedPayload();
            final var firmware = (LwM2mObjectInstance) payload;
            final Long state = (Long) firmware.getResource(3).getValue();
            final Long updateResult = (Long) firmware.getResource(5).getValue();
            return createDittoOperationStatusCommand(props, state, updateResult)
                    .map(cmd -> new MappedDownstreamMessage(
                            targetAddress,
                            CONTENT_TYPE_DITTO,
                            cmd.toBuffer(),
                            props))
                    .recover(t -> {
                        LOG.debug("failed to map downstream message", t);
                        return super.mapDownstreamMessage(ctx, targetAddress, registrationInfo);
                    });
        } else {
            return super.mapDownstreamMessage(ctx, targetAddress, registrationInfo);
        }
    }

    private Future<JsonObject> createDittoOperationStatusCommand(
            final Map<String, Object> props,
            final Long firmwareState,
            final Long firmwareUpdateResult) {

        final int state = firmwareState.intValue();
        final int updateResult = firmwareUpdateResult.intValue();

        // the following statements are based on the Firmware Update State Machine as defined
        // in Section E.6.1 of the LwM2M 1.0.2 Technical Specification
        switch (state) {
        case 0:
            switch (updateResult) {
            case 0:
                return createStatusUpdateMessage(props, "STARTED", null);
            case 1:
                return createStatusUpdateMessage(props, "FINISHED_SUCCESS", "Firmware updated successfully");
            case 2:
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Not enough flash memory for the new firmware package");
            case 3:
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Out of RAM during downloading process");
            case 4:
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Connection lost during downloading process");
            case 5:
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Integrity check failure for new downloaded package");
            case 6:
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Unsupported package type");
            case 7:
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Invalid URI");
            case 8:
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Firmware update failed");
            case 9:
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Unsupported protocol");
            default:
                LOG.debug("device reported illegal firmware update status [State: {}, Update Result: {}]",
                        state, updateResult);
                return Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_BAD_REQUEST));
            }
        case 1:
            if (updateResult == 0) {
                return createStatusUpdateMessage(props, "DOWNLOADING", null);
            }
            LOG.debug("device reported illegal firmware update status [State: {}, Update Result: {}]",
                    state, updateResult);
            return Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_BAD_REQUEST));
        case 2:
            if (updateResult == 0) {
                return createStatusUpdateMessage(props, "DOWNLOADED", null);
            } else if (updateResult == 8) {
                return createStatusUpdateMessage(props, "FINISHED_ERROR", "Firmware update failed");
            }
            LOG.debug("device reported illegal firmware update status [State: {}, Update Result: {}]",
                    state, updateResult);
            return Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_BAD_REQUEST));
        case 3:
            if (updateResult == 0) {
                return createStatusUpdateMessage(props, "INSTALLING", null);
            }
            LOG.debug("device reported illegal firmware update status [State: {}, Update Result: {}]",
                    state, updateResult);
            return Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_BAD_REQUEST));
        default:
            LOG.debug("device reported illegal firmware update status [State: {}, Update Result: {}]",
                    state, updateResult);
            return Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_BAD_REQUEST));
        }
    }

    private Future<JsonObject> createStatusUpdateMessage(
            final Map<String, Object> props,
            final String status,
            final String message) {

        final String namespacedThing = LwM2MUtils.getValue(props, KEY_DITTO_NAMESPACED_THING_NAME, String.class);
        if (namespacedThing == null) {
            return Future.failedFuture(new IllegalArgumentException("notification context does not contain namespaced Thing name"));
        }

        final String correlationId = LwM2MUtils.getValue(props, KEY_STATUS_CORRELATION_ID, String.class);
        if (correlationId == null) {
            return Future.failedFuture(new IllegalArgumentException("notification context does not contain status correlation ID"));
        }

        final String softwareModuleName = LwM2MUtils.getValue(props, KEY_STATUS_SOFTWARE_MODULE_NAME, String.class);
        if (softwareModuleName == null) {
            return Future.failedFuture(new IllegalArgumentException("notification context does not contain software module name"));
        }
        final String softwareModuleVersion = LwM2MUtils.getValue(props, KEY_STATUS_SOFTWARE_MODULE_VERSION, String.class);
        if (softwareModuleVersion == null) {
            return Future.failedFuture(new IllegalArgumentException("notification context does not contain software module version"));
        }

        final JsonObject cmd = new JsonObject();

        final var softwareModule = new JsonObject()
                .put("name", softwareModuleName)
                .put("version", softwareModuleVersion);

        final var value = new JsonObject();

        final var lastOperationStatus = new JsonObject();
        lastOperationStatus.put("correlationId", correlationId);
        lastOperationStatus.put("status", status);
        Optional.ofNullable(message).ifPresent(msg -> lastOperationStatus.put("message", msg));
        lastOperationStatus.put("softwareModule", softwareModule);
        value.put("lastOperation", lastOperationStatus);

        if ("FINISHED_ERROR".equals(status)) {
            value.put("lastFailedOperation", lastOperationStatus);
        }

        cmd.put("topic", String.format("%s/things/twin/commands/merge", namespacedThing));
        cmd.put("headers", new JsonObject().put("content-type", CONTENT_TYPE_MERGE_PATCH_JSON));
        cmd.put("path", "features/softwareUpdateable/properties");
        cmd.put("value", value);
        return Future.succeededFuture(cmd);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<MappedUpstreamMessage> mapUpstreamMessage(
            final RegistrationAssertion registrationInfo,
            final CommandContext commandContext,
            final Registration registration) {

        final var command = commandContext.getCommand();
        if (isDittoMessage(command)) {
            return mapUpstreamDittoMessage(registrationInfo, commandContext);
        } else {
            return super.mapUpstreamMessage(registrationInfo, commandContext, registration);
        }
    }

    private Future<MappedUpstreamMessage> mapUpstreamDittoMessage(
            final RegistrationAssertion registrationInfo,
            final CommandContext commandContext) {

        final Promise<MappedUpstreamMessage> result = Promise.promise();
        final var command = commandContext.getCommand();
        final var payload = command.getPayload().toJsonObject();

        if (isSoftwareUpdatableDownloadInvocation(payload)) {

            final String url = (String) JsonPointer.from("/value/softwareModules/0/artifacts/0/download/HTTPS/url")
                    .queryJson(payload);
            final ContentFormat contentFormat = ContentFormat.TEXT;
            final var node = LwM2mSingleResource.newStringResource(1, url);
            final Map<String, String> props = new HashMap<>();
            addNamespacedThingname(payload, props);
            addPropertiesForFirmwareObservation(payload, props);
            final MappedUpstreamMessage mappedMessage = new MappedUpstreamMessage(
                    new WriteRequest(Mode.REPLACE, contentFormat, "/5/0/1", node),
                    ObservationSpec.from(PATH_FIRMWARE_OBJECT_INSTANCE, EndpointType.EVENT, props));
            result.complete(mappedMessage);

        } else if (isSoftwareUpdatableInstallInvocation(payload)) {

            final Map<String, String> props = new HashMap<>();
            addNamespacedThingname(payload, props);
            addPropertiesForFirmwareObservation(payload, props);
            final MappedUpstreamMessage mappedMessage = new MappedUpstreamMessage(
                    new ExecuteRequest("/5/0/2"),
                    ObservationSpec.from(PATH_FIRMWARE_OBJECT_INSTANCE, EndpointType.EVENT, props));
            result.complete(mappedMessage);

        } else {
            result.fail(new ClientErrorException(HttpURLConnection.HTTP_BAD_REQUEST, "unsupported Ditto envelope topic/path"));
        }
        return result.future();
    }

    private void addPropertiesForFirmwareObservation(
            final JsonObject modifiedEventValue,
            final Map<String, String> props) {

        Optional.ofNullable(modifiedEventValue.getJsonObject("value"))
            .flatMap(value -> {
                Optional.ofNullable(value.getString("correlationId"))
                    .ifPresent(id -> props.put(KEY_STATUS_CORRELATION_ID, id));
                return Optional.ofNullable(value.getJsonArray("softwareModules"));
            })
            .flatMap(array -> {
                if (array.isEmpty()) {
                    return Optional.empty();
                } else {
                    return Optional.of(array.getJsonObject(0));
                }
            })
            .flatMap(module -> Optional.ofNullable(module.getJsonObject("softwareModule")))
            .ifPresent(module -> {
                props.put(KEY_STATUS_SOFTWARE_MODULE_NAME, module.getString("name"));
                props.put(KEY_STATUS_SOFTWARE_MODULE_VERSION, module.getString("version"));
            });;
    }

    private boolean isDittoMessage(final Command command) {
        return CONTENT_TYPE_DITTO.equals(command.getContentType());
    }

    private boolean isModifiedEvent(final JsonObject dittoMessage) {
        return Optional.ofNullable(dittoMessage.getString("topic"))
                .map(s -> s.endsWith("/modified"))
                .orElseThrow(() -> new IllegalArgumentException("Ditto message does not contain topic"));
    }

    private boolean isSoftwareUpdatableDownloadInvocation(final JsonObject dittoMessage) {
        return isModifiedEvent(dittoMessage) && dittoMessage.getString("path")
                .equals("features/softwareUpdateable/properties/download");
    }

    private boolean isSoftwareUpdatableInstallInvocation(final JsonObject dittoMessage) {
        return isModifiedEvent(dittoMessage) && dittoMessage.getString("path")
                .equals("features/softwareUpdateable/properties/install");
    }

    private String addNamespacedThingname(
            final JsonObject dittoMessage,
            final Map<String, String> props) {

        return Optional.ofNullable(dittoMessage.getString("topic"))
                .map(ResourceIdentifier::fromString)
                .map(ResourceIdentifier::getBasePath)
                .map(s -> {
                    props.put(DittoProtocolMessageMapping.KEY_DITTO_NAMESPACED_THING_NAME, s);
                    return s;
                })
                .orElseThrow(() -> new IllegalArgumentException("Ditto message does not contain topic"));
    }
}
