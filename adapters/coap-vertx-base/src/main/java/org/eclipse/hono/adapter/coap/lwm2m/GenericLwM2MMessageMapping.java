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
import java.util.ArrayList;
import java.util.Set;

import org.eclipse.hono.adapter.client.command.CommandContext;
import org.eclipse.hono.client.ClientErrorException;
import org.eclipse.hono.service.metric.MetricsTags.EndpointType;
import org.eclipse.hono.util.RegistrationAssertion;
import org.eclipse.hono.util.ResourceIdentifier;
import org.eclipse.leshan.core.LwM2mId;
import org.eclipse.leshan.core.node.LwM2mPath;
import org.eclipse.leshan.core.node.codec.DefaultLwM2mNodeDecoder;
import org.eclipse.leshan.core.node.codec.LwM2mNodeDecoder;
import org.eclipse.leshan.core.request.ContentFormat;
import org.eclipse.leshan.core.request.DownlinkRequest;
import org.eclipse.leshan.core.request.ExecuteRequest;
import org.eclipse.leshan.core.request.ReadRequest;
import org.eclipse.leshan.core.request.WriteRequest;
import org.eclipse.leshan.core.request.WriteRequest.Mode;
import org.eclipse.leshan.core.response.LwM2mResponse;
import org.eclipse.leshan.server.model.LwM2mModelProvider;
import org.eclipse.leshan.server.model.StandardModelProvider;
import org.eclipse.leshan.server.registration.Registration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;


/**
 * A DittoProtocolMessageMapping.
 *
 */
public class GenericLwM2MMessageMapping implements LwM2MMessageMapping {

    protected static final LwM2mPath PATH_FIRMWARE_OBJECT_INSTANCE = new LwM2mPath(LwM2mId.FIRMWARE, 0);
    protected static final LwM2mNodeDecoder nodeDecoder = new DefaultLwM2mNodeDecoder();
    protected static final LwM2mModelProvider modelProvider = new StandardModelProvider();

    private static final Logger LOG = LoggerFactory.getLogger(GenericLwM2MMessageMapping.class);
    private static final String COMMAND_EXECUTE = "execute";
    private static final String COMMAND_READ = "read";
    private static final String COMMAND_UPDATE = "update";
    private static final String COMMAND_REPLACE = "replace";
    private static final Set<String> SUPPORTED_COMMANDS = Set.of(COMMAND_READ, COMMAND_EXECUTE, COMMAND_UPDATE, COMMAND_REPLACE);

    /**
     * {@inheritDoc}
     * <p>
     * This default implementation returns the original target address, payload
     * and message properties.
     */
    @Override
    public Future<MappedDownstreamMessage> mapDownstreamMessage(
            final NotificationContext ctx,
            final ResourceIdentifier targetAddress,
            final RegistrationAssertion registrationInfo) {

        return Future.succeededFuture(new MappedDownstreamMessage(
                targetAddress,
                ctx.getContentTypeAsString(),
                ctx.getPayload(),
                ctx.getDownstreamMessageProperties()));
    }

    /**
     * Maps a command to a LwM2M request using a canonical mapping algorithm.
     * <p>
     * The canonical algorithm is as follows:
     *
     * @param registrationInfo The information included in the registration assertion for
     *                         the device to which the command needs to be sent.
     * @param commandContext The original command to be mapped.
     * @param registration The LwM2M registration information for the device
     * @return A future indicating the outcome of the operation.
     *         The future will be completed with the request to send to the device or
     *         failed with a {@link org.eclipse.hono.client.ServiceInvocationException} if the command
     *         could not be mapped.
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    @Override
    public Future<MappedUpstreamMessage> mapUpstreamMessage(
            final RegistrationAssertion registrationInfo,
            final CommandContext commandContext,
            final Registration registration) {

        // extract CoAP request type and resource path from command name
        final var command = commandContext.getCommand();
        final String compoundCommandName = command.getName();
        final String[] commandSegments = compoundCommandName.split(":", 2);
        if (commandSegments.length < 2) {
            LOG.debug("unsupported/malformed command name: {}", compoundCommandName);
            return Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_BAD_REQUEST));
        }

        final String commandName = commandSegments[0].toLowerCase();
        final String resourcePath = commandSegments[1];

        if (!SUPPORTED_COMMANDS.contains(commandName)) {
            LOG.debug("unsupported command: {}", commandName);
            return Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_BAD_REQUEST));
        }

        final Promise<MappedUpstreamMessage> result = Promise.promise();
        final LwM2mPath path = new LwM2mPath(resourcePath);
        final DownlinkRequest<? extends LwM2mResponse> request;
        final var requiredObservations = new ArrayList<ObservationSpec>();

        if (COMMAND_READ.equals(commandName)) {
            request = new ReadRequest(path.getObjectId(), path.getObjectInstanceId(), path.getResourceId());
        } else if (COMMAND_EXECUTE.equals(commandName)) {
            request = new ExecuteRequest(path.getObjectId(), path.getObjectInstanceId(), path.getResourceId());
            if (path.getObjectId() == LwM2mId.FIRMWARE) {
                requiredObservations.add(ObservationSpec.from(PATH_FIRMWARE_OBJECT_INSTANCE, EndpointType.EVENT));
            }
        } else {
            // this is a update/replace request
            final ContentFormat contentFormat = ContentFormat.fromMediaType(command.getContentType());
            final var node = nodeDecoder.decode(
                    command.getPayload().getBytes(),
                    contentFormat,
                    path,
                    modelProvider.getObjectModel(registration));
            final Mode mode = COMMAND_UPDATE.equals(commandName) ? Mode.UPDATE : Mode.REPLACE;
            request = new WriteRequest(mode, contentFormat, resourcePath , node);
            if (path.getObjectId() == LwM2mId.FIRMWARE) {
                requiredObservations.add(ObservationSpec.from(PATH_FIRMWARE_OBJECT_INSTANCE, EndpointType.EVENT));
            }
        }
        result.complete(new MappedUpstreamMessage(request, requiredObservations.toArray(new ObservationSpec[] {})));
        return result.future();
    }
}
