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

import org.eclipse.hono.adapter.client.command.CommandContext;
import org.eclipse.hono.util.RegistrationAssertion;
import org.eclipse.hono.util.ResourceIdentifier;
import org.eclipse.leshan.server.registration.Registration;

import io.vertx.core.Future;


/**
 * A service for either processing messages uploaded by CoAP devices before they are being
 * forwarded downstream or mapping payload for commands to be sent to CoAP devices.
 *
 */
public interface LwM2MMessageMapping {

    /**
     * Maps a notification sent by a device to a downstream message.
     *
     * @param ctx              The context in which the notification has been received.
     * @param targetAddress    The downstream address that the notification will be forwarded to.
     * @param registrationInfo The information included in the registration assertion for
     *                         the device that has sent the notification.
     * @return                 A successful future containing the mapped message.
     *                         Otherwise, the future will be failed with a {@link org.eclipse.hono.client.ServiceInvocationException}
     *                         if the message could not be mapped.
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    Future<MappedDownstreamMessage> mapDownstreamMessage(
            NotificationContext ctx,
            ResourceIdentifier targetAddress,
            RegistrationAssertion registrationInfo);

    /**
     * Maps a command to a LwM2M request.
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
    Future<MappedUpstreamMessage> mapUpstreamMessage(
            RegistrationAssertion registrationInfo,
            CommandContext commandContext,
            Registration registration);
}
