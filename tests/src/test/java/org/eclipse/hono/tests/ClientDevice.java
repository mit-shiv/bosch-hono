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


package org.eclipse.hono.tests;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

/**
 * An abstraction of a device that interacts with one of Hono's
 * protocol adapters.
 * 
 * @param <S> The type of properties sent by the device in a message to the adapter.
 * @param <R> The type of properties received by the device in a message from the adapter.
 */
public interface ClientDevice<S, R> {

    /**
     * Establishes a connection to the adapter.
     * <p>
     * This operation is only supported if the adapter uses a
     * connection oriented protocol.
     * 
     * @return A future indicating the outcome of the operation.
     */
    Future<Void> connect();

    /**
     * Uploads data to the adapter.
     * <p>
     * The particular type of message, i.e. either a telemetry
     * message or an event, as well as the delivery semantics being used
     * depend on the device's configuration.
     * 
     * @param originDeviceId The identifier of the device that the data originates from.
     * @param properties The properties to include in the message.
     * @param payload The data to upload.
     * @return A future indicating the outcome of uploading the data.
     */
    Future<R> uploadData(String originDeviceId, S properties, Buffer payload);

    /**
     * Uploads telemetry data to the adapter.
     * 
     * @param qos The delivery semantics to use for uploading the data.
     * @param properties The properties to include in the message.
     * @param payload The data to upload.
     * @return A future indicating the outcome of uploading the data.
     */
    Future<R> uploadTelemetry(DeliverySemantics qos, S properties, Buffer payload);

    /**
     * Uploads telemetry data to the adapter on behalf of a device.
     * 
     * @param qos The delivery semantics to use for uploading the data.
     * @param originDeviceId The identifier of the device that the data originates from.
     * @param properties The properties to include in the message.
     * @param payload The data to upload.
     * @return A future indicating the outcome of uploading the data.
     */
    Future<R> uploadTelemetry(DeliverySemantics qos, String originDeviceId, S properties, Buffer payload);

    /**
     * Uploads an event to the adapter.
     * 
     * @param properties The properties to include in the message.
     * @param payload The data to upload.
     * @return A future indicating the outcome of uploading the event.
     */
    Future<R> uploadEvent(S properties, Buffer payload);

    /**
     * Uploads an event to the adapter on behalf of a device.
     * 
     * @param originDeviceId The identifier of the device that the event originates from.
     * @param properties The properties to include in the message.
     * @param payload The data to upload.
     * @return A future indicating the outcome of uploading the event.
     */
    Future<R> uploadEvent(String originDeviceId, S properties, Buffer payload);

    /**
     * Sends an empty notification event.
     * 
     * @param originDeviceId The identifier of the device that the event originates from.
     * @return A future indicating the outcome of uploading the event.
     */
    Future<R> sendEmptyNotification(String originDeviceId);

    /**
     * Sends an empty notification event.
     * 
     * @param originDeviceId The identifier of the device that the event originates from.
     * @param ttd The number of seconds that the device will stay connected.
     * @return A future indicating the outcome of uploading the event.
     */
    Future<R> sendEmptyNotification(String originDeviceId, int ttd);

    /**
     * Sends a response to a command.
     * 
     * @param uriPath The URI path of the resource to send the command response to.
     * @param status The status code indicating the outcome of processing the command.
     * @param contentType The type of content contained in the response body or {@code null}
     *                    if the response contains no payload.
     * @param payload The payload or {@code null}.
     * @return A future indicating the outcome of sending the command response.
     */
    Future<R> sendCommandResponse(
            String uriPath,
            int status,
            String contentType,
            Buffer payload);
}
