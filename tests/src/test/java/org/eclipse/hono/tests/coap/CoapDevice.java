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


package org.eclipse.hono.tests.coap;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapHandler;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.Utils;
import org.eclipse.californium.core.coap.CoAP.Code;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.coap.CoAP.Type;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.core.coap.OptionSet;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.core.network.Endpoint;
import org.eclipse.californium.core.network.config.NetworkConfig;
import org.eclipse.californium.scandium.DTLSConnector;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;
import org.eclipse.californium.scandium.dtls.pskstore.PskStore;
import org.eclipse.californium.scandium.dtls.pskstore.StaticPskStore;
import org.eclipse.hono.client.ServiceInvocationException;
import org.eclipse.hono.tests.ClientDevice;
import org.eclipse.hono.tests.CommandEndpointConfiguration.ClientType;
import org.eclipse.hono.tests.CommandEndpointConfiguration.TelemetryEndpoint;
import org.eclipse.hono.tests.DeliverySemantics;
import org.eclipse.hono.tests.IntegrationTestSupport;
import org.eclipse.hono.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;


/**
 * A device using the Constrained Application Protocol (CoAP) to connect to Hono.
 *
 */
public class CoapDevice implements ClientDevice<OptionSet, OptionSet> {

    private static final Logger LOG = LoggerFactory.getLogger(CoapDevice.class);

    private final String tenant;
    private final String id;
    private final CoapClient coapClient;

    private CoapCommandEndpointConfiguration endpointConfig;

    /**
     * Creates a new directly connected device for a tenant
     * that authenticates to the adapter.
     * 
     * @param tenant The tenant that the device belongs to.
     * @param identifier The device identifier.
     * @param secret The pre-shared secret to use for authentication.
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    public CoapDevice(final String tenant, final String identifier, final String secret) {
        this(tenant, identifier, secret, new CoapCommandEndpointConfiguration(ClientType.DEVICE, TelemetryEndpoint.TELEMETRY_AT_LEAST_ONCE, true));
    }

    /**
     * Creates a new device for a tenant.
     * 
     * @param tenant The tenant that the device belongs to.
     * @param identifier The device identifier.
     * @param secret The pre-shared secret to use for authentication.
     * @param endpointConfig The endpoint configuration to use for
     *                       interacting with the protocol adapter.
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    public CoapDevice(
            final String tenant,
            final String identifier,
            final String secret,
            final CoapCommandEndpointConfiguration endpointConfig) {
        this.tenant = Objects.requireNonNull(tenant);
        this.id = Objects.requireNonNull(identifier);
        this.endpointConfig = Objects.requireNonNull(endpointConfig);
        this.coapClient = createCoapClient(secret);
    }

    private static int toHttpStatusCode(final ResponseCode responseCode) {
        int result = 0;
        result += responseCode.codeClass * 100;
        result += responseCode.codeDetail;
        return result;
    }

    private static CoapHandler getHandler(final Handler<AsyncResult<OptionSet>> responseHandler) {
        return getHandler(responseHandler, ResponseCode.CHANGED);
    }

    private static CoapHandler getHandler(
            final Handler<AsyncResult<OptionSet>> responseHandler,
            final ResponseCode expectedStatusCode) {

        return new CoapHandler() {

            @Override
            public void onLoad(final CoapResponse response) {
                if (response.getCode() == expectedStatusCode) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("=> received {}", Utils.prettyPrint(response));
                    }
                    responseHandler.handle(Future.succeededFuture(response.getOptions()));
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("expected {} => received {}", expectedStatusCode, Utils.prettyPrint(response));
                    }
                    responseHandler.handle(Future.failedFuture(
                            new ServiceInvocationException(toHttpStatusCode(response.getCode()), response.getResponseText())));
                }
            }

            @Override
            public void onError() {
                responseHandler.handle(Future.failedFuture(new ServiceInvocationException(HttpURLConnection.HTTP_UNAVAILABLE)));
            }
        };
    }

    private static Type getCoapType(final DeliverySemantics qos) {
        switch (qos) {
        case AT_MOST_ONCE:
            return Type.NON;
        case AT_LEAST_ONCE:
        default:
            return Type.CON;
        }
    }

    private static URI getRequestUri(final String scheme, final String resource, final String query) {

        final int port;
        switch (scheme) {
        case "coap": 
            port = IntegrationTestSupport.COAP_PORT;
            break;
        case "coaps": 
            port = IntegrationTestSupport.COAPS_PORT;
            break;
        default:
            throw new IllegalArgumentException();
        }
        try {
            return new URI(scheme, null, IntegrationTestSupport.COAP_HOST, port, resource, query, null);
        } catch (final URISyntaxException e) {
            // cannot happen
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> connect() {
        return Future.succeededFuture();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<OptionSet> uploadData(final String originDeviceId, final OptionSet options, final Buffer payload) {

        switch (endpointConfig.getTelemetryEndpoint()) {
        case TELEMETRY_AT_LEAST_ONCE:
        case TELEMETRY_AT_MOST_ONCE:
            final DeliverySemantics qos = endpointConfig.getTelemetryEndpoint().getDeliverySemantics();
            return uploadTelemetry(qos, originDeviceId, options, payload);
        case EVENT:
        default:
            return uploadEvent(originDeviceId, options, payload);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<OptionSet> uploadTelemetry(final DeliverySemantics qos, final OptionSet options, final Buffer payload) {
        return uploadTelemetry(qos, id, options, payload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<OptionSet> uploadTelemetry(final DeliverySemantics qos, final String deviceId, final OptionSet options, final Buffer payload) {
        final Promise<OptionSet> result = Promise.promise();
        final Request request = createRequest(
                qos,
                endpointConfig.getTelemetryUri(tenant, deviceId),
                options,
                payload);
        coapClient.advanced(getHandler(result), request);
        return result.future();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<OptionSet> uploadEvent(final OptionSet options, final Buffer payload) {
        return uploadEvent(id, options, payload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<OptionSet> uploadEvent(final String originDeviceId, final OptionSet options, final Buffer payload) {
        final Promise<OptionSet> result = Promise.promise();
        final Request request = createRequest(
                DeliverySemantics.AT_LEAST_ONCE,
                endpointConfig.getEventUri(tenant, originDeviceId),
                options,
                payload);
        coapClient.advanced(getHandler(result), request);
        return result.future();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<OptionSet> sendEmptyNotification(final String originDeviceId) {
        return uploadData(
                originDeviceId,
                new OptionSet().addUriQuery("empty"),
                null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<OptionSet> sendEmptyNotification(final String originDeviceId, final int ttd) {
        return uploadData(
                originDeviceId,
                new OptionSet().addUriQuery("empty").addUriQuery(String.format("%s=%d", Constants.HEADER_TIME_TILL_DISCONNECT, ttd)),
                null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<OptionSet> sendCommandResponse(
            final String resource,
            final int status,
            final String contentType,
            final Buffer payload) {

        final Promise<OptionSet> result = Promise.promise();
        final OptionSet options = new OptionSet();
        options.addUriQuery(String.format("%s=%d", Constants.HEADER_COMMAND_RESPONSE_STATUS, status));
        Optional.ofNullable(contentType).ifPresent(ct -> options.setContentFormat(MediaTypeRegistry.parse(contentType)));
        final Code code = endpointConfig.isGatewayClient() ? Code.PUT : Code.POST;
        final Request request = createRequest(
                code,
                Type.CON,
                resource.startsWith("/") ? resource : "/" + resource,
                options,
                payload);
        coapClient.advanced(getHandler(result), request);
        return result.future();
    }

    private CoapClient createCoapClient(final String sharedSecret) {
        final CoapClient client = new CoapClient();
        if (endpointConfig.isAuthenticated()) {
            client.setEndpoint(getSecureEndpoint(new StaticPskStore(
                    IntegrationTestSupport.getUsername(id, tenant),
                    sharedSecret.getBytes(StandardCharsets.UTF_8))));
        }
        return client;
    }

    private Endpoint getSecureEndpoint(final PskStore pskStoreToUse) {

        final DtlsConnectorConfig.Builder dtlsConfig = new DtlsConnectorConfig.Builder();
        dtlsConfig.setAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        dtlsConfig.setPskStore(pskStoreToUse);
        dtlsConfig.setMaxRetransmissions(1);
        final CoapEndpoint.Builder builder = new CoapEndpoint.Builder();
        builder.setNetworkConfig(NetworkConfig.createStandardWithoutFile());
        builder.setConnector(new DTLSConnector(dtlsConfig.build()));
        return builder.build();
    }

    /**
     * Creates a CoAP request.
     * 
     * @param qos The delivery semantics to use for uploading the data.
     * @param resource the resource path.
     * @param options The options to include in the request.
     * @param payload The payload to send in the request body.
     * @return The request to send.
     */
    private Request createRequest(
            final DeliverySemantics qos,
            final String resource,
            final OptionSet options,
            final Buffer payload) {

        if (endpointConfig.isGatewayClientForSingleDevice() || !endpointConfig.isAuthenticated()) {
            return createRequest(Code.PUT, getCoapType(qos), resource, options, payload);
        }
        return createRequest(Code.POST, getCoapType(qos), resource, options, payload);
    }

    /**
     * Creates a CoAP request.
     * 
     * @param code The CoAP request code.
     * @param type The message type.
     * @param resource the resource path.
     * @param options The options to include in the request.
     * @param payload The payload to send in the request body.
     * @return The request to send.
     */
    private Request createRequest(
            final Code code,
            final Type type,
            final String resource,
            final OptionSet options,
            final Buffer payload) {

        final Request request = new Request(code, type);
        Optional.ofNullable(options).ifPresent(ct -> request.setOptions(options));
        // add URI options to given options
        final URI uri = getRequestUri(
                endpointConfig.isAuthenticated() ? "coaps" : "coap",
                resource,
                options.getUriQueryString());
        request.setURI(uri);
        Optional.ofNullable(payload).ifPresent(b -> request.setPayload(b.getBytes()));
        return request;
    }
}
