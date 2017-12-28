/**
 * Copyright (c) 2017, 2018 Bosch Software Innovations GmbH.
 * <p>
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * <p>
 * Contributors:
 * Bosch Software Innovations GmbH - initial creation
 */

package org.eclipse.hono.client.impl;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Objects;
import java.util.UUID;

import io.vertx.core.buffer.Buffer;
import org.eclipse.hono.client.CredentialsClient;
import org.eclipse.hono.client.StatusCodeMapper;
import org.eclipse.hono.config.ClientConfigProperties;
import org.eclipse.hono.util.CacheDirective;
import org.eclipse.hono.util.CredentialsConstants;
import org.eclipse.hono.util.CredentialsObject;
import org.eclipse.hono.util.CredentialsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.ProtonConnection;

/**
 * A Vertx-Proton based client for Hono's Credentials API.
 *
 */
public class CredentialsClientImpl extends AbstractRequestResponseClient<CredentialsResult<CredentialsObject>> implements CredentialsClient {

    private static Logger LOG = LoggerFactory.getLogger(CredentialsClientImpl.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Creates a new client for accessing the Credentials service.
     * 
     * @param context The vert.x context to use for interacting with the service.
     * @param config The configuration properties.
     * @param tenantId The identifier of the tenant for which the client should be created.
     */
    protected CredentialsClientImpl(final Context context, final ClientConfigProperties config, final String tenantId) {
        this(context, config, null, tenantId);
    }

    private CredentialsClientImpl(final Context context, final ClientConfigProperties config, final Tracer tracer, final String tenantId) {
        super(context, config, tracer, tenantId);
    }

    @Override
    protected final String getName() {

        return CredentialsConstants.CREDENTIALS_ENDPOINT;
    }

    @Override
    protected final String createMessageId() {

        return String.format("cred-client-%s", UUID.randomUUID());
    }

    @Override
    protected final CredentialsResult<CredentialsObject> getResult(final int status, final Buffer payload, final CacheDirective cacheDirective) {

        if (payload == null) {
            return CredentialsResult.from(status);
        } else {
            try {
                return CredentialsResult.from(status, OBJECT_MAPPER.readValue(payload.getBytes(), CredentialsObject.class), cacheDirective);
            } catch (final IOException e) {
                LOG.warn("received malformed payload from Credentials service", e);
                return CredentialsResult.from(HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
        }
    }

    /**
     * Gets the AMQP <em>target</em> address to use for sending requests to Hono's Credentials API endpoint.
     * 
     * @param tenantId The tenant to upload data for.
     * @return The target address.
     * @throws NullPointerException if tenant is {@code null}.
     */
    public static final String getTargetAddress(final String tenantId) {
        return String.format("%s/%s", CredentialsConstants.CREDENTIALS_ENDPOINT, Objects.requireNonNull(tenantId));
    }

    /**
     * Creates a new credentials client for a tenant.
     *
     * @param context The vert.x context to run all interactions with the server on.
     * @param clientConfig The configuration properties to use.
     * @param tracer The tracer to use for tracking request processing
     *               across process boundaries.
     * @param con The AMQP connection to the server.
     * @param tenantId The tenant for which credentials are handled.
     * @param senderCloseHook A handler to invoke if the peer closes the sender link unexpectedly.
     * @param receiverCloseHook A handler to invoke if the peer closes the receiver link unexpectedly.
     * @param creationHandler The handler to invoke with the outcome of the creation attempt.
     * @throws NullPointerException if any of the parameters other than tracer is {@code null}.
     */
    public static final void create(
            final Context context,
            final ClientConfigProperties clientConfig,
            final Tracer tracer,
            final ProtonConnection con,
            final String tenantId,
            final Handler<String> senderCloseHook,
            final Handler<String> receiverCloseHook,
            final Handler<AsyncResult<CredentialsClient>> creationHandler) {

        LOG.debug("creating new credentials client for [{}]", tenantId);
        final CredentialsClientImpl client = new CredentialsClientImpl(context, clientConfig, tracer, tenantId);
        client.createLinks(con, senderCloseHook, receiverCloseHook).setHandler(s -> {
            if (s.succeeded()) {
                LOG.debug("successfully created credentials client for [{}]", tenantId);
                creationHandler.handle(Future.succeededFuture(client));
            } else {
                LOG.debug("failed to create credentials client for [{}]", tenantId, s.cause());
                creationHandler.handle(Future.failedFuture(s.cause()));
            }
        });
    }

    /**
     * Invokes the <em>Get Credentials</em> operation of Hono's
     * <a href="https://www.eclipse.org/hono/api/Credentials-API">Credentials API</a>
     * on the service represented by the <em>sender</em> and <em>receiver</em> links.
     */
    @Override
    public Future<CredentialsObject> get(final String type, final String authId) {
        return get(type, authId, (SpanContext) null);
    }

    /**
     * Invokes the <em>Get Credentials</em> operation of Hono's
     * <a href="https://www.eclipse.org/hono/api/Credentials-API">Credentials API</a>
     * on the service represented by the <em>sender</em> and <em>receiver</em> links.
     */
    @Override
    public Future<CredentialsObject> get(final String type, final String authId, final SpanContext parent) {
        return get(type, authId, parent, new JsonObject());
    }

    /**
     * Invokes the <em>Get Credentials</em> operation of Hono's
     * <a href="https://www.eclipse.org/hono/api/Credentials-API">Credentials API</a>
     * on the service represented by the <em>sender</em> and <em>receiver</em> links.
     */
    @Override
    public final Future<CredentialsObject> get(final String type, final String authId, final JsonObject clientContext) {

        return get(type, authId, null, clientContext);
    }

    private Future<CredentialsObject> get(
            final String type,
            final String authId,
            final SpanContext parent,
            final JsonObject clientContext) {

        Objects.requireNonNull(type);
        Objects.requireNonNull(authId);
        Objects.requireNonNull(clientContext);

        final Future<CredentialsResult<CredentialsObject>> responseTracker = Future.future();
        final JsonObject specification = new JsonObject()
                .put(CredentialsConstants.FIELD_TYPE, type)
                .put(CredentialsConstants.FIELD_AUTH_ID, authId)
                .mergeIn(clientContext);
        final Span span = newChildSpan(parent, "get Credentials");
        span.setTag(CredentialsConstants.FIELD_TYPE, type);
        span.setTag(CredentialsConstants.FIELD_AUTH_ID, authId);

        createAndSendRequest(CredentialsConstants.CredentialsAction.get.toString(), specification.toBuffer(), responseTracker.completer(), span);
        return responseTracker.map(response -> {
            Tags.HTTP_STATUS.set(span, response.getStatus());
            span.finish();
            switch(response.getStatus()) {
            case HttpURLConnection.HTTP_OK:
                return response.getPayload();
            default:
                throw StatusCodeMapper.from(response);
            }
        });
    }
}
