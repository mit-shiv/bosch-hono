/**
 * Copyright (c) 2017, 2018 Bosch Software Innovations GmbH.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Bosch Software Innovations GmbH - initial creation
 */

package org.eclipse.hono.service.auth.device;

import io.opentracing.SpanContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;


/**
 * An authentication provider for verifying credentials that also supports monitoring by
 * means of health checks.
 *
 */
public interface HonoClientBasedAuthProvider extends AuthProvider {

    /**
     * Authenticate a user.
     * <p>
     * The first argument is a JSON object containing information for authenticating the user. What this actually contains
     * depends on the specific implementation. In the case of a simple username/password based
     * authentication it is likely to contain a JSON object with the following structure:
     * <pre>
     *   {
     *     "username": "tim",
     *     "password": "mypassword"
     *   }
     * </pre>
     * For other types of authentication it contain different information - for example a JWT token or OAuth bearer token.
     * <p>
     * If the user is successfully authenticated a {@link User} object is passed to the handler in an {@link io.vertx.core.AsyncResult}.
     * The user object can then be used for authorisation.
     *
     * @param authInfo  The auth information
     * @param currentSpan The Opentracing span providing the context for this execution.
     * @param resultHandler  The result handler
     */
    void authenticate(JsonObject authInfo, SpanContext currentSpan, Handler<AsyncResult<User>> resultHandler);

    /**
     * Validates credentials provided by a device against the credentials on record
     * for the device.
     * <p>
     * The credentials on record are retrieved using Hono's
     *  <a href="https://www.eclipse.org/hono/api/Credentials-API/">Credentials API</a>.
     *
     * @param credentials The credentials provided by the device.
     * @param resultHandler The handler to notify about the outcome of the validation. If validation succeeds,
     *                      the result contains an object representing the authenticated device.
     * @throws NullPointerException if credentials or result handler are {@code null}.
     */
    void authenticate(DeviceCredentials credentials, Handler<AsyncResult<Device>> resultHandler);

    /**
     * Validates credentials provided by a device against the credentials on record
     * for the device.
     * <p>
     * The credentials on record are retrieved using Hono's
     *  <a href="https://www.eclipse.org/hono/api/Credentials-API/">Credentials API</a>.
     *
     * @param credentials The credentials provided by the device.
     * @param currentSpan The OpenTracing span providing the context for this execution.
     * @param resultHandler The handler to notify about the outcome of the validation. If validation succeeds,
     *                      the result contains an object representing the authenticated device.
     * @throws NullPointerException if credentials or result handler are {@code null}.
     */
    void authenticate(DeviceCredentials credentials, SpanContext currentSpan, Handler<AsyncResult<Device>> resultHandler);
}
