/*******************************************************************************
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
 *******************************************************************************/
package org.eclipse.hono.deviceregistry.mongodb;

import io.opentracing.Span;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.json.JsonObject;
import org.eclipse.hono.deviceregistry.mongodb.services.MongoDbBasedTenantService;
import org.eclipse.hono.service.management.Id;
import org.eclipse.hono.service.management.OperationResult;
import org.eclipse.hono.service.management.Result;
import org.eclipse.hono.service.management.tenant.Tenant;
import org.eclipse.hono.service.management.tenant.TenantBackend;
import org.eclipse.hono.util.TenantResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import javax.security.auth.x500.X500Principal;
import java.util.Optional;

/**
 * A tenant backend for a mongodb based of device registry.
 */
@Repository
@Qualifier("backend")
@ConditionalOnProperty(name = "hono.app.type", havingValue = "mongodb", matchIfMissing = true)
public class MongoDbBasedTenantBackend extends AbstractVerticle
        implements TenantBackend, Verticle {

    private final MongoDbBasedTenantService tenantService;

    /**
     * Create a new instance.
     *
     * @param tenantService an implementation of tenant service.
     */
    @Autowired
    public MongoDbBasedTenantBackend(
            @Qualifier("serviceImpl") final MongoDbBasedTenantService tenantService) {
        this.tenantService = tenantService;
    }

    @Override
    public void createTenant(final Optional<String> tenantId, final Tenant tenantObj, final Span span, final Handler<AsyncResult<OperationResult<Id>>> resultHandler) {
        tenantService.createTenant(tenantId, tenantObj, span, resultHandler);

    }

    @Override
    public void readTenant(final String tenantId, final Span span, final Handler<AsyncResult<OperationResult<Tenant>>> resultHandler) {
        tenantService.readTenant(tenantId, span, resultHandler);

    }

    @Override
    public void updateTenant(final String tenantId, final Tenant tenantObj, final Optional<String> resourceVersion, final Span span, final Handler<AsyncResult<OperationResult<Void>>> resultHandler) {
        tenantService.updateTenant(tenantId, tenantObj, resourceVersion, span, resultHandler);
    }

    @Override
    public void deleteTenant(final String tenantId, final Optional<String> resourceVersion, final Span span, final Handler<AsyncResult<Result<Void>>> resultHandler) {
        tenantService.deleteTenant(tenantId, resourceVersion, span, resultHandler);
    }

    @Override
    public void get(final String tenantId, final Handler<AsyncResult<TenantResult<JsonObject>>> resultHandler) {

    }

    @Override
    public void get(final X500Principal subjectDn, final Handler<AsyncResult<TenantResult<JsonObject>>> resultHandler) {
//        resultHandler.handle(Future.failedFuture(Result.from(HttpURLConnection.HTTP_NOT_IMPLEMENTED)));
    }
}
