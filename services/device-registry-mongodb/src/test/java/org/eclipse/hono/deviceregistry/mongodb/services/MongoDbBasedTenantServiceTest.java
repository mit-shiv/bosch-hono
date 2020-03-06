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
package org.eclipse.hono.deviceregistry.mongodb.services;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.eclipse.hono.deviceregistry.mongodb.config.MongoDbBasedTenantsConfigProperties;
import org.eclipse.hono.deviceregistry.mongodb.config.MongoDbConfigProperties;
import org.eclipse.hono.deviceregistry.mongodb.utils.MongoDbCallExecutor;
import org.eclipse.hono.deviceregistry.util.Versioned;
import org.eclipse.hono.service.management.Id;
import org.eclipse.hono.service.management.OperationResult;
import org.eclipse.hono.service.management.tenant.Tenant;
import org.eclipse.hono.service.management.tenant.TenantManagementService;
import org.eclipse.hono.service.tenant.AbstractTenantServiceTest;
import org.eclipse.hono.service.tenant.TenantService;
import org.eclipse.hono.util.RegistrationConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
@ExtendWith(VertxExtension.class)
@Timeout(timeUnit = TimeUnit.SECONDS, value = 3)
class MongoDbBasedTenantServiceTest  extends AbstractTenantServiceTest {

    private final String DUMMY_TENANT_ID = "dummyTenantId";
    private final MongoDbBasedTenantsConfigProperties config = new MongoDbBasedTenantsConfigProperties();
    private Vertx vertx;
    private MongoDbCallExecutor mongoDbCallExecutor;
    private MongoClient mongoClient;
    private MongoDbBasedTenantsConfigProperties props;
    private MongoDbBasedTenantService svc;
    private MongoDbConfigProperties mongoDbProps;

    @BeforeEach
    void setUp() {
        final Context ctx = mock(Context.class);
        vertx = mock(Vertx.class);
        props = new MongoDbBasedTenantsConfigProperties();
        svc = new MongoDbBasedTenantService();
        mongoDbProps = new MongoDbConfigProperties();
        svc.setConfig(props);
        svc.init(vertx, ctx);

        mongoDbCallExecutor = mock(MongoDbCallExecutor.class);
        mongoClient = mock(MongoClient.class);
        when(mongoDbCallExecutor.getMongoClient()).thenReturn(mongoClient);
        svc.setExecutor(mongoDbCallExecutor);

        // set returns

//        .setSubjectDn("CN=taken")
//        doAnswer((Answer<Void>) invocation -> {
//            final Handler result = (Handler) invocation.getArguments()[2];
//            result.handle(Future.succeededFuture(DUMMY_TENANT_ID));
//            return null;
//        }).when(mongoClient)
//                .insert(anyString(), any(JsonObject.class), any(Handler.class));
    }

         @Override
    public TenantService getTenantService() {
        return svc;
    }

         @Override
    public TenantManagementService getTenantManagementService() {
        return svc;
    }

    @Test
    public void testDoStartHasTenantCollection(final VertxTestContext ctx) {

        // GIVEN a tenant service with existing tenant db collection
        when(mongoClient.getCollections(any(Handler.class))).thenAnswer(
                invocation -> {
                    final Handler result = (Handler) invocation.getArguments()[0];
                    result.handle(Future.succeededFuture(new ArrayList<String>(Arrays.asList("dummyCollection1", config.getCollectionName()))));
                    return null;
                });
        // THEN no new collection should be created
        final Promise<Void> startupTracker = Promise.promise();
        startupTracker.future().setHandler(ctx.succeeding(started -> ctx.verify(() -> {
            verify(mongoDbCallExecutor, never()).createCollectionIndex(anyString(), any(JsonObject.class), any());
            ctx.completeNow();
        })));

        // WHEN starting the service
        svc.start(startupTracker);
    }

    @Test
    public void testDoStartNoExistingTenantCollection(final VertxTestContext ctx) {

        // GIVEN a tenant service without existing tenant collection
        doAnswer((Answer<Void>) invocation -> {
            final Handler result = (Handler) invocation.getArguments()[0];
            result.handle(Future.succeededFuture(new ArrayList<String>(Arrays.asList("dummyCollection1"))));
            return null;
        }).when(mongoClient)
                .getCollections(any(Handler.class));

        when(mongoDbCallExecutor.createCollectionIndex(anyString(), any(JsonObject.class), any()))
                .thenReturn(Future.succeededFuture());

        // THEN a new indexed collection should be created
        final Promise<Void> startupTracker = Promise.promise();
        startupTracker.future().setHandler(ctx.succeeding(started -> ctx.verify(() -> {
            verify(mongoDbCallExecutor, atMostOnce()).createCollectionIndex(anyString(), any(JsonObject.class), any());
            ctx.completeNow();
        })));

        // WHEN starting the service
        svc.start(startupTracker);
    }

    @Test
    public void testAddTenantSucceedsWithGivenTenantId(final VertxTestContext ctx) {

        // GIVEN a empty tenant service db
        doAnswer((Answer<Void>) invocation -> {
            final Handler result = (Handler) invocation.getArguments()[3];
            result.handle(Future.succeededFuture(new JsonObject("{}")));
            return null;
        }).when(mongoClient)
                .findOne(anyString(), any(JsonObject.class), any(JsonObject.class), any(Handler.class));

        doAnswer((Answer<Void>) invocation -> {
            final Handler result = (Handler) invocation.getArguments()[2];
            result.handle(Future.succeededFuture(DUMMY_TENANT_ID));
            return null;
        }).when(mongoClient)
                .insert(anyString(), any(JsonObject.class), any(Handler.class));

        // THEN a new tenant should be inserted and response should be HTTP_CREATED
        final Promise<OperationResult<Id>> createdTenant = Promise.promise();
        createdTenant.future().setHandler(ctx.succeeding(successCreatedTenant -> ctx.verify(() -> {
            assertThat(successCreatedTenant.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);
            ctx.completeNow();
        })));

        // WHEN tenant creation method called
        svc.createTenant(Optional.of(DUMMY_TENANT_ID), new Tenant(), null, createdTenant);
    }
    @Test
    public void testAddTenantSucceedsWithoutTenantId(final VertxTestContext ctx) {

        // GIVEN a empty tenant service db
        doAnswer((Answer<Void>) invocation -> {
            final Handler result = (Handler) invocation.getArguments()[3];
            result.handle(Future.succeededFuture(new JsonObject("{}")));
            return null;
        }).when(mongoClient)
                .findOne(anyString(), any(JsonObject.class), any(JsonObject.class), any(Handler.class));

        doAnswer((Answer<Void>) invocation -> {
            final Handler result = (Handler) invocation.getArguments()[2];
            result.handle(Future.succeededFuture(DUMMY_TENANT_ID));
            return null;
        }).when(mongoClient)
                .insert(anyString(), any(JsonObject.class), any(Handler.class));

        // THEN a new tenant should be inserted and response should be HTTP_CREATED
        final Promise<OperationResult<Id>> createdTenant = Promise.promise();
        createdTenant.future().setHandler(ctx.succeeding(successCreatedTenant -> ctx.verify(() -> {
            assertThat(successCreatedTenant.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);
            ctx.completeNow();
        })));

        // WHEN tenant creation method called
        svc.createTenant(Optional.empty(), new Tenant(), null, createdTenant);
    }

    @Test
    public void testGetTenantFailsForNonExistingTenant(final VertxTestContext ctx) {

        // GIVEN tenant collection without a tenant with the dummy id
        doAnswer((Answer<Void>) invocation -> {
            final Handler<Future<JsonObject>> result = (Handler<Future<JsonObject>>) invocation.getArguments()[3];
            result.handle(Future.succeededFuture());
            return null;
        }).when(mongoClient)
                .findOne(anyString(), any(JsonObject.class), any(JsonObject.class), any(Handler.class));

        // THEN no tenant should be found and HTTP_NOT_FOUND 404 should be returned
        final Promise<OperationResult<Tenant>> didReadTenant = Promise.promise();
        didReadTenant.future().setHandler(ctx.succeeding(successDidReadTenant -> ctx.verify(() -> {
            assertThat(successDidReadTenant.getStatus()).isEqualTo(HttpURLConnection.HTTP_NOT_FOUND);
            ctx.completeNow();
        })));
        // WHEN tenant read method called
        svc.readTenant(DUMMY_TENANT_ID, null, didReadTenant);
    }

    @Test
    public void testGetTenantSucceedsForExistingTenant(final VertxTestContext ctx) {

        // GIVEN a tenant db with an existing tenant
        doAnswer((Answer<Void>) invocation -> {
            final Handler result = (Handler) invocation.getArguments()[3];
            final var dummyTenant = new Tenant();
            final Versioned<Tenant> newDummyTenant = new Versioned<>(dummyTenant);
            final JsonObject newTenantJson = JsonObject.mapFrom(newDummyTenant).put(RegistrationConstants.FIELD_PAYLOAD_TENANT_ID, DUMMY_TENANT_ID);
            result.handle(Future.succeededFuture(newTenantJson));
            return null;
        }).when(mongoClient)
                .findOne(anyString(), any(JsonObject.class), any(JsonObject.class), any(Handler.class));

        // THEN a tenant should be found and returned
        final Promise<OperationResult<Tenant>> didReadTenant = Promise.promise();
        didReadTenant.future().setHandler(ctx.succeeding(successDidReadTenant -> ctx.verify(() -> {
            assertThat(successDidReadTenant.getStatus()).isEqualTo(HttpURLConnection.HTTP_OK);
            assertThat(successDidReadTenant.getPayload()).isInstanceOf(Tenant.class);
            ctx.completeNow();
        })));

        // WHEN existing tenant is requested
        svc.readTenant(DUMMY_TENANT_ID, null, didReadTenant);
    }
}
