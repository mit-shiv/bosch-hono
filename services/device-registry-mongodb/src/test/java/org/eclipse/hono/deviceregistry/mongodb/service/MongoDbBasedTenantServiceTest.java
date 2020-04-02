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
package org.eclipse.hono.deviceregistry.mongodb.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.eclipse.hono.deviceregistry.mongodb.config.MongoDbBasedTenantsConfigProperties;
import org.eclipse.hono.deviceregistry.mongodb.utils.MongoDbCallExecutor;
import org.eclipse.hono.service.management.tenant.Tenant;
import org.eclipse.hono.service.management.tenant.TenantManagementService;
import org.eclipse.hono.service.management.tenant.TrustedCertificateAuthority;
import org.eclipse.hono.service.tenant.AbstractTenantServiceTest;
import org.eclipse.hono.service.tenant.TenantService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.opentracing.noop.NoopSpan;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
@Timeout(timeUnit = TimeUnit.SECONDS, value = 3)
class MongoDbBasedTenantServiceTest extends AbstractTenantServiceTest {

    private final String DUMMY_TENANT_ID = "dummyTenantId";
    private MongoDbCallExecutor mongoDbCallExecutor;
    private MongoClient mongoClient;
    private MongoDbBasedTenantService svc;

    @BeforeEach
    void setUp() {
        final Context ctx = mock(Context.class);
        final Vertx vertx = mock(Vertx.class);
        final MongoDbBasedTenantsConfigProperties config = new MongoDbBasedTenantsConfigProperties();
        svc = spy(new MongoDbBasedTenantService());
        svc.setConfig(config);
        svc.init(vertx, ctx);

        mongoDbCallExecutor = mock(MongoDbCallExecutor.class);
        mongoClient = new MongoDbClientMock();
        prepareMongoDbCallExecutorMock();
        svc.setExecutor(mongoDbCallExecutor);
        svc.start(Promise.promise());
    }

    private void prepareMongoDbCallExecutorMock() {
        when(mongoDbCallExecutor.getMongoClient()).thenReturn(mongoClient);
        when(mongoDbCallExecutor.createCollectionIndex(anyString(), any(JsonObject.class), any(IndexOptions.class), anyInt()))
                .then(invocation -> {
                    final String collection = invocation.getArgument(0);
                    final JsonObject key = invocation.getArgument(1);
                    final IndexOptions options = invocation.getArgument(2);
                    final Promise<Void> indexCreated = Promise.promise();
                    mongoClient.createIndexWithOptions(collection, key, options, indexCreated);
                    return indexCreated;
                });
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
    public void testAddTenantSucceedsWithGivenTenantId(final VertxTestContext ctx) {

        // WHEN tenant creation method called
        svc.createTenant(Optional.of(DUMMY_TENANT_ID), new Tenant(), NoopSpan.INSTANCE)
                .setHandler(ctx.succeeding(successCreatedTenant -> ctx.verify(() -> {
                    // THEN a new tenant should be inserted
                    assertThat(successCreatedTenant.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);
                    ctx.completeNow();
                })));

    }

    @Test
    public void testGetTenantFailsForNonExistingTenant(final VertxTestContext ctx) {

        // GIVEN tenant collection without a tenant
        // WHEN non tenant is read
        svc.readTenant(DUMMY_TENANT_ID, NoopSpan.INSTANCE)
                .setHandler(ctx.succeeding(successDidReadTenant -> ctx.verify(() -> {
                    // THEN no tenant should be found
                    assertThat(successDidReadTenant.getStatus()).isEqualTo(HttpURLConnection.HTTP_NOT_FOUND);
                    ctx.completeNow();
                })));
    }

    @Test
    public void testGetTenantSucceedsForExistingTenant(final VertxTestContext ctx) {

        // GIVEN a tenant db with an existing tenant
        svc.createTenant(Optional.of(DUMMY_TENANT_ID), new Tenant(), NoopSpan.INSTANCE)
                .map(ok -> {
                    // WHEN existing tenant is requested
                    svc.readTenant(DUMMY_TENANT_ID, null)
                            .setHandler(ctx.succeeding(successDidReadTenant -> ctx.verify(() -> {
                                // THEN a tenant should be found and returned
                                assertThat(successDidReadTenant.getStatus()).isEqualTo(HttpURLConnection.HTTP_OK);
                                assertThat(successDidReadTenant.getPayload()).isInstanceOf(Tenant.class);
                                ctx.completeNow();
                            })));
                    return null;
                });
    }

    @Test
    public void testDeleteTenantWithModificationDisabledFails(final VertxTestContext ctx) {

        // GIVEN a tenant db with an existing tenant
        svc.createTenant(Optional.of(DUMMY_TENANT_ID), new Tenant(), NoopSpan.INSTANCE)
                .map(ok -> {
                    final MongoDbBasedTenantsConfigProperties config = new MongoDbBasedTenantsConfigProperties();
                    config.setModificationEnabled(false);
                    svc.setConfig(config);
                    // WHEN existing tenant is deleted while modification is disabled
                    svc.deleteTenant(DUMMY_TENANT_ID, Optional.empty(), NoopSpan.INSTANCE)
                            .setHandler(ctx.succeeding(result -> ctx.verify(() -> {
                                // THEN deletion should fail
                                assertThat(result.getStatus()).isEqualTo(HttpURLConnection.HTTP_FORBIDDEN);
                                ctx.completeNow();
                            })));
                    return null;
                });
    }

    @Test
    public void testUpdateTenantWithModificationDisabledFails(final VertxTestContext ctx) {

        // GIVEN a tenant db with an existing tenant
        svc.createTenant(Optional.of(DUMMY_TENANT_ID), new Tenant(), NoopSpan.INSTANCE)
                .map(ok -> {
                    final MongoDbBasedTenantsConfigProperties config = new MongoDbBasedTenantsConfigProperties();
                    config.setModificationEnabled(false);
                    svc.setConfig(config);
                    // WHEN existing tenant is updated while modification is disabled
                    svc.updateTenant(DUMMY_TENANT_ID, new Tenant(), Optional.empty(), NoopSpan.INSTANCE)
                            .setHandler(ctx.succeeding(result -> ctx.verify(() -> {
                                // THEN update should fail
                                assertThat(result.getStatus()).isEqualTo(HttpURLConnection.HTTP_FORBIDDEN);
                                ctx.completeNow();
                            })));
                    return null;
                });
    }

    @Test
    public void testCreateTenantWithModificationDisabledFails(final VertxTestContext ctx) {

        final MongoDbBasedTenantsConfigProperties config = new MongoDbBasedTenantsConfigProperties();
        // GIVEN a device registry without tenants with modification is disabled
        config.setModificationEnabled(false);
        svc.setConfig(config);
        // WHEN a tenant is created
        svc.createTenant(Optional.of(DUMMY_TENANT_ID), new Tenant(), NoopSpan.INSTANCE)
                .setHandler(ctx.succeeding(result -> ctx.verify(() -> {
                    // THEN creation should fail
                    assertThat(result.getStatus()).isEqualTo(HttpURLConnection.HTTP_FORBIDDEN);
                    ctx.completeNow();
                })));

    }

    @Test
    public void testCreateTenantFailsForDuplicatedCa(final VertxTestContext ctx) {

        final TrustedCertificateAuthority trustedCa = new TrustedCertificateAuthority()
                .setSubjectDn("CN=taken")
                .setPublicKey("NOTAKEY".getBytes(StandardCharsets.UTF_8));

        final Tenant tenant = new Tenant()
                .setEnabled(true)
                .setTrustedCertificateAuthorities(Collections.singletonList(trustedCa));
        final Tenant tenant2 = new Tenant()
                .setEnabled(true)
                .setTrustedCertificateAuthorities(Collections.singletonList(trustedCa));

        // GIVEN a tenant db with an existing tenant
        svc.createTenant(Optional.of(DUMMY_TENANT_ID), tenant, NoopSpan.INSTANCE)
                .map(ok -> {
                    // WHEN a tenant is created with the same CA
                    svc.createTenant(Optional.of(DUMMY_TENANT_ID + "_2"), tenant2, NoopSpan.INSTANCE)
                            .setHandler(ctx.succeeding(result -> ctx.verify(() -> {
                                // THEN creation should fail due to a CA conflict
                                assertThat(result.getStatus()).isEqualTo(HttpURLConnection.HTTP_CONFLICT);
                                ctx.completeNow();
                            })));
                    return null;
                });
    }

    @Test
    public void testUpdateTenantWithNonExistingTenantFails(final VertxTestContext ctx) {

        // GIVEN a tenant db without tenants
        // WHEN a tenant is updated
        getTenantManagementService().updateTenant(
                DUMMY_TENANT_ID,
                new Tenant(),
                Optional.empty(),
                NoopSpan.INSTANCE)
                .setHandler(ctx.succeeding(s -> {
                    // THEN update should fail be cause of missing tenant
                    ctx.verify(() -> assertEquals(HttpURLConnection.HTTP_NOT_FOUND, s.getStatus()));
                    ctx.completeNow();
                }));
    }

    @Test
    public void testDeleteTenantWithNonExistingTenantFails(final VertxTestContext ctx) {

        // GIVEN a tenant db without tenants
        // WHEN a tenant is deleted
        svc.deleteTenant(DUMMY_TENANT_ID, Optional.empty(), NoopSpan.INSTANCE)
                .setHandler(ctx.succeeding(result -> ctx.verify(() -> {
                    // THEN deletion should fail
                    assertThat(result.getStatus()).isEqualTo(HttpURLConnection.HTTP_NOT_FOUND);
                    ctx.completeNow();
                })));
    }
}
