/*******************************************************************************
 * Copyright (c) 2016, 2019 Contributors to the Eclipse Foundation
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

import io.opentracing.Span;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientDeleteResult;
import org.eclipse.hono.deviceregistry.mongodb.config.MongoDbBasedTenantsConfigProperties;
import org.eclipse.hono.deviceregistry.mongodb.utils.MongoDbCallExecutor;
import org.eclipse.hono.deviceregistry.mongodb.utils.MongoDbDocumentBuilder;
import org.eclipse.hono.deviceregistry.mongodb.utils.MongoDbErrorHandler;
import org.eclipse.hono.deviceregistry.util.DeviceRegistryUtils;
import org.eclipse.hono.deviceregistry.util.Versioned;
import org.eclipse.hono.service.management.Id;
import org.eclipse.hono.service.management.OperationResult;
import org.eclipse.hono.service.management.Result;
import org.eclipse.hono.service.management.tenant.Tenant;
import org.eclipse.hono.service.management.tenant.TenantManagementService;
import org.eclipse.hono.service.tenant.TenantService;
import org.eclipse.hono.tracing.TracingHelper;
import org.eclipse.hono.util.RegistrationConstants;
import org.eclipse.hono.util.TenantConstants;
import org.eclipse.hono.util.TenantResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.security.auth.x500.X500Principal;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * A tenant backend that uses a mongodb client to manage tenants.
 */
@Component
@Qualifier("serviceImpl")
@ConditionalOnProperty(name = "hono.app.type", havingValue = "mongodb", matchIfMissing = true)
public final class MongoDbBasedTenantService extends AbstractVerticle implements TenantService, TenantManagementService {

    private static final Logger log = LoggerFactory.getLogger(MongoDbBasedTenantService.class);

    private MongoDbCallExecutor mongoDbCallExecutor;
    private MongoClient mongoClient;
    private MongoDbBasedTenantsConfigProperties config;

    /**
     * Autowires the mongodb client.
     *
     * @param mongoDbCallExecutor the executor singleton
     */
    @Autowired
    public void setExecutor(final MongoDbCallExecutor mongoDbCallExecutor) {
        this.mongoDbCallExecutor = mongoDbCallExecutor;
        this.mongoClient = this.mongoDbCallExecutor.getMongoClient();
    }

    public MongoDbBasedTenantsConfigProperties getConfig() {
        return config;
    }

    /**
     * Autowires the tenant config.
     *
     * @param configuration The tenant configuration
     */
    @Autowired
    public void setConfig(final MongoDbBasedTenantsConfigProperties configuration) {
        this.config = configuration;
    }

    @Override
    public void start(final Promise<Void> startPromise) {
        final Promise<List<String>> existingCollections = Promise.promise();
        mongoClient.getCollections(existingCollections);
        existingCollections.future()
                .compose(successExistingCollections -> {
                    if (successExistingCollections.contains(getConfig().getCollectionName())) {
                        return Future.succeededFuture();
                    } else {
                        // create index & implicit collection
                        return mongoDbCallExecutor.createCollectionIndex(getConfig().getCollectionName(),
                                new JsonObject().put(TenantConstants.FIELD_PAYLOAD_TENANT_ID, 1).put(TenantConstants.FIELD_PAYLOAD_DEVICE_ID, 1),
                                new IndexOptions().unique(true));
                    }
                })
                .compose(success -> {
                    if (getConfig().isModificationEnabled()) {
                        log.info("persistence is disabled, will not save tenant identities to mongoDB.");
                    }
                    log.debug("startup complete");
                    startPromise.complete();
                    return Future.succeededFuture();
                }).onFailure(reason -> {
            log.error("Index creation failed", reason);
            startPromise.fail(reason.toString());
        });
    }

    @Override
    public void stop(final Promise<Void> stopPromise) {
        this.mongoClient.close();
        stopPromise.complete();
    }

    @Override
    public void updateTenant(final String tenantId, final Tenant tenantObj, final Optional<String> resourceVersion, final Span span, final Handler<AsyncResult<OperationResult<Void>>> resultHandler) {

        // TODO: check TrustedCertificateAuthoritySubjectDN in use

        if (!getConfig().isModificationEnabled()) {
            final String errorMsg = "Modification disabled for tenant service.";
            TracingHelper.logError(span, errorMsg);
            log.info(errorMsg);
            resultHandler.handle(Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_FORBIDDEN)));
            return;
        }

        final Promise<Result<Void>> tenantDeletion = Promise.promise();
        deleteTenant(tenantId, resourceVersion, span, tenantDeletion);
        tenantDeletion.future().compose(successTenantDeletion -> {
            final Promise<OperationResult<Id>> tenantCreation = Promise.promise();
            createTenant(Optional.of(tenantId), tenantObj, span, tenantCreation);
            return tenantCreation.future();

        })
                .compose(successTenantCreation -> {
                    final OperationResult<Void> opResult = OperationResult.ok(
                            HttpURLConnection.HTTP_NO_CONTENT,
                            null,
                            Optional.empty(),
                            successTenantCreation.getResourceVersion()
                    );
                    return Future.succeededFuture(opResult);
                })
                .recover(errorTenantCreation -> MongoDbErrorHandler.operationFailed(log, span, String.format("tenant [%s] could no be updated.", tenantObj)))
                .setHandler(resultHandler);
    }

    @Override
    public void deleteTenant(final String tenantId, final Optional<String> resourceVersion, final Span span, final Handler<AsyncResult<Result<Void>>> resultHandler) {

        // TODO: check api version
        // -> TracingHelper.logError(span, "Resource Version mismatch.");
        //                    return Result.from(HttpURLConnection.HTTP_PRECON_FAILED);

        if (!getConfig().isModificationEnabled()) {
            final String errorMsg = "Modification disabled for tenant service.";
            TracingHelper.logError(span, errorMsg);
            log.info(errorMsg);
            resultHandler.handle(Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_FORBIDDEN)));
            return;
        }

        final Promise<MongoClientDeleteResult> deleteTenant = Promise.promise();
        final JsonObject removeTenantQuery = new MongoDbDocumentBuilder()
                .withTenantId(tenantId)
                .create();
        mongoClient.removeDocument(getConfig().getCollectionName(), removeTenantQuery, deleteTenant);
        deleteTenant.future().compose(successDeleteTenant -> {
            if (successDeleteTenant.getRemovedCount() == 1) {
                return Future.succeededFuture(Result.<Void>from(HttpURLConnection.HTTP_NO_CONTENT));
            } else {
                return Future.succeededFuture(Result.<Void>from(HttpURLConnection.HTTP_NOT_FOUND));
            }
        })
                .recover(errorDeleteTenant -> MongoDbErrorHandler.operationFailed(log, span, String.format("tenant with id [%s] could no be deleted.", tenantId)))
                .setHandler(resultHandler);

    }

    @Override
    public void get(final String tenantId, final Handler<AsyncResult<TenantResult<JsonObject>>> resultHandler) {

    }

    @Override
    public void get(final X500Principal subjectDn, final Handler<AsyncResult<TenantResult<JsonObject>>> resultHandler) {

    }

    @Override
    public void readTenant(final String tenantId, final Span span, final Handler<AsyncResult<OperationResult<Tenant>>> resultHandler) {
        final JsonObject findTenantQuery = new MongoDbDocumentBuilder()
                .withTenantId(tenantId)
                .create();
        final Promise<JsonObject> didReadTenant = Promise.promise();
        mongoClient.findOne(getConfig().getCollectionName(), findTenantQuery, new JsonObject(), didReadTenant);
        didReadTenant.future().compose(successDidReadTenant -> Optional.ofNullable(successDidReadTenant)
                .map(found -> Future.succeededFuture(OperationResult.ok(
                        HttpURLConnection.HTTP_OK,
                        found.mapTo(Tenant.class),
                        Optional.ofNullable(DeviceRegistryUtils.getCacheDirective(config.getCacheMaxAge())),
                        Optional.ofNullable(found.getString("version")))
                )).orElseGet(() -> Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_NOT_FOUND))))
                .recover(errorDidReadTenant -> MongoDbErrorHandler.operationFailed(log, span, "tenants could not be read."))
                .setHandler(resultHandler);
    }

    private Future<Boolean> tenantIdExist(final String tenantId) {
        final JsonObject findTenantQuery = new MongoDbDocumentBuilder()
                .withTenantId(tenantId)
                .create();
        final Promise<JsonObject> checkedIdExist = Promise.promise();
        mongoClient.findOne(getConfig().getCollectionName(), findTenantQuery, new JsonObject(), checkedIdExist);
        return checkedIdExist.future().compose(successCheckedIdExist -> Future.succeededFuture(successCheckedIdExist != null && !successCheckedIdExist.isEmpty()))
                .recover(errorDidReadTenant -> MongoDbErrorHandler.operationFailed(log, null, "tenants could not be read."));
    }

    /**
     * Generate a random tenant ID.
     */
    private Future<String> createConflictFreeUUID() {
        final String id = UUID.randomUUID().toString();
        return tenantIdExist(id).compose(successIdExist -> {
            if (successIdExist) {
                return createConflictFreeUUID();
            } else {
                return Future.succeededFuture(id);
            }
        });
    }

    @Override
    public void createTenant(final Optional<String> tenantId, final Tenant tenantObj, final Span span, final Handler<AsyncResult<OperationResult<Id>>> resultHandler) {

        if (!getConfig().isModificationEnabled()) {
            final String errorMsg = "Modification disabled for tenant service.";
            TracingHelper.logError(span, errorMsg);
            log.info(errorMsg);
            resultHandler.handle(Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_FORBIDDEN)));
            return;
        }

        final Future<String> getTenantId;
        final Versioned<Tenant> newTenant = new Versioned<>(tenantObj);

        if (tenantId.isPresent()) {
            getTenantId = Future.succeededFuture(tenantId.get());
        } else {
            getTenantId = createConflictFreeUUID();
        }

        getTenantId
                .compose(successTenantId -> {
                    final Promise<String> getTenantInsertion = Promise.promise();
                    final JsonObject newTenantJson = JsonObject.mapFrom(newTenant).put(RegistrationConstants.FIELD_PAYLOAD_TENANT_ID, successTenantId);
                    mongoClient.insert(getConfig().getCollectionName(), newTenantJson, getTenantInsertion);
                    return getTenantInsertion.future()
                            .compose(successTenantInsertion -> Future.succeededFuture(OperationResult.ok(HttpURLConnection.HTTP_CREATED, Id.of(successTenantId), Optional.empty(), Optional.of(newTenant.getVersion()))))
                            .recover(errorTenantInsertion -> {
                                if (MongoDbErrorHandler.ifDuplicateKeyError(errorTenantInsertion)) {
                                    final var errorMsg = String.format("Tenant [%s] with id [%s] already exist.", newTenant.getValue(), successTenantId);
                                    log.error(errorMsg);
                                    TracingHelper.logError(span, errorMsg);
                                    return Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_CONFLICT));
                                } else {
                                    final var errorMsg = String.format("Tenant [%s] could no be created.", newTenant.getValue());
                                    log.error(errorMsg);
                                    TracingHelper.logError(span, errorMsg);
                                    return Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_INTERNAL_ERROR));
                                }
                            });
                })
                .recover(errorTenantId -> {
                    final var errorMsg = String.format("Tenant [%s] could no be created.", newTenant.getValue());
                    log.error(errorMsg);
                    TracingHelper.logError(span, errorMsg);
                    return Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_INTERNAL_ERROR));
                })
                .setHandler(resultHandler);
    }
}
