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

import java.net.HttpURLConnection;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.security.auth.x500.X500Principal;

import org.eclipse.hono.client.ClientErrorException;
import org.eclipse.hono.client.ServerErrorException;
import org.eclipse.hono.deviceregistry.mongodb.config.MongoDbBasedTenantsConfigProperties;
import org.eclipse.hono.deviceregistry.mongodb.model.TenantDto;
import org.eclipse.hono.deviceregistry.mongodb.utils.MongoDbCallExecutor;
import org.eclipse.hono.deviceregistry.mongodb.utils.MongoDbDocumentBuilder;
import org.eclipse.hono.deviceregistry.mongodb.utils.MongoDbDeviceRegistryUtils;
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
import org.eclipse.hono.util.TenantResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import io.opentracing.Span;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientDeleteResult;
import io.vertx.ext.mongo.MongoClientUpdateResult;

/**
 * An implementation of the tenant service and the tenant management service
 * that uses a mongodb database to store tenants.
 * <p>
 * On startup this adapter tries to find the tenant collection, if not found, it gets created.
 *
 * @see <a href="https://www.eclipse.org/hono/docs/api/tenant/">Tenant API</a>
 * @see <a href="https://www.eclipse.org/hono/docs/api/management/">Device Registry Management API</a>
 */
@Component
@Qualifier("serviceImpl")
public final class MongoDbBasedTenantService extends AbstractVerticle
        implements TenantService, TenantManagementService {

    private static final Logger log = LoggerFactory.getLogger(MongoDbBasedTenantService.class);

    private static final int INDEX_CREATION_MAX_RETRIES = 3;
    private MongoClient mongoClient;
    private MongoDbCallExecutor mongoDbCallExecutor;
    private MongoDbBasedTenantsConfigProperties config;

    /**
     * Sets an instance of the {@code MongoDbCallExecutor} and {@code MongoClient}.
     *
     * @param mongoDbCallExecutor An instance of the mongoDbCallExecutor.
     * @throws NullPointerException if the mongoDbCallExecutor is {@code null}.
     */
    @Autowired
    public void setExecutor(final MongoDbCallExecutor mongoDbCallExecutor) {
        this.mongoDbCallExecutor = Objects.requireNonNull(mongoDbCallExecutor);
        this.mongoClient = Objects.requireNonNull(this.mongoDbCallExecutor.getMongoClient());
    }

    public MongoDbBasedTenantsConfigProperties getConfig() {
        return config;
    }

    /**
     * Sets the configuration properties for this service.
     *
     * @param config The configuration properties.
     * @throws NullPointerException if the configuration is {@code null}.
     */
    @Autowired
    public void setConfig(final MongoDbBasedTenantsConfigProperties config) {
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public void start(final Promise<Void> startPromise) {
        mongoDbCallExecutor.createCollectionIndex(config.getCollectionName(),
                new JsonObject().put(RegistrationConstants.FIELD_PAYLOAD_TENANT_ID,
                        1),
                new IndexOptions().unique(true), INDEX_CREATION_MAX_RETRIES)
                .map(success -> {
                    startPromise.complete();
                    return null;
                })
                .onFailure(error -> {
                    log.error("Index creation failed", error);
                    startPromise.fail(error);
                });
    }

    @Override
    public void stop(final Promise<Void> stopPromise) {
        this.mongoClient.close();
        stopPromise.complete();
    }

    /**
     * Updates configuration information of a tenant.
     *
     * @param tenantId The identifier of the tenant.
     * @param tenantObj The updated configuration information for the tenant (may be {@code null}).
     * @param resourceVersion The identifier of the resource version to update.
     * @param span The active OpenTracing span for this operation. It is not to be closed in this method! An
     *            implementation should log (error) events on this span and it may set tags and use this span as the
     *            parent for any spans created in this method.
     * @return A future indicating the outcome of the operation.
     *         The <em>status</em> will be
     *         <ul>
     *         <li><em>204 No Content</em> if the tenant has been removed successfully.</li>
     *         <li><em>403 forbidden</em> if tenant modification is forbidden due to configuration settings</li>
     *         <li><em>404 Not Found</em> if no tenant with the given identifier and version exists.</li>
     *         <li><em>409 Conflict</em> if provided trusted certificate authority is already present in another
     *         tenant.</li>
     *         <li><em>412 Precondition Failed</em> if the version exists but mismatch the current tenants version.</li>
     *         </ul>
     * @throws NullPointerException if any of the parameters is {@code null}.
     * @see <a href="https://www.eclipse.org/hono/docs/api/management/#/tenants/updateTenant"> Device Registry
     *      Management API - Update Tenant</a>
     */
    @Override
    public Future<OperationResult<Void>> updateTenant(final String tenantId, final Tenant tenantObj,
            final Optional<String> resourceVersion, final Span span) {
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(tenantObj);
        Objects.requireNonNull(resourceVersion);

        return MongoDbDeviceRegistryUtils.isAllowedToModify(config, tenantId)
                .compose(ok -> findTenant(tenantId))
                .compose(tenantDto -> checkConflictingCATenants(Optional.of(tenantDto),
                        tenantObj.getTrustedCertificateAuthoritySubjectDNs(), span))
                .compose(tenantDto -> processUpdateTenant(tenantId, tenantObj, resourceVersion, span))
                .recover(error -> Future.succeededFuture(MongoDbDeviceRegistryUtils.mapErrorToResult(error, span)));
    }

    private Future<OperationResult<Void>> processUpdateTenant(final String tenantId, final Tenant newTenant,
            final Optional<String> resourceVersion, final Span span) {
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(newTenant);
        Objects.requireNonNull(resourceVersion);

        final JsonObject tenantUpdateQuery = resourceVersion
                .map(version -> new MongoDbDocumentBuilder().withVersion(version))
                .orElse(new MongoDbDocumentBuilder())
                .withTenantId(tenantId)
                .document();

        final Promise<MongoClientUpdateResult> tenantUpdated = Promise.promise();
        final TenantDto newTenantDto = new TenantDto(tenantId, newTenant,
                new Versioned<>(newTenant).getVersion());
        mongoClient.updateCollection(config.getCollectionName(), tenantUpdateQuery,
                new JsonObject().put("$set", JsonObject.mapFrom(newTenantDto)), tenantUpdated);

        return tenantUpdated.future()
                .compose(tenantUpdateResult -> {
                    if (tenantUpdateResult.getDocMatched() == 0) {

                        return getTenantNotFoundOrVersionMismatch(tenantId, resourceVersion);
                    } else {
                        span.log(String.format("successfully updated tenant [%s]", tenantId));
                        return Future.succeededFuture(OperationResult.ok(
                                HttpURLConnection.HTTP_NO_CONTENT,
                                null,
                                Optional.empty(),
                                Optional.of(newTenantDto.getVersion())));
                    }

                });
    }

    /**
     * Checks if a tenant cannot be found or tenant mismatches provided version.
     *
     * @param tenantId The tenant id to be checked.
     * @param resourceVersion The version to be checked.
     * @return A future indicating the outcome of the operation.
     *         The <em>status</em> will be
     *         <ul>
     *         <li><em>404 Not Found</em> if no tenant with the given identifier and version exists.</li>
     *         <li><em>412 Precondition Failed</em> if the version exists but mismatch the current tenants version.</li>
     *         <li><em>500 Internal Server Error</em> if the tenant under test does not generate any of the above errors.
     *         This means that none of the expected standard cases happened.</li>
     *         </ul>
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    private <T extends Result<Void>> Future<T> getTenantNotFoundOrVersionMismatch(final String tenantId,
            final Optional<String> resourceVersion) {
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(resourceVersion);

        return findTenant(tenantId)
                .compose(tenantDto -> MongoDbDeviceRegistryUtils.checkResourceVersion(tenantDto,
                        resourceVersion))
                .compose(t -> Future.<T> failedFuture(
                        new ServerErrorException(HttpURLConnection.HTTP_INTERNAL_ERROR,
                                "Unknown error: Should be version mismatch or tenant not found.")))
                .recover(Future::failedFuture);
    }

    /**
     * Deletes a present tenant.
     *
     * <p>
     * Provided version {@code resourceVersion} have to be empty or equal to the version of present tenant.
     * </p>
     *
     * @param tenantId The identifier of the tenant.
     * @param resourceVersion The identifier of the resource version to delete.
     * @param span The active OpenTracing span for this operation. It is not to be closed in this method! An
     *            implementation should log (error) events on this span and it may set tags and use this span as the
     *            parent for any spans created in this method.
     * @return A future indicating the outcome of the operation.
     *         The <em>status</em> will be
     *         <ul>
     *         <li><em>204 No Content</em> if the tenant has been removed successfully.</li>
     *         <li><em>403 forbidden</em> if tenant modification is forbidden due to configuration settings.</li>
     *         <li><em>404 Not Found</em> if no tenant with the given identifier and version exists.</li>
     *         <li><em>412 Precondition Failed</em> if the version exists but mismatch the current tenants version.</li>
     *         </ul>
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    @Override
    public Future<Result<Void>> deleteTenant(final String tenantId, final Optional<String> resourceVersion,
            final Span span) {
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(resourceVersion);

        return MongoDbDeviceRegistryUtils.isAllowedToModify(config, tenantId)
                .compose(ok -> processDeleteTenant(tenantId, resourceVersion, span))
                .recover(error -> Future.succeededFuture(MongoDbDeviceRegistryUtils.mapErrorToResult(error, span)));
    }

    private Future<Result<Void>> processDeleteTenant(final String tenantId, final Optional<String> resourceVersion,
            final Span span) {
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(resourceVersion);

        final JsonObject tenantDeleteQuery = resourceVersion
                .map(version -> new MongoDbDocumentBuilder().withVersion(version))
                .orElse(new MongoDbDocumentBuilder())
                .withTenantId(tenantId)
                .document();

        final Promise<MongoClientDeleteResult> deleteTenantPromise = Promise.promise();
        mongoClient.removeDocument(config.getCollectionName(), tenantDeleteQuery, deleteTenantPromise);
        return deleteTenantPromise.future()
                .compose(tenantDeleteResult -> {
                    if (tenantDeleteResult.getRemovedCount() == 0) {
                        return getTenantNotFoundOrVersionMismatch(tenantId, resourceVersion);
                    } else {
                        span.log(String.format("successfully deleted tenant [%s]", tenantId));
                        return Future.succeededFuture(Result.from(HttpURLConnection.HTTP_NO_CONTENT));
                    }
                });
    }

    @Override
    public Future<TenantResult<JsonObject>> get(final String tenantId) {
        Objects.requireNonNull(tenantId);

        return get(tenantId, null);
    }

    @Override
    public Future<TenantResult<JsonObject>> get(final String tenantId, final Span span) {
        Objects.requireNonNull(tenantId);

        return readTenant(tenantId, span)
                .compose(successTenantRead -> {
                    if (successTenantRead.getStatus() != HttpURLConnection.HTTP_OK) {
                        TracingHelper.logError(span, String.format("tenant [%s] not found.", tenantId));
                        return Future.succeededFuture(TenantResult.from(HttpURLConnection.HTTP_NOT_FOUND));
                    }
                    return Future.succeededFuture(TenantResult.from(
                            HttpURLConnection.HTTP_OK,
                            DeviceRegistryUtils
                                    .convertTenant(tenantId, successTenantRead.getPayload(), true),
                            DeviceRegistryUtils.getCacheDirective(config.getCacheMaxAge())));
                });
    }

    @Override
    public Future<TenantResult<JsonObject>> get(final X500Principal subjectDn) {
        Objects.requireNonNull(subjectDn);

        return get(subjectDn, null);
    }

    @Override
    public Future<TenantResult<JsonObject>> get(final X500Principal subjectDn, final Span span) {
        Objects.requireNonNull(subjectDn);

        return getForCertificateAuthority(subjectDn, span);
    }

    /**
     * Finds an existing tenant by the subject DN of its configured certificate authority.
     *
     * @param subjectDn the subject DN to find the tenant with.
     * @param span The active OpenTracing span for this operation. It is not to be closed in this method!
     *            An implementation should log (error) events on this span and it may set tags
     *            and use this span as the parent for any spans created in this method.
     * @return A future indicating the outcome of the operation.
     *         The <em>status</em> will be
     *         <ul>
     *         <li><em>200 OK</em> if a tenant with the given certificate authority is registered.
     *         The <em>payload</em> will contain the tenant's configuration information.</li>
     *         <li><em>404 Bad Request</em> if the certificate authority is missing.</li>
     *         <li><em>404 Not Found</em> if no tenant with the given identifier.</li>
     *         </ul>
     */
    private Future<TenantResult<JsonObject>> getForCertificateAuthority(final X500Principal subjectDn,
            final Span span) {
        if (subjectDn == null) {
            TracingHelper.logError(span, "missing subject DN");
            return Future.succeededFuture(TenantResult.from(HttpURLConnection.HTTP_BAD_REQUEST));
        } else {
            final Future<Map.Entry<String, Versioned<Tenant>>> tenantsFound = getByCa(subjectDn);
            return tenantsFound.compose(successTenantRead -> {
                if (successTenantRead == null) {
                    TracingHelper.logError(span, "no tenant found for subject DN");
                    return Future.succeededFuture(TenantResult.from(HttpURLConnection.HTTP_NOT_FOUND));
                } else {
                    return Future.succeededFuture(TenantResult.from(
                            HttpURLConnection.HTTP_OK,
                            DeviceRegistryUtils.convertTenant(successTenantRead.getKey(),
                                    successTenantRead.getValue().getValue(), true),
                            DeviceRegistryUtils.getCacheDirective(config.getCacheMaxAge())));
                }
            });
        }
    }

    /**
     * Fetches tenant by subject DN.
     *
     * @param subjectDn the subject DN.
     * @return A Future Map consisting of pair of a tenant id and a versioned tenant.
     */
    private Future<Map.Entry<String, Versioned<Tenant>>> getByCa(final X500Principal subjectDn) {

        if (subjectDn == null) {
            return Future.failedFuture("missing subject DN");
        } else {
            final JsonObject findTenantQuery = new MongoDbDocumentBuilder()
                    .withCa("tenant", subjectDn.getName())
                    .document();

            final Promise<JsonObject> tenantsFound = Promise.promise();
            mongoClient.findOne(getConfig().getCollectionName(), findTenantQuery, new JsonObject(), tenantsFound);
            return tenantsFound.future().compose(successTenantsFound -> {
                if (successTenantsFound == null || successTenantsFound.size() == 0) {
                    return Future.succeededFuture(null);
                }
                final TenantDto tenantDtoFound = successTenantsFound.mapTo(TenantDto.class);
                return Future.succeededFuture(
                        new AbstractMap.SimpleEntry<>(tenantDtoFound.getTenantId(),
                                new Versioned<>(tenantDtoFound.getVersion(),
                                        tenantDtoFound.getTenant())));
            });
        }
    }

    @Override
    public Future<OperationResult<Tenant>> readTenant(final String tenantId, final Span span) {
        Objects.requireNonNull(tenantId);

        return processReadTenant(tenantId, span)
                .recover(error -> Future.succeededFuture(MongoDbDeviceRegistryUtils.mapErrorToResult(error, span)));
    }

    /**
     * Retrieves a tenant by tenant id.
     *
     * @param tenantId The identifier of the tenant.
     * @param span The active OpenTracing span for this operation. It is not to be closed in this method!
     *             An implementation should log (error) events on this span and it may set tags
     *             and use this span as the parent for any spans created in this method.
     * @return A future indicating the outcome of the operation.
     *         The <em>status</em> will be
     *         <ul>
     *         <li><em>200 OK</em> if a tenant with the given ID is registered. The <em>payload</em> will contain the
     *         tenant's configuration information.</li>
     *         <li><em>404 Not Found</em> if no tenant with the given identifier and version exists.</li>
     *         </ul>
     * @throws NullPointerException if {@code tenantId} is {@code null}.
     */
    private Future<OperationResult<Tenant>> processReadTenant(final String tenantId, final Span span) {
        Objects.requireNonNull(tenantId);

        return findTenant(tenantId)
                .compose(tenantDto -> Future.succeededFuture(OperationResult.ok(
                        HttpURLConnection.HTTP_OK,
                        tenantDto.getTenant(),
                        Optional.ofNullable(DeviceRegistryUtils.getCacheDirective(config.getCacheMaxAge())),
                        Optional.ofNullable(tenantDto.getVersion()))));

    }

    /**
     * Gets a tenant dto by tenant id.
     *
     * @param tenantId the tenant id.
     * @return A future indicating the outcome of the operation.
     *         <p>
     *         The future with a {@link TenantDto} be provided if found.
     *         Otherwise the future will fail with
     *         a {@link ClientErrorException} with {@link HttpURLConnection#HTTP_NOT_FOUND}.
     * @throws NullPointerException if the parameters is {@code null}.
     */
    private Future<TenantDto> findTenant(final String tenantId) {
        Objects.requireNonNull(tenantId);

        final JsonObject findTenantQuery = new MongoDbDocumentBuilder()
                .withTenantId(tenantId)
                .document();
        final Promise<JsonObject> didReadTenant = Promise.promise();
        mongoClient.findOne(getConfig().getCollectionName(), findTenantQuery, new JsonObject(), didReadTenant);
        return didReadTenant.future()
                .compose(successDidReadTenant -> Optional.ofNullable(successDidReadTenant)
                        .map(tenant -> tenant.mapTo(TenantDto.class))
                        .map(Future::succeededFuture)
                        .orElseGet(() -> Future.failedFuture(new ClientErrorException(HttpURLConnection.HTTP_NOT_FOUND,
                                String.format("Tenant [%s] not found.", tenantId)))));
    }

    @Override
    public Future<OperationResult<Id>> createTenant(final Optional<String> tenantId, final Tenant tenantObj,
            final Span span) {
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(tenantObj);

        return MongoDbDeviceRegistryUtils.isAllowedToModify(config, tenantId.orElse(""))
                .compose(ok -> checkConflictingCATenants(Optional.empty(),
                        tenantObj.getTrustedCertificateAuthoritySubjectDNs(), span))
                .compose(ok -> processCreateTenant(tenantId, tenantObj, span))
                .recover(error -> Future.succeededFuture(MongoDbDeviceRegistryUtils.mapErrorToResult(error, span)));
    }

    /**
     * Inserts a tenant into the device registry.
     * <p>
     * Optional: A tenant id can be provided. If none is set, a UUID will be created. A provided <em>certificate
     * authority</em> must not be set in other tenants.
     * </p>
     *
     * @param tenantId The identifier of the tenant to create.
     * @param tenantObj The configuration information to add for the tenant (may be {@code null}).
     * @param span The active OpenTracing span for this operation. It is not to be closed in this method!
     *             An implementation should log (error) events on this span and it may set tags
     *             and use this span as the parent for any spans created in this method.
     * @return A future indicating the outcome of the operation.
     *         The <em>status</em> will be
     *         <ul>
     *         <li><em>201 Created</em> if the tenant has been added successfully.</li>
     *         <li><em>409 Conflict</em> if a tenant with the given
     *         identifier or certificate authority already exists.</li>
     *         <li><em>500 Internal Server Error</em> if tenants cannot be accessed.</li>
     *         </ul>
     * @throws NullPointerException if {@code tenantId} or {@code tenantObj} are {@code null}.
     */
    private Future<OperationResult<Id>> processCreateTenant(final Optional<String> tenantId, final Tenant tenantObj,
            final Span span) {
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(tenantObj);

        final String tenantIdOrGenerated = tenantId.orElse(DeviceRegistryUtils.getUniqueIdentifier());
        final TenantDto newTenantDto = new TenantDto(tenantIdOrGenerated, tenantObj,
                new Versioned<>(tenantObj).getVersion());

        final JsonObject newTenantDtoJson = JsonObject.mapFrom(newTenantDto);
        final Promise<String> getTenantInsertion = Promise.promise();
        mongoClient.insert(getConfig().getCollectionName(), newTenantDtoJson, getTenantInsertion);
        return getTenantInsertion.future()
                .compose(successTenantInsertion -> {
                    span.log(String.format("successfully created tenant [%s]", tenantIdOrGenerated));
                    return Future.succeededFuture(OperationResult.ok(
                            HttpURLConnection.HTTP_CREATED,
                            Id.of(tenantIdOrGenerated),
                            Optional.empty(),
                            Optional.of(newTenantDto.getVersion())));
                })
                .recover(errorTenantInsertion -> {
                    if (MongoDbDeviceRegistryUtils.ifDuplicateKeyError(errorTenantInsertion)) {
                        final var errorMsg = String.format(
                                "Tenant id [%s] already exist.",
                                tenantIdOrGenerated);
                        log.debug(errorMsg);
                        TracingHelper.logError(span, errorMsg);
                        return Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_CONFLICT));
                    } else {
                        final var errorMsg = String.format("Error adding Tenant [%s].",
                                tenantIdOrGenerated);
                        log.error(errorMsg);
                        TracingHelper.logError(span, errorMsg);
                        return Future.succeededFuture(OperationResult.empty(HttpURLConnection.HTTP_INTERNAL_ERROR));
                    }
                });
    }

    /**
     * Checks if trusted certificate authority subject DN already exist in present tenants. Ignoring subject DN found
     * with id of the provided tenantDto.
     *
     * @param tenantDto the tenant dto whose id should be ignored in the search for conflicting CA subject DNs.
     * @param trustedCertificateAuthoritySubjectDNs the set of trusted certificate authority subject DNs that cannot be
     *            in present tenants, except the provided tenant {@code tenantDto}.
     * @return A future indicating the outcome of the operation.
     *         <p>
     *         The future will succeed with {@code tenantDto} if the version matches.
     *         Otherwise the future will fail with
     *         a {@link ClientErrorException} with {@link HttpURLConnection#HTTP_CONFLICT}.
     * @throws NullPointerException if {@code trustedCertificateAuthoritySubjectDNs} is {@code null}.
     */
    private Future<TenantDto> checkConflictingCATenants(final Optional<TenantDto> tenantDto,
            final Set<X500Principal> trustedCertificateAuthoritySubjectDNs, final Span span) {
        Objects.requireNonNull(trustedCertificateAuthoritySubjectDNs);

        final TenantDto ignoreTenant = tenantDto.orElse(null);
        final Future<Map.Entry<String, Versioned<Tenant>>> conflictingTenant = trustedCertificateAuthoritySubjectDNs
                .stream()
                .map(this::getByCa)
                .filter(Objects::nonNull)
                .findFirst()
                .orElseGet(() -> Future.succeededFuture(null));
        return conflictingTenant.compose(conflictingTenantFound -> {
            if (conflictingTenantFound != null) {
                // possible conflicting tenant found
                if (ignoreTenant == null || !ignoreTenant.getTenantId().equals(conflictingTenantFound.getKey())) {
                    // no current tenantDto passed or unequal to current tenantDto id
                    TracingHelper.logError(span, "Conflict : CA already used by an existing tenant.");
                    return Future.failedFuture(
                            new ClientErrorException(HttpURLConnection.HTTP_CONFLICT,
                                    "CA already used by an existing tenant."));
                }
            }
            return Future.succeededFuture(ignoreTenant);
        });

    }
}
