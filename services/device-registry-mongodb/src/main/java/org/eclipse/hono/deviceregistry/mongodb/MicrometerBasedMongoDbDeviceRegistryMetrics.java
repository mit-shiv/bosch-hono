/*******************************************************************************
 * Copyright (c) 2022 Contributors to the Eclipse Foundation
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.hono.service.DeviceRegistryMetrics;
import org.eclipse.hono.service.metric.MicrometerBasedMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

/**
 * Metrics for Mongo DB based Device Registry service.
 */
public class MicrometerBasedMongoDbDeviceRegistryMetrics extends MicrometerBasedMetrics
        implements MongoDbDeviceRegistryMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(MicrometerBasedMongoDbDeviceRegistryMetrics.class);

    private final MongoClient mongoClient;
    private final String collectionName;

    /**
     * Create a new metrics instance for the Device Registry service.
     *
     * @param vertx The Vert.x instance to use.
     * @param registry The meter registry to use.
     * @param mongoClient The client to use for accessing the Mongo DB.
     * @param collectionName Tenants collection name in Mongo DB.
     *
     * @throws NullPointerException if either parameter is {@code null}.
     */
    public MicrometerBasedMongoDbDeviceRegistryMetrics(final Vertx vertx, final MeterRegistry registry,
            final MongoClient mongoClient, final String collectionName) {
        super(registry, vertx);
        this.mongoClient = mongoClient;
        this.collectionName = collectionName;
    }

    @Override
    public void registerInitialTenantsCount() {
        register();
    }

    private void register() {
        Gauge.builder(TOTAL_TENANTS_METRIC_KEY, () -> {
            final Future<Long> query = mongoClient.count(collectionName, new JsonObject());
            try {
                final long totalTenantsCount = query.toCompletionStage().toCompletableFuture().get();
                return new AtomicLong(totalTenantsCount);
            } catch (final InterruptedException | ExecutionException e) {
                LOG.warn("Error while querying '{}' metric from MongoDB", DeviceRegistryMetrics.TOTAL_TENANTS_METRIC_KEY, e);
                return -1;
            }
        }).register(registry);
    }
}
