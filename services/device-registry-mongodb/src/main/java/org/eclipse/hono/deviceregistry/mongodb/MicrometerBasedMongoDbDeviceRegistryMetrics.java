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

import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.hono.service.metric.MicrometerBasedMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;

/**
 * Metrics for Mongo DB based Device Registry service.
 */
public class MicrometerBasedMongoDbDeviceRegistryMetrics extends MicrometerBasedMetrics
        implements MongoDbDeviceRegistryMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(MicrometerBasedMongoDbDeviceRegistryMetrics.class);

    private AtomicLong totalTenants;

    /**
     * Create a new metrics instance for the Device Registry service.
     *
     * @param vertx The Vert.x instance to use.
     * @param registry The meter registry to use.
     *
     * @throws NullPointerException if either parameter is {@code null}.
     */
    public MicrometerBasedMongoDbDeviceRegistryMetrics(final Vertx vertx, final MeterRegistry registry) {
        super(registry, vertx);
    }

    @Override
    public void setInitialTenantsCount(final long tenantsCount) {
        this.totalTenants = new AtomicLong(tenantsCount);
        Gauge.builder(TOTAL_TENANTS_METRIC_KEY, () -> totalTenants).strongReference(true).register(registry);
    }

    @Override
    public void incrementTotalTenants() {
        if (totalTenants == null) { 
            LOG.warn("Attempt to increment '{}' metric, but Mongo DB is not ready. Skipping...", TOTAL_TENANTS_METRIC_KEY);
            return;
        }
        totalTenants.incrementAndGet();
    }

    @Override
    public void decrementTenants() {
        if (totalTenants == null) {
            LOG.warn("Attempt to decrement '{}' metric, but Mongo DB is not ready. Skipping...", TOTAL_TENANTS_METRIC_KEY);
            return;
        }
        totalTenants.decrementAndGet();
    }
}
