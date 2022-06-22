/**
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
 */

package org.eclipse.hono.service;

import org.eclipse.hono.service.metric.Metrics;
import org.eclipse.hono.service.metric.NoopBasedMetrics;

/**
 * Metrics for the Device Registry service.
 */
public interface DeviceRegistryMetrics extends Metrics {

    /**
     * Metric key for total number of Tenants in the system.
     */
    String TOTAL_TENANTS_METRIC_KEY = "total.tenants";

    /**
     * A no-op implementation for this specific metrics type.
     */
    final class Noop extends NoopBasedMetrics implements DeviceRegistryMetrics {

        private Noop() {
        }

        @Override
        public void incrementTotalTenants() {
            // Do nothing, used for tests only.
        }

        @Override
        public void decrementTenants() {
            // Do nothing, used for tests only.
        }

        @Override
        public void setInitialTenantsCount(final long tenantsCount) {
            // Do nothing, used for tests only.            
        }
    }

    /**
     * The no-op implementation.
     */
    DeviceRegistryMetrics NOOP = new Noop();

    /**
     * Increment metric when a new Tenant is created.
     */
    void incrementTotalTenants();

    /**
     * Decrement metric when a Tenant is deleted.
     */
    void decrementTenants();

    /**
     * Set the initial tenants count. Will be called on startup
     * @param tenantsCount tenants count
     */
    void setInitialTenantsCount(long tenantsCount);
}
