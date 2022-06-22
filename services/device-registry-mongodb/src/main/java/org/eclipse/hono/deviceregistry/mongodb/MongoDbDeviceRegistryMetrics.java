/**
 * Copyright (c) 2018, 2021 Contributors to the Eclipse Foundation
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
package org.eclipse.hono.deviceregistry.mongodb;

import org.eclipse.hono.service.DeviceRegistryMetrics;
import org.eclipse.hono.service.metric.NoopBasedMetrics;

/**
 * Metrics for the MongoDB based Device Registry service.
 */
public interface MongoDbDeviceRegistryMetrics extends DeviceRegistryMetrics {

    /**
     * A no-op implementation for this specific metrics type.
     */
    final class Noop extends NoopBasedMetrics implements MongoDbDeviceRegistryMetrics {

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
    MongoDbDeviceRegistryMetrics NOOP = new Noop();
}
