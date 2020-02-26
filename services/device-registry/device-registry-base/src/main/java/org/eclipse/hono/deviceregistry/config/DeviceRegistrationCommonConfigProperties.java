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
package org.eclipse.hono.deviceregistry.config;

/**
 * A POJO for configuring common properties for device registration.
 *
 */
public class DeviceRegistrationCommonConfigProperties extends DeviceRegistryCommonConfigProperties{

    /**
     * The default number of devices that can be registered for each tenant.
     */
    public static final int DEFAULT_MAX_DEVICES_PER_TENANT = 100;
    private int maxDevicesPerTenant = DEFAULT_MAX_DEVICES_PER_TENANT;

    /**
     * Gets the maximum number of devices that can be registered for each tenant.
     * <p>
     * The default value of this property is {@link #DEFAULT_MAX_DEVICES_PER_TENANT}.
     * 
     * @return The maximum number of devices.
     */
    public int getMaxDevicesPerTenant() {
        return maxDevicesPerTenant;
    }

    /**
     * Sets the maximum number of devices that can be registered for each tenant.
     * <p>
     * The default value of this property is {@link #DEFAULT_MAX_DEVICES_PER_TENANT}.
     * 
     * @param maxDevices The maximum number of devices.
     * @throws IllegalArgumentException if the number of devices is &lt;= 0.
     */
    public void setMaxDevicesPerTenant(final int maxDevices) {
        if (maxDevices <= 0) {
            throw new IllegalArgumentException("max devices must be > 0");
        }
        this.maxDevicesPerTenant = maxDevices;
    }
}
