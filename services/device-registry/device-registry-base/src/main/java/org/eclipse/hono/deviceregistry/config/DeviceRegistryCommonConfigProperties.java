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
 * A POJO for configuring common properties for device registries.
 *
 */
public class DeviceRegistryCommonConfigProperties {

    /**
     * The default number of seconds that information returned by the service's
     * operations may be cached for.
     */
    public static final int DEFAULT_MAX_AGE_SECONDS = 180;
    private int cacheMaxAge = DEFAULT_MAX_AGE_SECONDS;
    private boolean modificationEnabled = true;

    /**
     * Sets the maximum period of time that information returned by the service's
     * operations may be cached for.
     * <p>
     * The default value of this property is {@link #DEFAULT_MAX_AGE_SECONDS} seconds.
     * 
     * @param maxAge The period of time in seconds.
     * @throws IllegalArgumentException if max age is &lt; 0.
     */
    public final void setCacheMaxAge(final int maxAge) {
        if (maxAge < 0) {
            throw new IllegalArgumentException("max age must be >= 0");
        }
        this.cacheMaxAge = maxAge;
    }

    /**
     * Gets the maximum period of time that information returned by the service's
     * operations may be cached for.
     * <p>
     * The default value of this property is {@link #DEFAULT_MAX_AGE_SECONDS} seconds.
     * 
     * @return The period of time in seconds.
     */
    public final int getCacheMaxAge() {
        return cacheMaxAge;
    }

    /**
     * Checks whether this registry allows the creation, modification and removal of entries.
     * <p>
     * If set to {@code false} then methods for creating, updating or deleting an entry should return a <em>403 Forbidden</em> response.
     * <p>
     * The default value of this property is {@code true}.
     *
     * @return The flag.
     */
    public final boolean isModificationEnabled() {
        return modificationEnabled;
    }

    /**
     * Sets whether this registry allows creation, modification and removal of entries.
     * <p>
     * If set to {@code false} then for creating, updating or deleting an entry should return a <em>403 Forbidden</em> response.
     * <p>
     * The default value of this property is {@code true}.
     *
     * @param flag The flag.
     */
    public final void setModificationEnabled(final boolean flag) {
        modificationEnabled = flag;
    }
}
