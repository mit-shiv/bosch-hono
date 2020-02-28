/*******************************************************************************
 * Copyright (c) 2016, 2020 Contributors to the Eclipse Foundation
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

package org.eclipse.hono.deviceregistry.file;

import org.eclipse.hono.deviceregistry.config.DeviceRegistrationCommonConfigProperties;

/**
 * Common configuration properties for file based implementations of the APIs of Hono's device registry as own server.
 * <p>
 * This class is intended to be used as base class for property classes that configure a specific file based API implementation.
 */
public abstract class AbstractFileBasedRegistryConfigProperties extends DeviceRegistrationCommonConfigProperties {

    private String filename = getDefaultFileName();
    private boolean saveToFile = false;
    private boolean startEmpty = false;

    /**
     * Gets the path to the file that the registry should be persisted to periodically.
     * <p>
     *
     * @return The path to the file.
     */
    protected abstract String getDefaultFileName();

    /**
     * Checks whether the content of the registry should be persisted to the file system
     * periodically.
     * <p>
     * Default value is {@code false}.
     *
     * @return {@code true} if registry content should be persisted.
     */
    public final boolean isSaveToFile() {
        return saveToFile;
    }

    /**
     * Sets whether the content of the registry should be persisted to the file system
     * periodically.
     * <p>
     * Default value is {@code false}.
     *
     * @param enabled {@code true} if registry content should be persisted.
     * @throws IllegalStateException if this registry is already running.
     */
    public final void setSaveToFile(final boolean enabled) {
        this.saveToFile = enabled;
    }

    /**
     * Gets the path to the file that the registry should be persisted to
     * periodically.
     * <p>
     *
     * @return The file name.
     */
    public final String getFilename() {
        return filename;
    }

    /**
     * Sets the path to the file that the registry should be persisted to
     * periodically.
     * <p>
     *
     * @param filename The name of the file to persist to (can be a relative or absolute path).
     */
    public final void setFilename(final String filename) {
        this.filename = filename;
    }

    /**
     * Checks whether this registry should ignore the provided JSON file when starting up.
     * <p>
     * If set to {@code true} then the starting process will result in an empty registry, even if populated JSON file are provided.
     * <p>
     * The default value of this property is {@code false}.
     *
     * @return The flag.
     */
    public final boolean isStartEmpty() {
        return startEmpty;
    }

    /**
     * Sets whether this registry should ignore the provided JSON file when starting up.
     * <p>
     * Default value is {@code false}.
     *
     * @param flag {@code true} if registry content should be persisted.
     */
    public final void setStartEmpty(final boolean flag) {
        this.startEmpty = flag;
    }
}
