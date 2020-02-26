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

package org.eclipse.hono.deviceregistry.file;

/**
 * Configuration properties for Hono's registration API as own server.
 *
 */
public final class FileBasedRegistrationConfigProperties extends AbstractFileBasedRegistryConfigProperties {

    private static final String DEFAULT_DEVICES_FILENAME = "/var/lib/hono/device-registry/device-identities.json";

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getDefaultFileName() {
        return DEFAULT_DEVICES_FILENAME;
    }
}
