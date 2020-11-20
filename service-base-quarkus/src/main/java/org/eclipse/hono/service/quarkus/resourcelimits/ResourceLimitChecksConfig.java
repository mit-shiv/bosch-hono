/**
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
 */


package org.eclipse.hono.service.quarkus.resourcelimits;

import org.eclipse.hono.service.resourcelimits.PrometheusBasedResourceLimitChecksConfig;

import io.quarkus.arc.config.ConfigProperties;
import io.quarkus.arc.config.ConfigProperties.NamingStrategy;

/**
 * Resource Limit Checks configuration.
 */
@ConfigProperties(prefix = "hono.resourceLimits.prometheusBased", namingStrategy = NamingStrategy.VERBATIM, failOnMismatchingMember = false)
public class ResourceLimitChecksConfig extends PrometheusBasedResourceLimitChecksConfig {
}
