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

package org.eclipse.hono.deviceconnection.infinispan.client;

import java.util.Optional;
import java.util.UUID;

import org.eclipse.hono.client.BasicDeviceConnectionClientFactory;
import org.eclipse.hono.util.DeviceConnectionConstants;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.vertx.core.Vertx;

/**
 * Spring Boot configuration class defining beans for accessing a Hotrod based (remote) cache.
 */
@Configuration
public class HotrodCacheConfig {

    /**
     * Gets properties for configuring the connection to the Infinispan
     * data grid that contains device connection information.
     * 
     * @return The properties.
     */
    @Bean
    @ConfigurationProperties(prefix = "hono.device-connection")
    @ConditionalOnProperty(prefix = "hono.device-connection", name = "server-list")
    public InfinispanRemoteConfigurationProperties remoteCacheProperties() {
        return new InfinispanRemoteConfigurationProperties();
    }

    /**
     * Exposes the Infinispan data grid that contains device connection information
     * as a remote cache manager.
     * 
     * @return The newly created cache manager. The manager will not be started.
     */
    @Bean
    @ConditionalOnProperty(prefix = "hono.device-connection", name = "server-list")
    public RemoteCacheManager remoteCacheManager() {
        final InfinispanRemoteConfigurationProperties properties = remoteCacheProperties();
        return new RemoteCacheManager(properties.getConfigurationBuilder().build(), false);
    }

    /**
     * Exposes a remote cache for accessing the Infinispan data grid that contains device
     * connection information.
     *
     * @param vertx The vert.x instance to run on.
     * @return The cache.
     */
    @Bean
    @ConditionalOnProperty(prefix = "hono.device-connection", name = "server-list")
    public HotrodCache<String, String> remoteCache(final Vertx vertx) {
        return new HotrodCache<>(
                vertx,
                remoteCacheManager(),
                DeviceConnectionConstants.CACHE_NAME,
                UUID.randomUUID().toString(),
                "VALUE_CHECK_CONNECTION");
    }

    /**
     * Exposes a factory for creating clients for accessing device connection information
     * in an Infinispan data grid.
     *
     * @param cache The remote cache in the Infinispan data grid.
     * @param tracer The OpenTracing {@code Tracer} to use for tracking requests done by clients created by this factory.
     *               If an empty Optional is given, the {@code NoopTracer} from OpenTracing will be used.
     * @return The factory.
     */
    @Bean
    @Qualifier(DeviceConnectionConstants.DEVICE_CONNECTION_ENDPOINT)
    @ConditionalOnProperty(prefix = "hono.device-connection", name = "server-list")
    public BasicDeviceConnectionClientFactory hotrodBasedDeviceConnectionClientFactory(
            final HotrodCache<String, String> cache, final Optional<Tracer> tracer) {
        return new HotrodBasedDeviceConnectionClientFactory(cache, tracer.orElse(NoopTracerFactory.create()));
    }
}
