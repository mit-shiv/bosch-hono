/**
 * Copyright (c) 2018 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 1.0 which is available at
 * https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 */

package org.eclipse.hono.tracing;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;

import io.opentracing.propagation.TextMap;
import io.vertx.core.json.JsonObject;


/**
 * An adapter for injecting an Opentracing {@code SpanContext} into
 * a JSON object.
 *
 */
public class JsonObjectInjectAdapter implements TextMap {

    private final JsonObject spanContext;

    /**
     * Creates a new adapter for a JSON object and property name.
     * 
     * @param json The container JSON object.
     * @param propertyName The name of the JSON property which contains
     *              the span context.
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    public JsonObjectInjectAdapter(final JsonObject json, final String propertyName) {
        Objects.requireNonNull(json);
        Objects.requireNonNull(propertyName);
        this.spanContext = new JsonObject();
        json.put(propertyName, spanContext);
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * 
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    @Override
    public void put(final String key, final String value) {
        spanContext.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
    }

}
