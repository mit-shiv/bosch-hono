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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import io.opentracing.propagation.TextMap;
import io.vertx.core.json.JsonObject;


/**
 * An adapter for extracting an Opentracing {@code SpanContext} from
 * a JSON object.
 *
 */
public class JsonObjectExtractAdapter implements TextMap {

    private final Map<String, String> spanContext;

    /**
     * Creates a new adapter for a JSON object and property name.
     * 
     * @param json The container JSON object.
     * @param propertyName The name of the JSON property which contains
     *              the span context.
     * @throws NullPointerException if any of the parameters are {@code null}.
     * @throws IllegalArgumentException if the JSON object does not contain
     *              another JSON object under the given property name.
     */
    public JsonObjectExtractAdapter(final JsonObject json, final String propertyName) {
        Objects.requireNonNull(json);
        Objects.requireNonNull(propertyName);
        final Object obj = json.getValue(propertyName);
        if (obj instanceof JsonObject) {
            this.spanContext = ((JsonObject) obj).stream().filter(entry -> entry.getValue() instanceof String)
                    .collect(Collectors.toMap(
                            entry -> entry.getKey(),
                            entry -> (String) entry.getValue(),
                            (a, b) -> a,
                            HashMap::new));
        } else {
            throw new IllegalArgumentException("JSON does not contain property of given name");
        }
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return spanContext.entrySet().iterator();
    }

    @Override
    public void put(final String key, final String value) {
        throw new UnsupportedOperationException();
    }

}
