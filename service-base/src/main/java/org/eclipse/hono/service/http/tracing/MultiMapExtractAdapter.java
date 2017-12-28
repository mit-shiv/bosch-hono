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
package org.eclipse.hono.service.http.tracing;

import io.opentracing.propagation.TextMap;
import io.vertx.core.MultiMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Pavol Loffay
 */
public class MultiMapExtractAdapter implements TextMap {

    private MultiMap headers;

    /**
     * @param headers The headers to extract the SpanContext from.
     */
    public MultiMapExtractAdapter(final MultiMap headers) {
        this.headers = headers;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return headers.entries().iterator();
    }

    @Override
    public void put(final String key, final String value) {
        throw new UnsupportedOperationException();
    }
}