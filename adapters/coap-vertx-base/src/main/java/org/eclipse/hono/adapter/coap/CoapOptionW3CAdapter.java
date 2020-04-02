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


package org.eclipse.hono.adapter.coap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import org.eclipse.californium.core.coap.Option;
import org.eclipse.californium.core.coap.OptionSet;

import io.opentracing.propagation.TextMap;


/**
 * A CoapOptionW3CAdapter.
 *
 */
public class CoapOptionW3CAdapter implements TextMap {

    /**
     * The CoAP option holding the W3C Trace Context <em>traceparent</em> value.
     * <p>
     * Note that the option number is from the <em>experimental</em> range to reflect its
     * <em>for internal use only</em> character. As such, the CoAP adapter's
     * capability to extract a trace context from this option remains undocumented.
     * <p>
     * The option is elective (bit 0 = 0), safe-to-forward (bit 1 = 1) and
     * must not be used as a cache-key (bits 2-4 = 1).
     * 
     * @see <a href="https://tools.ietf.org/html/rfc7252#section-5.4.6">RFC 7252, Option Numbers</a>
     */
    public static final int OPTION_TRACEPARENT = 0b1111111000011110; // 65054
    /**
     * The CoAP option holding the W3C Trace Context <em>tracestate</em> value.
     * <p>
     * Note that the option number is from the <em>experimental</em> range to reflect its
     * <em>for internal use only</em> character. As such, the CoAP adapter's
     * capability to extract a trace context from this option remains undocumented.
     * <p>
     * The option is elective (bit 0 = 0), safe-to-forward (bit 1 = 1) and
     * must not be used as a cache-key (bits 2-4 = 1).
     * 
     * @see <a href="https://tools.ietf.org/html/rfc7252#section-5.4.6">RFC 7252, Option Numbers</a>
     */
    public static final int OPTION_TRACESTATE = 0b1111111000111110; // 65086

    enum TraceContextOption {

        TRACEPARENT("traceparent", OPTION_TRACEPARENT),
        TRACESTATE("tracestate", OPTION_TRACESTATE),
        UNKNOWN("unknown", -1);

        private int number;
        private String name;

        TraceContextOption(final String name, final int number) {
            this.name = name;
            this.number = number;
        }

        static TraceContextOption from(final String name) {
            if (TRACEPARENT.name.equals(name)) {
                return TRACEPARENT;
            } else if (TRACESTATE.name.equals(name)) {
                return TRACESTATE;
            } else {
                return UNKNOWN;
            }
        }

        static TraceContextOption from(final int number) {
            if (TRACEPARENT.number == number) {
                return TRACEPARENT;
            } else if (TRACESTATE.number == number) {
                return TRACESTATE;
            } else {
                return null;
            }
        }

        int getNumber() {
            return number;
        }
    }

    private final Map<String, String> entries = new HashMap<>(2);
    private final OptionSet options;

    /**
     * Creates a new adapter for CoAP options.
     * 
     * @param options The CoAP options to inject/extract the context to/from.
     * @throws NullPointerException if options is {@code null}.
     */
    public CoapOptionW3CAdapter(final OptionSet options) {
        this.options = Objects.requireNonNull(options);
        extractTraceContext();
    }

    private void extractTraceContext() {
        options.getOthers().stream()
            .forEach(option -> {
                Optional.ofNullable(TraceContextOption.from(option.getNumber()))
                    .ifPresent(o -> entries.put(o.name, option.getStringValue()));
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final String key, final String value) {
        switch (TraceContextOption.from(key)) {
        case TRACEPARENT:
            options.addOption(new Option(TraceContextOption.TRACEPARENT.number, value));
            break;
        case TRACESTATE:
            options.addOption(new Option(TraceContextOption.TRACESTATE.number, value));
            break;
        default:
            // ignore
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Entry<String, String>> iterator() {
        return entries.entrySet().iterator();
    }
}
