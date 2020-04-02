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

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.californium.core.coap.Option;
import org.eclipse.californium.core.coap.OptionSet;
import org.eclipse.hono.adapter.coap.CoapOptionW3CAdapter.TraceContextOption;
import org.junit.jupiter.api.Test;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.CodecConfiguration;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;


/**
 * A CoapOptionW3CAdapterTest.
 *
 */
public class CoapOptionW3CAdapterTest {

    /**
     * Verifies that the Jaeger tracer implementation can successfully use the
     * adapter to extract a SpanContext using the <em>W3C</em> format.
     */
    @Test
    public void testJaegerTracerCanExtractAndInjectW3CTraceContext() {

        final Configuration config = new Configuration("test").withCodec(CodecConfiguration.fromString("w3c"));
        final Tracer tracer = config.getTracer();

        final String traceparent = "00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        OptionSet optionSet = new OptionSet();
        optionSet.addOption(new Option(TraceContextOption.TRACEPARENT.getNumber(), traceparent));

        final SpanContext context = tracer.extract(Format.Builtin.TEXT_MAP, new CoapOptionW3CAdapter(optionSet));
        assertThat(context.toTraceId()).isEqualTo("1af7651916cd43dd8448eb211c80319c");
        assertThat(context.toSpanId()).isEqualTo("b7ad6b7169203331");

        optionSet = new OptionSet();
        tracer.inject(context, Format.Builtin.TEXT_MAP, new CoapOptionW3CAdapter(optionSet));
        final Option injectedTraceParent = optionSet.getOthers().stream()
                .filter(option -> option.getNumber() == TraceContextOption.TRACEPARENT.getNumber())
                .findFirst()
                .get();
        assertThat(injectedTraceParent.getStringValue()).isEqualTo(traceparent);
    }

}
