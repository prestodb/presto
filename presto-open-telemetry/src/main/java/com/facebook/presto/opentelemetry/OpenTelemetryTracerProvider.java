/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.opentelemetry;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.tracing.Tracer;
import com.facebook.presto.spi.tracing.TracerHandle;
import com.facebook.presto.spi.tracing.TracerProvider;
import com.google.inject.Inject;

import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.opentelemetry.OpenTelemetryErrorCode.OPEN_TELEMETRY_CONTEXT_PROPAGATOR_ERROR;
import static com.facebook.presto.opentelemetry.OpenTelemetryHeaders.PRESTO_B3_SINGLE_HEADER_PROPAGATION;
import static com.facebook.presto.opentelemetry.OpenTelemetryHeaders.PRESTO_BAGGAGE_HEADER;
import static com.facebook.presto.opentelemetry.OpenTelemetryHeaders.PRESTO_TRACE_TOKEN;
import static com.facebook.presto.opentelemetry.OpenTelemetryHeaders.PRESTO_W3C_PROPAGATION;

public class OpenTelemetryTracerProvider
        implements TracerProvider
{
    @Inject
    public OpenTelemetryTracerProvider() {}

    @Override
    public String getName()
    {
        return "Open telemetry tracer provider";
    }

    @Override
    public String getTracerType()
    {
        return "otel";
    }

    @Override
    public Function<Map<String, String>, TracerHandle> getHandleGenerator()
    {
        return headers -> {
            String contextPropagator = determineContextPropagationMode(headers);
            return new OpenTelemetryTracerHandle(
                    headers.get(PRESTO_TRACE_TOKEN),
                    contextPropagator,
                    getPropagatedContextFromHeader(contextPropagator, headers),
                    headers.get(PRESTO_BAGGAGE_HEADER));
        };
    }

    @Override
    public Tracer getNewTracer(TracerHandle handle)
    {
        OpenTelemetryTracerHandle tracerHandle = (OpenTelemetryTracerHandle) handle;
        return new OpenTelemetryTracer(
                tracerHandle.getTraceToken(),
                tracerHandle.getContextPropagator(),
                tracerHandle.getPropagatedContext(),
                tracerHandle.getBaggage());
    }

    /**
     * Take header values and determine which context propagation mode is used
     * Currently only supports b3 single header
     * @param headers HTTP request headers
     * @return context propagation mode
     */
    private String determineContextPropagationMode(Map<String, String> headers)
    {
        if (headers.containsKey(PRESTO_B3_SINGLE_HEADER_PROPAGATION)) {
            return OpenTelemetryContextPropagator.B3_SINGLE_HEADER;
        }
        if (headers.containsKey(PRESTO_W3C_PROPAGATION)) {
            throw new PrestoException(
                    OPEN_TELEMETRY_CONTEXT_PROPAGATOR_ERROR,
                    "Only b3 single header context propagation mode is currently supported.");
        }
        return null;
    }

    /**
     * Currently only supports b3 single header and w3c propagation
     * @param contextPropagator context propagator to use
     * @param headers http request headers
     * @return header value extracted from http request based on context propagator
     */
    private static String getPropagatedContextFromHeader(String contextPropagator, Map<String, String> headers)
    {
        if (contextPropagator != null) {
            if (contextPropagator.equals(OpenTelemetryContextPropagator.B3_SINGLE_HEADER)) {
                return headers.get(PRESTO_B3_SINGLE_HEADER_PROPAGATION);
            }
            if (contextPropagator.equals(OpenTelemetryContextPropagator.W3C)) {
                return headers.get(PRESTO_W3C_PROPAGATION);
            }
        }
        return null;
    }
}
