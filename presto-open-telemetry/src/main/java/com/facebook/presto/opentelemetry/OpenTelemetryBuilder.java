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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

public final class OpenTelemetryBuilder
{
    private OpenTelemetryBuilder()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
    }

    /**
     * Get instance of propagator.
     * Currently, only B3_SINGLE_HEADER can be passed in.
     */
    private static TextMapPropagator getPropagatorInstance(String contextPropagator)
    {
        TextMapPropagator propagator;
        if (contextPropagator.equals(OpenTelemetryContextPropagator.W3C)) {
            propagator = W3CTraceContextPropagator.getInstance();
        }
        else if (contextPropagator.equals(OpenTelemetryContextPropagator.B3_SINGLE_HEADER)) {
            propagator = B3Propagator.injectingSingleHeader();
        }
        else {
            propagator = B3Propagator.injectingMultiHeaders();
        }
        return propagator;
    }

    public static OpenTelemetry build(String contextPropagator)
    {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "presto")));

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(OtlpGrpcSpanExporter.builder().setEndpoint(System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")).build()).build())
                .setResource(resource)
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(OpenTelemetryBuilder.getPropagatorInstance(contextPropagator)))
                .buildAndRegisterGlobal();
    }
}
