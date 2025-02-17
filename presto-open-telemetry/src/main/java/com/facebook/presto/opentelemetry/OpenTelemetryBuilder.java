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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import java.util.concurrent.TimeUnit;

public final class OpenTelemetryBuilder
{
    private static final Logger log = Logger.get(OpenTelemetryBuilder.class);

    private OpenTelemetryBuilder()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
    }

    public static OpenTelemetry build()
    {
        TelemetryConfig telemetryConfig = TelemetryConfig.getTelemetryConfig();
        OpenTelemetry openTelemetry = OpenTelemetry.noop(); //default instance for tracing disabled case

        if (TelemetryConfig.getTracingEnabled()) {
            log.debug("telemetry tracing is enabled");
            Resource resource = Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "Presto"));

            SpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                    .setEndpoint(telemetryConfig.getTracingBackendUrl())
                    .setTimeout(10, TimeUnit.SECONDS)
                    .build();
            log.debug("telemetry span exporter configured");

            SpanProcessor spanProcessor = BatchSpanProcessor.builder(spanExporter)
                    .setMaxExportBatchSize(telemetryConfig.getMaxExporterBatchSize())
                    .setMaxQueueSize(telemetryConfig.getMaxQueueSize())
                    .setScheduleDelay(telemetryConfig.getScheduleDelay(), TimeUnit.MILLISECONDS)
                    .setExporterTimeout(telemetryConfig.getExporterTimeout(), TimeUnit.MILLISECONDS)
                    .build();
            log.debug("telemetry span processor configured");

            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .setSampler(Sampler.traceIdRatioBased(telemetryConfig.getSamplingRatio()))
                    .addSpanProcessor(spanProcessor)
                    .setResource(resource)
                    .build();
            log.debug("telemetry tracer provider set");

            openTelemetry = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setPropagators(ContextPropagators.create(
                            TextMapPropagator.composite(W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
                    .build();
            log.debug("opentelemetry instance created");
        }
        else {
            log.debug("telemetry tracing is disabled");
        }

        return openTelemetry;
    }
}
