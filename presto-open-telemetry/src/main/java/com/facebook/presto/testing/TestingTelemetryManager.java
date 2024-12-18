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
package com.facebook.presto.testing;

import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.telemetry.TelemetryManager;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.util.List;

public class TestingTelemetryManager
        extends TelemetryManager
{
    private static OpenTelemetry openTelemetry = OpenTelemetry.noop();

    private static Tracer tracer = openTelemetry.getTracer("no-op");

    private static InMemorySpanExporter inMemorySpanExporter;

    public void createInstances()
    {
        inMemorySpanExporter = InMemorySpanExporter.create();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(inMemorySpanExporter)
                        .build())
                .setResource(Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "Presto")))
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(
                        TextMapPropagator.composite(W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
                .build();
        TelemetryManager.setOpenTelemetry(openTelemetry);

        if (TelemetryConfig.getTracingEnabled()) {
            tracer = openTelemetry.getTracer("sdk in mem tracer");
            TelemetryManager.setTracer(tracer);
        }
    }

    public List<SpanData> getFinishedSpanItems()
    {
        return inMemorySpanExporter.getFinishedSpanItems();
    }

    public boolean isSpansEmpty()
    {
        return getFinishedSpanItems().isEmpty();
    }

    public boolean spansAnyMatch(String spanName)
    {
        return getFinishedSpanItems().stream().anyMatch(sn -> spanName.equals(sn.getName()));
    }

    public void clearSpanList()
    {
        inMemorySpanExporter.reset();
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return openTelemetry;
    }

    public static Tracer getTracer()
    {
        return tracer;
    }

    public static void setTracer(Tracer tracer)
    {
        TestingTelemetryManager.tracer = tracer;
    }
}
