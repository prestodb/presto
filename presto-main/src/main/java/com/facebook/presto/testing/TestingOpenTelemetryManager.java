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
import com.facebook.presto.opentelemetry.OpenTelemetryImpl;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.FileAssert.fail;

public class TestingOpenTelemetryManager
        extends TelemetryManager
{
    private OpenTelemetry openTelemetry = OpenTelemetry.noop();
    private Tracer tracer = openTelemetry.getTracer("no-op");
    private TelemetryManager openTelemetryManager;
    private OpenTelemetryImpl openTelemetryFactory;
    Map<String, String> properties = new HashMap<>();

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

        tracer = openTelemetry.getTracer("sdk in mem tracer");

        if (TelemetryConfig.getTracingEnabled()) {
            TelemetryManager.setTracer(tracer);
        }
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return this.openTelemetry;
    }

    public List<SpanData> getFinishedSpanItems()
    {
        return inMemorySpanExporter.getFinishedSpanItems();
    }

    public void clearSpanList()
    {
        inMemorySpanExporter.reset();
    }

    @BeforeMethod
    public void setUp() throws Exception
    {
        properties.put("otel-factory.name", "otel");
        properties.put("span-exporter.name", "otlpgrpc");
        properties.put("span-processor.name", "batch");
        properties.put("tracing-enabled", "true");
        properties.put("exporter-endpoint", "http://localhost:4317");
        properties.put("max-exporter-batch-size", "256");
        properties.put("max-queue-size", "1024");
        properties.put("exporter-timeout", "5000");
        properties.put("schedule-delay", "1000");
        properties.put("trace-sampling-ratio", "1.0");
        properties.put("span-sampling", "true");

        openTelemetryManager = new TelemetryManager();

        openTelemetryFactory = new OpenTelemetryImpl();
        openTelemetryManager.clearFactories();
        openTelemetryManager.addOpenTelemetryFactory(openTelemetryFactory);

        resetTelemetryConfigSingleton(true);
    }

    @AfterMethod
    public void tearDown() throws Exception
    {
        openTelemetryManager.clearFactories();
        resetTelemetryConfigSingleton(true);
    }

    private void resetTelemetryConfigSingleton(Boolean value) throws Exception
    {
        java.lang.reflect.Field instanceField = TelemetryConfig.class.getDeclaredField("telemetryConfig");
        instanceField.setAccessible(value);
        instanceField.set(null, null);
    }

    @Test
    public void testAddOpenTelemetryFactory()
    {
        try {
            openTelemetryManager.addOpenTelemetryFactory(openTelemetryFactory);
            fail("Expected IllegalArgumentException due to duplicate factory");
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "openTelemetry factory 'otel' is already registered");
        }
    }

    @Test
    public void testLoadConfiguredOpenTelemetry() throws Exception
    {
        TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);

        openTelemetryManager.loadConfiguredOpenTelemetry();

        assertNotNull(openTelemetryManager.getOpenTelemetry());
        assertNotNull(openTelemetryManager.getTracer());

        Tracer tracer = openTelemetryManager.getTracer();
        assertNotNull(tracer);
        assertNotEquals(tracer, OpenTelemetry.noop().getTracer("no-op"));
    }

    @Test
    public void testSetTracerWhenTracingEnabled() throws Exception
    {
        TelemetryConfig.getTelemetryConfig().setTracingEnabled(true);

        openTelemetryManager.loadConfiguredOpenTelemetry();
        openTelemetryManager.createTracer();

        Tracer tracer = openTelemetryManager.getTracer();
        assertNotNull(tracer);
        assertNotEquals(tracer, OpenTelemetry.noop().getTracer("no-op"));
    }

    @Test
    public void testSetTracerWhenTracingDisabled()
    {
        TelemetryConfig.getTelemetryConfig().setTracingEnabled(false);

        openTelemetryManager.createTracer();

        Tracer tracer = openTelemetryManager.getTracer();
        assertEquals(tracer, OpenTelemetry.noop().getTracer("no-op"));
    }

    @Test
    public void testLoadConfiguredOpenTelemetry_WithRealFactory() throws Exception
    {
        TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);

        openTelemetryManager.loadConfiguredOpenTelemetry();
        assertNotNull(openTelemetryManager.getOpenTelemetry());

        assertNotNull(openTelemetryManager.getTracer());
        assertNotEquals(openTelemetryManager.getTracer(), OpenTelemetry.noop().getTracer("no-op"));
    }
}
