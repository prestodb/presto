
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
package com.facebook.presto.telemetry;

import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.opentelemetry.OpenTelemetryImpl;
import com.facebook.presto.testing.TestingOpenTelemetryTracingManager;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class TestOpenTelemetryTracingManager
{
    private TestingOpenTelemetryTracingManager telemetryManager;
    private OpenTelemetryImpl openTelemetryFactory;
    Map<String, String> properties = new HashMap<>();

    @BeforeMethod
    public void setUp() throws Exception
    {
        properties.put("tracing-factory.name", "otel");
        properties.put("tracing-enabled", "true");
        properties.put("tracing-backend-url", "http://localhost:4317");
        properties.put("max-exporter-batch-size", "256");
        properties.put("max-queue-size", "1024");
        properties.put("exporter-timeout", "5000");
        properties.put("schedule-delay", "1000");
        properties.put("trace-sampling-ratio", "1.0");
        properties.put("span-sampling", "true");

        telemetryManager = new TestingOpenTelemetryTracingManager();

        openTelemetryFactory = new OpenTelemetryImpl();
        telemetryManager.clearFactories();
        telemetryManager.addOpenTelemetryFactory(openTelemetryFactory);

        resetTelemetryConfigSingleton(true);
    }

    @AfterMethod
    public void tearDown() throws Exception
    {
        telemetryManager.clearFactories();
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
            telemetryManager.addOpenTelemetryFactory(openTelemetryFactory);
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

        telemetryManager.createInstances();

        assertNotNull(telemetryManager.getOpenTelemetry());

        Tracer tracer = telemetryManager.getTracer();
        assertNotNull(tracer);
        assertNotEquals(tracer, OpenTelemetry.noop().getTracer("no-op"));
    }

    @Test
    public void testSetTracerWhenTracingEnabled() throws Exception
    {
        TelemetryConfig.getTelemetryConfig().setTracingEnabled(true);

        telemetryManager.createInstances();

        Tracer tracer = telemetryManager.getTracer();
        assertNotNull(tracer);
        assertNotEquals(tracer, OpenTelemetry.noop().getTracer("no-op"));
    }

    @Test
    public void testSetTracerWhenTracingDisabled()
    {
        telemetryManager.setTracer(OpenTelemetry.noop().getTracer("no-op"));
        TelemetryConfig.getTelemetryConfig().setTracingEnabled(false);
        telemetryManager.createInstances();

        Tracer tracer = telemetryManager.getTracer();
        assertEquals(tracer, OpenTelemetry.noop().getTracer("no-op"));
    }

    @Test
    public void testLoadConfiguredOpenTelemetry_WithRealFactory() throws Exception
    {
        TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);

        telemetryManager.loadConfiguredOpenTelemetry();
        assertNotNull(telemetryManager.getOpenTelemetry());

        assertNotNull(telemetryManager.getTracer());
        assertNotEquals(telemetryManager.getTracer(), OpenTelemetry.noop().getTracer("no-op"));
    }
}
