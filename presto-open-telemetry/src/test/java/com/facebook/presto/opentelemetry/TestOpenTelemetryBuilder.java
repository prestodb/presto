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

import com.facebook.presto.common.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestOpenTelemetryBuilder
{
    @BeforeMethod
    public void setUp() throws Exception
    {
        resetTelemetryConfigSingleton();
    }

    @Test
    public void testCreateWithTracingEnabled()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("tracing-enabled", "true");
        properties.put("tracing-backend-url", "http://localhost:4317");
        properties.put("max-exporter-batch-size", "256");
        properties.put("max-queue-size", "1024");
        properties.put("exporter-timeout", "5000");
        properties.put("schedule-delay", "1000");
        properties.put("trace-sampling-ratio", "1.0");
        properties.put("span-sampling", "true");

        TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);
        OpenTelemetry openTelemetry = OpenTelemetryBuilder.build();

        assertNotNull(openTelemetry);
        assertTrue(openTelemetry instanceof OpenTelemetrySdk, "sdk instance");
    }

    @Test
    public void testCreateWithTracingDisabled()
    {
        TelemetryConfig.getTelemetryConfig().setTracingEnabled(false);
        OpenTelemetry openTelemetry = OpenTelemetryBuilder.build();

        assertNotNull(openTelemetry);
        assertEquals(openTelemetry, OpenTelemetry.noop(), "no-op instance");
    }

    private void resetTelemetryConfigSingleton() throws Exception
    {
        Field instanceField = TelemetryConfig.class.getDeclaredField("telemetryConfig");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }
}
