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
package com.facebook.presto.common;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestTelemetryConfig
{
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("tracing-factory.name", "otltest")
                .put("tracing-enabled", "false")
                .put("tracing-backend-url", "http://0.0.0.0:123")
                .put("max-exporter-batch-size", "123")
                .put("max-queue-size", "1234")
                .put("schedule-delay", "4567")
                .put("exporter-timeout", "6789")
                .put("trace-sampling-ratio", "2.0")
                .put("span-sampling", "false")
                .build();

        TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);
        TelemetryConfig telemetryConfig = TelemetryConfig.getTelemetryConfig();
        assertEquals(TelemetryConfig.getTracingEnabled(), false);
        assertEquals(telemetryConfig.getTracingBackendUrl(), "http://0.0.0.0:123");
        assertEquals(telemetryConfig.getMaxExporterBatchSize(), 123);
        assertEquals(telemetryConfig.getMaxQueueSize(), 1234);
        assertEquals(telemetryConfig.getScheduleDelay(), 4567);
        assertEquals(telemetryConfig.getExporterTimeout(), 6789);
        assertEquals(telemetryConfig.getSamplingRatio(), 2.0);
        assertEquals(TelemetryConfig.getSpanSampling(), false);
    }
}
