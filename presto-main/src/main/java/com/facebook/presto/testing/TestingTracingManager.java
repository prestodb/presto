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

import com.facebook.presto.spi.telemetry.TelemetryFactory;
import com.facebook.presto.spi.telemetry.TelemetryTracing;
import com.facebook.presto.spi.testing.TestingTelemetryTracing;
import com.facebook.presto.telemetry.TracingManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class TestingTracingManager
        extends TracingManager
{
    private final Map<String, TelemetryFactory> openTelemetryFactories = new ConcurrentHashMap<>();
    private static AtomicReference<TestingTelemetryTracing> configuredTelemetryTracing = new AtomicReference<>();

    /**
     * adds and registers all the OpenTelemetryFactory implementations to support different configurations
     * @param telemetryFactory
     */
    public void addOpenTelemetryFactory(TelemetryFactory telemetryFactory)
    {
/*        requireNonNull(telemetryFactory, "openTelemetryFactory is null");
        if (openTelemetryFactories.putIfAbsent(telemetryFactory.getName(), telemetryFactory) != null) {
            throw new IllegalArgumentException(format("openTelemetry factory '%s' is already registered", telemetryFactory.getName()));
        }*/
        if ("oteltest".equals(telemetryFactory.getName())) {
            configuredTelemetryTracing.set((TestingTelemetryTracing) telemetryFactory.create());
        }

        if ("otel".equals(telemetryFactory.getName())) {
            TracingManager.setConfiguredTelemetryTracing((TelemetryTracing) telemetryFactory.create());
        }
    }

    public void loadConfiguredOpenTelemetry()
    {
/*        TelemetryFactory openTelemetryFactory = openTelemetryFactories.get("oteltest");
        this.configuredTelemetryTracing.set((TestingTelemetryTracing) openTelemetryFactory.create());*/

        //log.debug("setting telemetry properties");
        //TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);
        configuredTelemetryTracing.get().loadConfiguredOpenTelemetry();
    }

    public boolean isSpansEmpty()
    {
        return configuredTelemetryTracing.get().isSpansEmpty();
    }

    public boolean spansAnyMatch(String task)
    {
        return configuredTelemetryTracing.get().spansAnyMatch(task);
    }

    public void clearSpanList()
    {
        configuredTelemetryTracing.get().clearSpanList();
    }
}
