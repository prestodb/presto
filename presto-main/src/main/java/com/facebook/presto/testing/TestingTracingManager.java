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

import com.facebook.presto.spi.testing.TestingTracer;
import com.facebook.presto.spi.tracing.Tracer;
import com.facebook.presto.spi.tracing.TracerProvider;
import com.facebook.presto.tracing.TracingManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class TestingTracingManager
        extends TracingManager
{
    private static final String OTEL_TEST = "oteltest";
    private static final String OTEL = "otel";

    private final Map<String, TracerProvider> openTelemetryFactories = new ConcurrentHashMap<>();
    private static AtomicReference<TestingTracer> configuredTracer = new AtomicReference<>();

    /**
     * adds and registers all the TracerProvider implementations to support testing.
     * @param tracerProvider
     */
    public void addTraceProvider(TracerProvider tracerProvider)
    {
        if (OTEL_TEST.equals(tracerProvider.getName())) {
            configuredTracer.set((TestingTracer) tracerProvider.create());
        }

        if (OTEL.equals(tracerProvider.getName())) {
            TracingManager.setConfiguredTelemetryTracer((Tracer) tracerProvider.create());
        }
    }

    public void loadConfiguredOpenTelemetry()
    {
        configuredTracer.get().loadConfiguredOpenTelemetry();
    }

    public boolean isSpansEmpty()
    {
        return configuredTracer.get().isSpansEmpty();
    }

    public boolean spansAnyMatch(String task)
    {
        return configuredTracer.get().spansAnyMatch(task);
    }

    public void clearSpanList()
    {
        configuredTracer.get().clearSpanList();
    }
}
