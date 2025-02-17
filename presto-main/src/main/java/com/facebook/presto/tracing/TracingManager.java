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
package com.facebook.presto.tracing;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.spi.tracing.BaseSpan;
import com.facebook.presto.spi.tracing.Tracer;
import com.facebook.presto.spi.tracing.TracerProvider;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * TelemetryManager class creates and manages Telemetry and Tracer instances.
 */
public class TracingManager
{
    private static final Logger log = Logger.get(TracingManager.class);
    private static final File OPENTELEMETRY_CONFIGURATION = new File("etc/telemetry-tracing.properties");
    private static final String TRACING_FACTORY_NAME = "tracing-factory.name";

    private final Map<String, TracerProvider> traceProviders = new ConcurrentHashMap<>();
    private static AtomicReference<Tracer> configuredTelemetryTracer = new AtomicReference<>(new DefaultTelemetryTracer());

    /**
     * Adds and registers all the TracerProvider implementations using PluginManager to support tracing.
     *
     * @param tracerProvider the telemetry factory
     */
    public void addTraceProvider(TracerProvider tracerProvider)
    {
        requireNonNull(tracerProvider, "tracerProvider is null");
        log.debug("Adding telemetry factory");
        if (traceProviders.putIfAbsent(tracerProvider.getName(), tracerProvider) != null) {
            throw new IllegalArgumentException(format("Telemetry factory '%s' is already registered", tracerProvider.getName()));
        }
    }

    /**
     * Instantiates required Telemetry instances after loading the plugin implementation.
     * Called from PrestoServer after the plugin implementation got loaded.
     *
     * @throws Exception the exception
     */
    public void loadConfiguredOpenTelemetry()
            throws Exception
    {
        if (OPENTELEMETRY_CONFIGURATION.exists()) {
            Map<String, String> properties = loadProperties(OPENTELEMETRY_CONFIGURATION);
            checkArgument(
                    !isNullOrEmpty(properties.get(TRACING_FACTORY_NAME)),
                    "Telemetry configuration %s does not contain %s",
                    OPENTELEMETRY_CONFIGURATION.getAbsoluteFile(),
                    TRACING_FACTORY_NAME);

            if (properties.isEmpty()) {
                log.debug("telemetry properties not loaded");
            }

            properties = new HashMap<>(properties);
            String telemetryFactoryName = properties.remove(TRACING_FACTORY_NAME);

            checkArgument(!isNullOrEmpty(telemetryFactoryName), TRACING_FACTORY_NAME + " property must be present");

            TracerProvider tracerProvider = traceProviders.get(telemetryFactoryName);
            checkState(tracerProvider != null, "Telemetry factory %s is not registered", telemetryFactoryName);
            this.configuredTelemetryTracer.set((Tracer) tracerProvider.create());

            log.debug("setting telemetry-tracing properties");
            TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);
            configuredTelemetryTracer.get().loadConfiguredOpenTelemetry();
        }
    }

    public static void setConfiguredTelemetryTracer(Tracer tracer)
    {
        TracingManager.configuredTelemetryTracer.set(tracer);
    }

    public static Runnable getCurrentContextWrap(Runnable runnable)
    {
        return configuredTelemetryTracer.get().getCurrentContextWrap(runnable);
    }

    public static boolean isRecording()
    {
        return configuredTelemetryTracer.get().isRecording();
    }

    public static Map<String, String> getHeadersMap(BaseSpan span)
    {
        return configuredTelemetryTracer.get().getHeadersMap(span);
    }

    public static void endSpanOnError(BaseSpan querySpan, Throwable throwable)
    {
        configuredTelemetryTracer.get().endSpanOnError(querySpan, throwable);
    }

    public static void addEvent(BaseSpan querySpan, String eventName)
    {
        configuredTelemetryTracer.get().addEvent(querySpan, eventName);
    }

    public static void addEvent(BaseSpan querySpan, String eventName, String eventState)
    {
        configuredTelemetryTracer.get().addEvent(querySpan, eventName, eventState);
    }

    public static void setAttributes(BaseSpan span, Map<String, String> attributes)
    {
        configuredTelemetryTracer.get().setAttributes(span, attributes);
    }

    public static void recordException(BaseSpan querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode)
    {
        configuredTelemetryTracer.get().recordException(querySpan, message, runtimeException, errorCode);
    }

    public static void setSuccess(BaseSpan querySpan)
    {
        configuredTelemetryTracer.get().setSuccess(querySpan);
    }

    public static BaseSpan getInvalidSpan()
    {
        return configuredTelemetryTracer.get().getInvalidSpan();
    }

    public static BaseSpan getRootSpan(String traceId)
    {
        return configuredTelemetryTracer.get().getRootSpan(traceId);
    }

    public static BaseSpan getSpan(String spanName)
    {
        return configuredTelemetryTracer.get().getSpan(spanName);
    }

    public static BaseSpan getSpan(String traceParent, String spanName)
    {
        return configuredTelemetryTracer.get().getSpan(traceParent, spanName);
    }

    public static BaseSpan getSpan(BaseSpan parentSpan, String spanName, Map<String, String> attributes)
    {
        return configuredTelemetryTracer.get().getSpan(parentSpan, spanName, attributes);
    }

    public static Optional<String> spanString(BaseSpan span)
    {
        return configuredTelemetryTracer.get().spanString(span);
    }

    public static BaseSpan scopedSpan(String name, Boolean... skipSpan)
    {
        return configuredTelemetryTracer.get().scopedSpan(name, skipSpan);
    }

    public static BaseSpan scopedSpan(BaseSpan span, Boolean... skipSpan)
    {
        return configuredTelemetryTracer.get().scopedSpan(span, skipSpan);
    }

    public static BaseSpan scopedSpan(BaseSpan parentSpan, String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        return configuredTelemetryTracer.get().scopedSpan(parentSpan, spanName, attributes, skipSpan);
    }

    public static BaseSpan scopedSpan(BaseSpan parentSpan, String spanName, Boolean... skipSpan)
    {
        return configuredTelemetryTracer.get().scopedSpan(parentSpan, spanName, skipSpan);
    }

    public static BaseSpan scopedSpan(String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        return configuredTelemetryTracer.get().scopedSpan(spanName, attributes, skipSpan);
    }
}
