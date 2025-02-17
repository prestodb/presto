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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.spi.telemetry.BaseSpan;
import com.facebook.presto.spi.telemetry.TelemetryFactory;
import com.facebook.presto.spi.telemetry.TelemetryTracing;

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

    private final Map<String, TelemetryFactory> openTelemetryFactories = new ConcurrentHashMap<>();
    private static AtomicReference<TelemetryTracing> configuredTelemetryTracing = new AtomicReference<>(new TelemetryTracingImpl());

    /**
     * Adds and registers all the TelemetryFactory implementations using PluginManager to support tracing.
     *
     * @param telemetryFactory the telemetry factory
     */
    public void addOpenTelemetryFactory(TelemetryFactory telemetryFactory)
    {
        requireNonNull(telemetryFactory, "telemetryFactory is null");
        log.debug("Adding telemetry factory");
        if (openTelemetryFactories.putIfAbsent(telemetryFactory.getName(), telemetryFactory) != null) {
            throw new IllegalArgumentException(format("Telemetry factory '%s' is already registered", telemetryFactory.getName()));
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
            String openTelemetryFactoryName = properties.remove(TRACING_FACTORY_NAME);

            checkArgument(!isNullOrEmpty(openTelemetryFactoryName), TRACING_FACTORY_NAME + " property must be present");

            TelemetryFactory openTelemetryFactory = openTelemetryFactories.get(openTelemetryFactoryName);
            checkState(openTelemetryFactory != null, "Telemetry factory %s is not registered", openTelemetryFactoryName);
            this.configuredTelemetryTracing.set((TelemetryTracing) openTelemetryFactory.create());

            log.debug("setting telemetry-tracing properties");
            TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);
            configuredTelemetryTracing.get().loadConfiguredOpenTelemetry();
        }
    }

    public static void setConfiguredTelemetryTracing(TelemetryTracing telemetryTracing)
    {
        TracingManager.configuredTelemetryTracing.set(telemetryTracing);
    }

    public static Runnable getCurrentContextWrap(Runnable runnable)
    {
        return configuredTelemetryTracing.get().getCurrentContextWrap(runnable);
    }

    public static boolean isRecording()
    {
        return configuredTelemetryTracing.get().isRecording();
    }

    public static Map<String, String> getHeadersMap(BaseSpan span)
    {
        return configuredTelemetryTracing.get().getHeadersMap(span);
    }

    public static void endSpanOnError(BaseSpan querySpan, Throwable throwable)
    {
        configuredTelemetryTracing.get().endSpanOnError(querySpan, throwable);
    }

    public static void addEvent(BaseSpan querySpan, String eventName)
    {
        configuredTelemetryTracing.get().addEvent(querySpan, eventName);
    }

    public static void addEvent(BaseSpan querySpan, String eventName, String eventState)
    {
        configuredTelemetryTracing.get().addEvent(querySpan, eventName, eventState);
    }

    public static void setAttributes(BaseSpan span, Map<String, String> attributes)
    {
        configuredTelemetryTracing.get().setAttributes(span, attributes);
    }

    public static void recordException(BaseSpan querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode)
    {
        configuredTelemetryTracing.get().recordException(querySpan, message, runtimeException, errorCode);
    }

    public static void setSuccess(BaseSpan querySpan)
    {
        configuredTelemetryTracing.get().setSuccess(querySpan);
    }

    public static BaseSpan getInvalidSpan()
    {
        return configuredTelemetryTracing.get().getInvalidSpan();
    }

    public static BaseSpan getRootSpan()
    {
        return configuredTelemetryTracing.get().getRootSpan();
    }

    public static BaseSpan getSpan(String spanName)
    {
        return configuredTelemetryTracing.get().getSpan(spanName);
    }

    public static BaseSpan getSpan(String traceParent, String spanName)
    {
        return configuredTelemetryTracing.get().getSpan(traceParent, spanName);
    }

    public static BaseSpan getSpan(BaseSpan parentSpan, String spanName, Map<String, String> attributes)
    {
        return configuredTelemetryTracing.get().getSpan(parentSpan, spanName, attributes);
    }

    public static Optional<String> spanString(BaseSpan span)
    {
        return configuredTelemetryTracing.get().spanString(span);
    }

    public static BaseSpan scopedSpan(String name, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(name, skipSpan);
    }

    public static BaseSpan scopedSpan(BaseSpan span, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(span, skipSpan);
    }

    public static BaseSpan scopedSpan(BaseSpan parentSpan, String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(parentSpan, spanName, attributes, skipSpan);
    }

    public static BaseSpan scopedSpan(BaseSpan parentSpan, String spanName, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(parentSpan, spanName, skipSpan);
    }

    public static BaseSpan scopedSpan(String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(spanName, attributes, skipSpan);
    }
}
