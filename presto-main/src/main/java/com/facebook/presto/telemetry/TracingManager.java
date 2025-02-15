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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * OpenTelemetryManager class creates and manages OpenTelemetry and Tracer instances.
 */
public class TracingManager
{
    private static final Logger log = Logger.get(TracingManager.class);
    private static final File OPENTELEMETRY_CONFIGURATION = new File("etc/telemetry-tracing.properties");
    private static final String TRACING_FACTORY_NAME = "tracing-factory.name";

    private final Map<String, TelemetryFactory> openTelemetryFactories = new ConcurrentHashMap<>();
    private static AtomicReference<TelemetryTracing> configuredTelemetryTracing = new AtomicReference<>(new TelemetryTracingImpl());

    /**
     * adds and registers all the OpenTelemetryFactory implementations to support different configurations
     *
     * @param telemetryFactory the telemetry factory
     */
    public void addOpenTelemetryFactory(TelemetryFactory telemetryFactory)
    {
        requireNonNull(telemetryFactory, "openTelemetryFactory is null");
        log.debug("Adding telemetry factory");
        if (openTelemetryFactories.putIfAbsent(telemetryFactory.getName(), telemetryFactory) != null) {
            throw new IllegalArgumentException(format("openTelemetry factory '%s' is already registered", telemetryFactory.getName()));
        }
    }

    /**
     * called from PrestoServer for loading the properties after OpenTelemetryManager is bound and injected
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
                    "Opentelemetry configuration %s does not contain %s",
                    OPENTELEMETRY_CONFIGURATION.getAbsoluteFile(),
                    TRACING_FACTORY_NAME);

            if (properties.isEmpty()) {
                log.debug("telemetry properties not loaded");
            }

            properties = new HashMap<>(properties);
            String openTelemetryFactoryName = properties.remove(TRACING_FACTORY_NAME);

            checkArgument(!isNullOrEmpty(openTelemetryFactoryName), "otel-factory.name property must be present");

            TelemetryFactory openTelemetryFactory = openTelemetryFactories.get(openTelemetryFactoryName);
            checkState(openTelemetryFactory != null, "Opentelemetry factory %s is not registered", openTelemetryFactoryName);
            this.configuredTelemetryTracing.set((TelemetryTracing) openTelemetryFactory.create());

            log.debug("setting telemetry properties");
            TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);
            configuredTelemetryTracing.get().loadConfiguredOpenTelemetry();
        }
    }

    private static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
        }
        return fromProperties(properties);
    }

    /**
     * Sets configured telemetry tracing.
     *
     * @param telemetryTracing the telemetry tracing
     */
    public static void setConfiguredTelemetryTracing(TelemetryTracing telemetryTracing)
    {
        TracingManager.configuredTelemetryTracing.set(telemetryTracing);
    }

    /**
     * Gets current context wrap.
     *
     * @param runnable the runnable
     * @return the current context wrap
     */
    public static Runnable getCurrentContextWrap(Runnable runnable)
    {
        return configuredTelemetryTracing.get().getCurrentContextWrap(runnable);
    }

    /**
     * Is recording boolean.
     *
     * @return the boolean
     */
    public static boolean isRecording()
    {
        return configuredTelemetryTracing.get().isRecording();
    }

    /**
     * Gets headers map.
     *
     * @param span the span
     * @return the headers map
     */
    public static Map<String, String> getHeadersMap(BaseSpan span)
    {
        return configuredTelemetryTracing.get().getHeadersMap(span);
    }

    /**
     * End span on error.
     *
     * @param querySpan the query span
     * @param throwable the throwable
     */
    public static void endSpanOnError(BaseSpan querySpan, Throwable throwable)
    {
        configuredTelemetryTracing.get().endSpanOnError(querySpan, throwable);
    }

    /**
     * Add event.
     *
     * @param querySpan the query span
     * @param eventName the event name
     */
    public static void addEvent(BaseSpan querySpan, String eventName)
    {
        configuredTelemetryTracing.get().addEvent(querySpan, eventName);
    }

    /**
     * Add event.
     *
     * @param querySpan  the query span
     * @param eventName  the event name
     * @param eventState the event state
     */
    public static void addEvent(BaseSpan querySpan, String eventName, String eventState)
    {
        configuredTelemetryTracing.get().addEvent(querySpan, eventName, eventState);
    }

    /**
     * Sets attributes.
     *
     * @param span       the span
     * @param attributes the attributes
     */
    public static void setAttributes(BaseSpan span, Map<String, String> attributes)
    {
        configuredTelemetryTracing.get().setAttributes(span, attributes);
    }

    /**
     * Record exception.
     *
     * @param querySpan        the query span
     * @param message          the message
     * @param runtimeException the runtime exception
     * @param errorCode        the error code
     */
    public static void recordException(BaseSpan querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode)
    {
        configuredTelemetryTracing.get().recordException(querySpan, message, runtimeException, errorCode);
    }

    /**
     * Sets success.
     *
     * @param querySpan the query span
     */
    public static void setSuccess(BaseSpan querySpan)
    {
        configuredTelemetryTracing.get().setSuccess(querySpan);
    }

    /**
     * Gets invalid span.
     *
     * @return the invalid span
     */
//GetSpans
    public static BaseSpan getInvalidSpan()
    {
        return configuredTelemetryTracing.get().getInvalidSpan();
    }

    /**
     * Gets root span.
     *
     * @return the root span
     */
    public static BaseSpan getRootSpan()
    {
        return configuredTelemetryTracing.get().getRootSpan();
    }

    /**
     * Gets span.
     *
     * @param spanName the span name
     * @return the span
     */
    public static BaseSpan getSpan(String spanName)
    {
        return configuredTelemetryTracing.get().getSpan(spanName);
    }

    /**
     * Gets span.
     *
     * @param traceParent the trace parent
     * @param spanName    the span name
     * @return the span
     */
    public static BaseSpan getSpan(String traceParent, String spanName)
    {
        return configuredTelemetryTracing.get().getSpan(traceParent, spanName);
    }

    /**
     * Gets span.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param attributes the attributes
     * @return the span
     */
    public static BaseSpan getSpan(BaseSpan parentSpan, String spanName, Map<String, String> attributes)
    {
        return configuredTelemetryTracing.get().getSpan(parentSpan, spanName, attributes);
    }

    /**
     * Span string optional.
     *
     * @param span the span
     * @return the optional
     */
    public static Optional<String> spanString(BaseSpan span)
    {
        return configuredTelemetryTracing.get().spanString(span);
    }

    //Scoped Span

    /**
     * starts a basic span and passes it to overloaded method. This method is used for creating basic spans with no attributes.
     *
     * @param name     name of span to be created
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return base span
     */
    public static BaseSpan scopedSpan(String name, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(name, skipSpan);
    }

    /**
     * creates a ScopedSpan with the current span. This method is used when we manually create spans in the classes and
     * set attributes to them before passing to the Scopedspan.
     *
     * @param span     created span instance
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return base span
     */
    public static BaseSpan scopedSpan(BaseSpan span, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(span, skipSpan);
    }

    /**
     * Scoped span base span.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param attributes the attributes
     * @param skipSpan   the skip span
     * @return the base span
     */
    public static BaseSpan scopedSpan(BaseSpan parentSpan, String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(parentSpan, spanName, attributes, skipSpan);
    }

    /**
     * Scoped span base span.
     *
     * @param parentSpan the parent span
     * @param spanName   the span name
     * @param skipSpan   the skip span
     * @return the base span
     */
    public static BaseSpan scopedSpan(BaseSpan parentSpan, String spanName, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(parentSpan, spanName, skipSpan);
    }

    /**
     * Scoped span base span.
     *
     * @param spanName   the span name
     * @param attributes the attributes
     * @param skipSpan   the skip span
     * @return the base span
     */
    public static BaseSpan scopedSpan(String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        return configuredTelemetryTracing.get().scopedSpan(spanName, attributes, skipSpan);
    }
}
