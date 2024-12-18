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
import com.facebook.presto.common.telemetry.tracing.TracingEnum;
import com.facebook.presto.common.util.TextMapGetterImpl;
import com.facebook.presto.opentelemetry.OpenTelemetryImpl;
import com.facebook.presto.opentelemetry.tracing.TracingSpan;
import com.facebook.presto.spi.telemetry.TelemetryFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * OpenTelemetryManager class creates and manages OpenTelemetry and Tracer instances.
 */
public class OpenTelemetryTracingManager
{
    private static final Logger log = Logger.get(OpenTelemetryTracingManager.class);
    private static final File OPENTELEMETRY_CONFIGURATION = new File("etc/telemetry-tracing.properties");
    private static final String TRACING_FACTORY_NAME = "tracing-factory.name";

    private final Map<String, TelemetryFactory> openTelemetryFactories = new ConcurrentHashMap<>();
    private static OpenTelemetry configuredOpenTelemetry;
    private static Tracer tracer = OpenTelemetry.noop().getTracer("no-op"); //default tracer

    public OpenTelemetryTracingManager()
    {
        addOpenTelemetryFactory(new OpenTelemetryImpl());
    }

    /**
     * adds and registers all the OpenTelemetryFactory implementations to support different configurations
     * @param openTelemetryFactory
     */
    public void addOpenTelemetryFactory(TelemetryFactory openTelemetryFactory)
    {
        requireNonNull(openTelemetryFactory, "openTelemetryFactory is null");
        log.debug("Adding telemetry factory");
        if (openTelemetryFactories.putIfAbsent(openTelemetryFactory.getName(), openTelemetryFactory) != null) {
            throw new IllegalArgumentException(format("openTelemetry factory '%s' is already registered", openTelemetryFactory.getName()));
        }
    }

    public void clearFactories()
    {
        openTelemetryFactories.clear();
    }

    /**
     * called from PrestoServer for loading the properties after OpenTelemetryManager is bound and injected
     * @throws Exception
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

            TelemetryFactory<OpenTelemetry> openTelemetryFactory = openTelemetryFactories.get(openTelemetryFactoryName);
            checkState(openTelemetryFactory != null, "Opentelemetry factory %s is not registered", openTelemetryFactoryName);

            log.debug("setting telemetry properties");
            TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);

            log.debug("creating opentelemetry instance");
            this.configuredOpenTelemetry = openTelemetryFactory.create();

            log.debug("creating telemetry tracer");
            createTracer();
        }
    }

    /**
     * creates and updates sdk tracer instance if tracing is enabled. Else uses a default no-op instance.
     */
    public void createTracer()
    {
        if (TelemetryConfig.getTracingEnabled()) {
            tracer = configuredOpenTelemetry.getTracer("Presto");
        }
    }

    public static Tracer getTracer()
    {
        return tracer;
    }

    public static void setTracer(Tracer tracer)
    {
        OpenTelemetryTracingManager.tracer = tracer;
    }

    public OpenTelemetry getOpenTelemetry()
    {
        return this.configuredOpenTelemetry;
    }

    public static void setOpenTelemetry(OpenTelemetry configuredOpenTelemetry)
    {
        OpenTelemetryTracingManager.configuredOpenTelemetry = configuredOpenTelemetry;
    }

    public static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
        }
        return fromProperties(properties);
    }

    public static Runnable getCurrentContextWrap(Runnable runnable)
    {
        return Context.current().wrap(runnable);
    }

    public static Context getCurrentContext()
    {
        return Context.current();
    }

    public static Context getCurrentContextWith(TracingSpan tracingSpan)
    {
        return Context.current().with(tracingSpan.getSpan());
    }

    public static Context getContext(TracingSpan span)
    {
        return span != null ? getCurrentContextWith(span) : getCurrentContext();
    }

    private static Context getContext(String traceParent)
    {
        TextMapPropagator propagator = configuredOpenTelemetry.getPropagators().getTextMapPropagator();
        Context context = propagator.extract(Context.current(), traceParent, new TextMapGetterImpl());
        return context;
    }

    public static boolean isRecording()
    {
        return TracingSpan.fromContext(getCurrentContext()).isRecording();
    }

    public static Map<String, String> getHeadersMap(TracingSpan span)
    {
        TextMapPropagator propagator = configuredOpenTelemetry.getPropagators().getTextMapPropagator();
        Map<String, String> headersMap = new HashMap<>();
        Context context = (span != null) ? Context.current().with(span.getSpan()) : Context.current();
        Context currentContext = (TelemetryConfig.getTracingEnabled()) ? context : null;
        propagator.inject(currentContext, headersMap, Map::put);
        return headersMap;
    }

    public static void endSpanOnError(TracingSpan querySpan, Throwable throwable)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().setStatus(StatusCode.ERROR, throwable.getMessage())
                    .recordException(throwable)
                    .end();
        }
    }

    public static void addEvent(String eventState, TracingSpan querySpan)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().addEvent("query_state", Attributes.of(AttributeKey.stringKey("EVENT_STATE"), eventState));
        }
    }

    public static void setAttributeQueryType(TracingSpan querySpan, String queryType)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().setAttribute("QUERY_TYPE", queryType);
        }
    }

    public static void recordException(TracingSpan querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().setStatus(StatusCode.ERROR, nullToEmpty(message))
                    .recordException(runtimeException)
                    .setAttribute("ERROR_CODE", errorCode.getCode())
                    .setAttribute("ERROR_NAME", errorCode.getName())
                    .setAttribute("ERROR_TYPE", errorCode.getType().toString());
        }
    }

    public static void setSuccess(TracingSpan querySpan)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().setStatus(StatusCode.OK);
        }
    }

    //GetSpans
    public static TracingSpan getRootSpan()
    {
        return !TelemetryConfig.getTracingEnabled() ? null : new TracingSpan(tracer.spanBuilder(TracingEnum.ROOT.getName()).setSpanKind(SpanKind.SERVER)
                .startSpan());
    }

    public static TracingSpan getSpan(String spanName)
    {
        return !TelemetryConfig.getTracingEnabled() ? null : new TracingSpan(tracer.spanBuilder(spanName)
                .startSpan());
    }

    public static TracingSpan getSpan(String traceParent, String spanName)
    {
        TracingSpan span = !TelemetryConfig.getTracingEnabled() && traceParent != null ? null : new TracingSpan(tracer.spanBuilder(spanName)
                .setParent(getContext(traceParent))
                .startSpan());
        //context.makeCurrent();
        return span;
    }

    public static TracingSpan getSpan(TracingSpan parentSpan, String spanName, Map<String, String> attributes)
    {
        return !TelemetryConfig.getTracingEnabled() ? null : new TracingSpan(setAttributes(tracer.spanBuilder(spanName), attributes)
                .setParent(getContext(parentSpan))
                .startSpan());
    }

    private static SpanBuilder setAttributes(SpanBuilder spanBuilder, Map<String, String> attributes)
    {
        attributes.forEach(spanBuilder::setAttribute);
        return spanBuilder;
    }

    public static Optional<String> spanString(TracingSpan span)
    {
        return Optional.ofNullable(span)
                .filter(s -> span.getSpan().getSpanContext().isValid())
                .map(s -> toStringHelper("Span")
                        .add("spanId", span.getSpan().getSpanContext().getSpanId())
                        .add("traceId", span.getSpan().getSpanContext().getTraceId())
                        .toString());
    }
}
