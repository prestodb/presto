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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.common.telemetry.tracing.TracingEnum;
import com.facebook.presto.opentelemetry.tracing.ScopedSpan;
import com.facebook.presto.opentelemetry.tracing.TracingSpan;
import com.facebook.presto.spi.telemetry.TelemetryTracing;
import com.google.errorprone.annotations.MustBeClosed;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.nullToEmpty;

public class OpenTelemetryTracingImpl
        implements TelemetryTracing<TracingSpan, ScopedSpan>
{
    private static final Logger log = Logger.get(OpenTelemetryTracingImpl.class);
    private static OpenTelemetry configuredOpenTelemetry;
    private static Tracer tracer = OpenTelemetry.noop().getTracer("no-op"); //default tracer

    /**
     * called from PrestoServer for loading the properties after OpenTelemetryManager is bound and injected
     */
    @Override
    public void loadConfiguredOpenTelemetry()
    {
        log.debug("creating opentelemetry instance");
        configuredOpenTelemetry = createOpenTelemetry();

        log.debug("creating telemetry tracer");
        createTracer();
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

    /**
     * Create opentelemetry instance
     *
     * @return {@link OpenTelemetry}
     */
    public OpenTelemetry createOpenTelemetry()
    {
        TelemetryConfig telemetryConfig = TelemetryConfig.getTelemetryConfig();
        OpenTelemetry openTelemetry = OpenTelemetry.noop(); //default instance for tracing disabled case

        if (TelemetryConfig.getTracingEnabled()) {
            log.debug("telemetry tracing is enabled");
            Resource resource = Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "Presto"));

            SpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                    .setEndpoint(telemetryConfig.getTracingBackendUrl())
                    .setTimeout(10, TimeUnit.SECONDS)
                    .build();
            log.debug("telemetry span exporter configured");

            SpanProcessor spanProcessor = BatchSpanProcessor.builder(spanExporter)
                    .setMaxExportBatchSize(telemetryConfig.getMaxExporterBatchSize())
                    .setMaxQueueSize(telemetryConfig.getMaxQueueSize())
                    .setScheduleDelay(telemetryConfig.getScheduleDelay(), TimeUnit.MILLISECONDS)
                    .setExporterTimeout(telemetryConfig.getExporterTimeout(), TimeUnit.MILLISECONDS)
                    .build();
            log.debug("telemetry span processor configured");

            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .setSampler(Sampler.traceIdRatioBased(telemetryConfig.getSamplingRatio()))
                    .addSpanProcessor(spanProcessor)
                    .setResource(resource)
                    .build();
            log.debug("telemetry tracer provider set");

            openTelemetry = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setPropagators(ContextPropagators.create(
                            TextMapPropagator.composite(W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
                    .build();
            log.debug("opentelemetry instance created");
        }
        else {
            log.debug("telemetry tracing is disabled");
        }

        return openTelemetry;
    }

    public static void setOpenTelemetry(OpenTelemetry configuredOpenTelemetry)
    {
        OpenTelemetryTracingImpl.configuredOpenTelemetry = configuredOpenTelemetry;
    }

    public static void setTracer(Tracer tracer)
    {
        OpenTelemetryTracingImpl.tracer = tracer;
    }

    @Override
    public Runnable getCurrentContextWrap(Runnable runnable)
    {
        return Context.current().wrap(runnable);
    }

    private static Context getCurrentContext()
    {
        return Context.current();
    }

    private static Context getCurrentContextWith(TracingSpan tracingSpan)
    {
        return Context.current().with(tracingSpan.getSpan());
    }

    private static Context getContext(TracingSpan span)
    {
        return span != null ? getCurrentContextWith(span) : getCurrentContext();
    }

    private static Context getContext(String traceParent)
    {
        TextMapPropagator propagator = configuredOpenTelemetry.getPropagators().getTextMapPropagator();
        return propagator.extract(Context.current(), traceParent, new TextMapGetterImpl());
    }

    @Override
    public boolean isRecording()
    {
        return Span.fromContext(getCurrentContext()).isRecording();
    }

    @Override
    public Map<String, String> getHeadersMap(TracingSpan span)
    {
        TextMapPropagator propagator = configuredOpenTelemetry.getPropagators().getTextMapPropagator();
        Map<String, String> headersMap = new HashMap<>();
        Context context = (span != null) ? Context.current().with(span.getSpan()) : Context.current();
        Context currentContext = (TelemetryConfig.getTracingEnabled()) ? context : null;
        propagator.inject(currentContext, headersMap, Map::put);
        return headersMap;
    }

    @Override
    public void endSpanOnError(TracingSpan querySpan, Throwable throwable)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().setStatus(StatusCode.ERROR, throwable.getMessage())
                    .recordException(throwable)
                    .end();
        }
    }

    @Override
    public void addEvent(TracingSpan span, String eventName)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            span.getSpan().addEvent(eventName);
        }
    }

    @Override
    public void addEvent(TracingSpan querySpan, String eventName, String eventState)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().addEvent(eventName, Attributes.of(AttributeKey.stringKey("EVENT_STATE"), eventState));
        }
    }

    @Override
    public void setAttributes(TracingSpan span, Map<String, String> attributes)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            attributes.forEach(span::setAttribute);
        }
    }

    @Override
    public void recordException(TracingSpan querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().setStatus(StatusCode.ERROR, nullToEmpty(message))
                    .recordException(runtimeException)
                    .setAttribute("ERROR_CODE", errorCode.getCode())
                    .setAttribute("ERROR_NAME", errorCode.getName())
                    .setAttribute("ERROR_TYPE", errorCode.getType().toString());
        }
    }

    @Override
    public void setSuccess(TracingSpan querySpan)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(querySpan)) {
            querySpan.getSpan().setStatus(StatusCode.OK);
        }
    }

    //GetSpans
    @Override
    public TracingSpan getInvalidSpan()
    {
        return new TracingSpan(Span.getInvalid());
    }

    @Override
    public TracingSpan getRootSpan()
    {
        return !TelemetryConfig.getTracingEnabled() ? null : new TracingSpan(tracer.spanBuilder(TracingEnum.ROOT.getName()).setSpanKind(SpanKind.SERVER)
                .startSpan());
    }

    @Override
    public TracingSpan getSpan(String spanName)
    {
        return !TelemetryConfig.getTracingEnabled() ? null : new TracingSpan(tracer.spanBuilder(spanName)
                .startSpan());
    }

    @Override
    public TracingSpan getSpan(String traceParent, String spanName)
    {
        TracingSpan span = !TelemetryConfig.getTracingEnabled() || traceParent == null ? null : new TracingSpan(tracer.spanBuilder(spanName)
                .setParent(getContext(traceParent))
                .startSpan());
        //context.makeCurrent();
        return span;
    }

    @Override
    public TracingSpan getSpan(TracingSpan parentSpan, String spanName, Map<String, String> attributes)
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

    @Override
    public Optional<String> spanString(TracingSpan span)
    {
        return Optional.ofNullable(span)
                .filter(s -> span.getSpan().getSpanContext().isValid())
                .map(s -> toStringHelper("Span")
                        .add("spanId", span.getSpan().getSpanContext().getSpanId())
                        .add("traceId", span.getSpan().getSpanContext().getTraceId())
                        .toString());
    }

    //Scoped Span
    /**
     * starts a basic span and passes it to overloaded method. This method is used for creating basic spans with no attributes.
     * @param name name of span to be created
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return
     */
    @MustBeClosed
    @Override
    public ScopedSpan scopedSpan(String name, Boolean... skipSpan)
    {
        if (!TelemetryConfig.getTracingEnabled() || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        return scopedSpan(new TracingSpan(tracer.spanBuilder(name).startSpan()));
    }

    /**
     * creates a ScopedSpan with the current span. This method is used when we manually create spans in the classes and
     * set attributes to them before passing to the ScopedSpan.
     * @param span created span instance
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return
     */
    @MustBeClosed
    @Override
    public ScopedSpan scopedSpan(TracingSpan span, Boolean... skipSpan)
    {
        if ((!TelemetryConfig.getTracingEnabled() || Objects.isNull(span)) || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        return new ScopedSpan(span.getSpan());
    }

    @MustBeClosed
    @Override
    public ScopedSpan scopedSpan(TracingSpan parentSpan, String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        if (!TelemetryConfig.getTracingEnabled() || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName);
        Span span = setAttributes(spanBuilder, attributes)
                .setParent(getContext(parentSpan))
                .startSpan();
        return new ScopedSpan(span);
    }

    @MustBeClosed
    @Override
    public ScopedSpan scopedSpan(TracingSpan parentSpan, String spanName, Boolean... skipSpan)
    {
        if (!TelemetryConfig.getTracingEnabled() || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        Span span = tracer.spanBuilder(spanName)
                .setParent(getContext(parentSpan))
                .startSpan();
        return new ScopedSpan(span);
    }

    @MustBeClosed
    @Override
    public ScopedSpan scopedSpan(String spanName, Map<String, String> attributes, Boolean... skipSpan)
    {
        if (!TelemetryConfig.getTracingEnabled() || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName);
        Span span = setAttributes(spanBuilder, attributes).startSpan();
        return new ScopedSpan(span);
    }
}
