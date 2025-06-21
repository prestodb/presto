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
import com.facebook.presto.spi.tracing.Tracer;
import com.google.errorprone.annotations.MustBeClosed;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Strings.nullToEmpty;

/**
 * Open Telemetry implementation of tracing.
 */
public class OpenTelemetryTracer
        implements Tracer<TracingSpan, ScopedSpan>
{
    private static final Logger log = Logger.get(OpenTelemetryTracer.class);
    private static OpenTelemetry configuredOpenTelemetry;
    private static io.opentelemetry.api.trace.Tracer tracer = OpenTelemetry.noop().getTracer("no-op"); //default tracer

    public OpenTelemetryTracer()
    {
        loadConfiguredTelemetry();
    }

    /**
     * Create and set the opentelemetry and tracer instance.
     * Called from TracingManager.
     */
    public void loadConfiguredTelemetry()
    {
        log.debug("creating opentelemetry instance");
        configuredOpenTelemetry = OpenTelemetryBuilder.build();

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

    public static void setOpenTelemetry(OpenTelemetry configuredOpenTelemetry)
    {
        OpenTelemetryTracer.configuredOpenTelemetry = configuredOpenTelemetry;
    }

    public static void setTracer(io.opentelemetry.api.trace.Tracer tracer)
    {
        OpenTelemetryTracer.tracer = tracer;
    }

    /**
     * get current context wrapped .
     *
     * @param runnable runnable
     * @return Runnable
     */
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

    /**
     * returns true if the span records tracing events.
     *
     * @return boolean
     */
    @Override
    public boolean isRecording()
    {
        return Span.fromContext(getCurrentContext()).isRecording();
    }

    /**
     * Returns headers map from the input span.
     *
     * @param span span
     * @return Map<String, String>
     */
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

    /**
     * Ends span by updating the status to error and record input exception.
     *
     * @param span querySpan
     * @param throwable throwable
     * @return Map<String, String>
     */
    @Override
    public void endSpanOnError(TracingSpan span, Throwable throwable)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            span.getSpan().setStatus(StatusCode.ERROR, throwable.getMessage())
                    .recordException(throwable)
                    .end();
        }
    }

    /**
     * Add the input event to the input span.
     *
     * @param span span
     * @param eventName eventName
     */
    @Override
    public void addEvent(TracingSpan span, String eventName)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            span.getSpan().addEvent(eventName);
        }
    }

    /**
     * Add the input event to the input span.
     *
     * @param span span
     * @param eventName eventName
     * @param eventState eventState
     */
    @Override
    public void addEvent(TracingSpan span, String eventName, String eventState)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            span.getSpan().addEvent(eventName, Attributes.of(AttributeKey.stringKey("EVENT_STATE"), eventState));
        }
    }

    /**
     * Sets the attributes map to the input span.
     *
     * @param span span
     * @param attributes attributes
     */
    @Override
    public void setAttributes(TracingSpan span, Map<String, String> attributes)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            attributes.forEach(span::setAttribute);
        }
    }

    /**
     * Records exception to the input span with error code and message.
     *
     * @param span span
     * @param message message
     * @param exception exception
     * @param errorCode errorCode
     */
    @Override
    public void recordException(TracingSpan span, String message, Exception exception, ErrorCode errorCode)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            span.getSpan().setStatus(StatusCode.ERROR, nullToEmpty(message))
                    .recordException(exception)
                    .setAttribute("ERROR_CODE", errorCode.getCode())
                    .setAttribute("ERROR_NAME", errorCode.getName())
                    .setAttribute("ERROR_TYPE", errorCode.getType().toString());
        }
    }

    /**
     * Sets the status of the input span to success.
     *
     * @param span span
     */
    @Override
    public void setSuccess(TracingSpan span)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            span.getSpan().setStatus(StatusCode.OK);
        }
    }

    //Tracing spans
    /**
     * Returns an invalid Span. An invalid Span is used when tracing is disabled.
     * @return TracingSpan
     */
    @Override
    public TracingSpan getInvalidSpan()
    {
        return new TracingSpan(Span.getInvalid());
    }

    /**
     * Creates and returns the root span.
     * @return TracingSpan
     */
    @Override
    public TracingSpan getRootSpan(String traceId)
    {
        return !TelemetryConfig.getTracingEnabled() ? new TracingSpan(Span.getInvalid()) : new TracingSpan(tracer.spanBuilder(TracingEnum.ROOT.getName()).setSpanKind(SpanKind.SERVER).setAttribute("trace_id", traceId)
                .startSpan());
    }

    /**
     * Creates and returns the span with input name.
     * @param spanName name of span to be created
     * @return TracingSpan
     */
    @Override
    public TracingSpan getSpan(String spanName)
    {
        return !TelemetryConfig.getTracingEnabled() ? new TracingSpan(Span.getInvalid()) : new TracingSpan(tracer.spanBuilder(spanName)
                .startSpan());
    }

    /**
     * Creates and returns the span with input name and parent context from input.
     * @param traceParent trace parent string.
     * @param spanName name of the span to be created.
     * @return TracingSpan
     */
    @Override
    public TracingSpan getSpan(String traceParent, String spanName)
    {
        return !TelemetryConfig.getTracingEnabled() || traceParent == null ? new TracingSpan(Span.getInvalid()) : new TracingSpan(tracer.spanBuilder(spanName)
                .setParent(getContext(traceParent))
                .startSpan());
    }

    /**
     * Creates and returns the span with input name, attributes and parent span as the input span.
     * @param parentSpan parent span.
     * @param spanName name of the span to be created.
     * @param attributes input attributes to set in span.
     * @return TracingSpan
     */
    @Override
    public TracingSpan getSpan(TracingSpan parentSpan, String spanName, Map<String, String> attributes)
    {
        return !TelemetryConfig.getTracingEnabled() ? new TracingSpan(Span.getInvalid()) : new TracingSpan(setAttributes(tracer.spanBuilder(spanName), attributes)
                .setParent(getContext(parentSpan))
                .startSpan());
    }

    private static SpanBuilder setAttributes(SpanBuilder spanBuilder, Map<String, String> attributes)
    {
        attributes.forEach(spanBuilder::setAttribute);
        return spanBuilder;
    }

    //Scoped Spans
    /**
     * Creates and returns the ScopedSpan with input name.
     * @param spanName name of span to be created
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return ScopedSpan
     */
    @MustBeClosed
    @Override
    public ScopedSpan scopedSpan(String spanName, Boolean... skipSpan)
    {
        if (!TelemetryConfig.getTracingEnabled() || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        return scopedSpan(new TracingSpan(tracer.spanBuilder(spanName).startSpan()));
    }

    /**
     * Creates and returns the ScopedSpan with parent span as input span.
     * @param parentSpan parent span
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return ScopedSpan
     */
    @MustBeClosed
    @Override
    public ScopedSpan scopedSpan(TracingSpan parentSpan, Boolean... skipSpan)
    {
        if ((!TelemetryConfig.getTracingEnabled() || Objects.isNull(parentSpan)) || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        return new ScopedSpan(parentSpan.getSpan());
    }

    /**
     * Creates and returns the ScopedSpan with input name, attributes and parent span as the input span.
     * @param parentSpan parent span
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return ScopedSpan
     */
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

    /**
     * Creates and returns the ScopedSpan with input name and the parent span as input span.
     * @param parentSpan parent span
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return ScopedSpan
     */
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

    /**
     * Creates and returns the ScopedSpan with input name and attributes.
     * @param spanName span name
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return ScopedSpan
     */
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
