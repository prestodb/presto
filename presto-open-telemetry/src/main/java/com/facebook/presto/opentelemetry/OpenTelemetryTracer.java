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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.tracing.Tracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.StandardErrorCode.DISTRIBUTED_TRACING_ERROR;

public class OpenTelemetryTracer
        implements Tracer
{
    private static String currentContextPropagator = OpenTelemetryContextPropagator.B3_SINGLE_HEADER;
    private static OpenTelemetry openTelemetry = OpenTelemetryBuilder.build(currentContextPropagator);
    private final io.opentelemetry.api.trace.Tracer openTelemetryTracer;
    private final String traceToken;
    private final Span parentSpan;
    private final Context parentContext;

    public final Map<String, Span> spanMap = new ConcurrentHashMap<>();
    public final Map<String, Span> recorderSpanMap = new LinkedHashMap<>();

    public OpenTelemetryTracer(String traceToken, String contextPropagator, String propagatedContext, String baggage)
    {
        // Trivial getter method to return carrier
        // Carrier will be equal to the data to be fetched
        TextMapGetter<String> trivialGetter = new TextMapGetter<String>()
        {
            @Override
            public String get(String carrier, String key)
            {
                return carrier;
            }

            @Override
            public Iterable<String> keys(String carrier)
            {
                return Arrays.asList(get(carrier, null));
            }
        };

        // Rebuild OPEN_TELEMETRY instance if necessary (to use different context propagator)
        // Will only occur once at max, if contextPropagator is different from B3_SINGLE_HEADER
        if (contextPropagator != null && !contextPropagator.equals(currentContextPropagator)) {
            this.openTelemetry = OpenTelemetryBuilder.build(contextPropagator);
            this.currentContextPropagator = contextPropagator;
        }

        this.openTelemetryTracer = openTelemetry.getTracer(tracerName);
        this.traceToken = traceToken;

        if (propagatedContext != null) {
            // Only process baggage headers if context propagation is successful
            Context extractedContext = openTelemetry.getPropagators().getTextMapPropagator()
                    .extract(Context.current(), propagatedContext, trivialGetter);
            Context contextWithBaggage = W3CBaggagePropagator.getInstance().extract(
                    extractedContext, baggage, trivialGetter);
            try (Scope ignored = contextWithBaggage.makeCurrent()) {
                this.parentSpan = createParentSpan();
            }
            this.parentContext = contextWithBaggage;
        }
        else {
            this.parentSpan = createParentSpan();
            this.parentContext = Context.current();
        }
        addBaggageToSpanAttributes(this.parentSpan);

        synchronized (recorderSpanMap) {
            recorderSpanMap.put("Trace start", this.parentSpan);
        }
    }

    /**
     * Take parent context baggage and set as span attributes.
     * Call during each span and nested span creation to properly propagate tags.
     */
    private void addBaggageToSpanAttributes(Span span)
    {
        Baggage baggage = Baggage.fromContext(parentContext);
        baggage.forEach((s, baggageEntry) -> span.setAttribute(s, baggageEntry.getValue()));
    }

    private Span createParentSpan()
    {
        Span parentSpan = openTelemetryTracer.spanBuilder("Trace start").startSpan();
        parentSpan.setAttribute("trace_id", traceToken);
        return parentSpan;
    }

    private void endUnendedBlocks()
    {
        List<String> blocks = new ArrayList<>(spanMap.keySet());
        for (String currBlock : blocks) {
            endBlock(currBlock, "");
        }
    }

    /**
     * Add annotation as event to parent span
     * @param annotation event to add
     */
    @Override
    public void addPoint(String annotation)
    {
        parentSpan.addEvent(annotation);
    }

    /**
     * Create new span with Open Telemetry tracer
     * @param blockName name of span
     * @param annotation event to add to span
     */

    @Override
    public void startBlock(String blockName, String annotation)
    {
        if (spanMap.containsKey(blockName)) {
            throw new PrestoException(DISTRIBUTED_TRACING_ERROR, "Duplicated block inserted: " + blockName);
        }
        Span span = openTelemetryTracer.spanBuilder(blockName)
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        span.addEvent(annotation);
        addBaggageToSpanAttributes(span);

        spanMap.put(blockName, span);
        synchronized (recorderSpanMap) {
            recorderSpanMap.put(blockName, span);
        }
    }

    @Override
    public void addPointToBlock(String blockName, String annotation)
    {
        if (!spanMap.containsKey(blockName)) {
            throw new PrestoException(DISTRIBUTED_TRACING_ERROR, "Adding point to non-existing block: " + blockName);
        }
        spanMap.get(blockName).addEvent(annotation);
    }

    /**
     * End Open Telemetry span
     * @param blockName name of span
     * @param annotation event to add to span
     */
    @Override
    public void endBlock(String blockName, String annotation)
    {
        if (!spanMap.containsKey(blockName)) {
            throw new PrestoException(DISTRIBUTED_TRACING_ERROR, "Trying to end a non-existing block: " + blockName);
        }
        spanMap.remove(blockName);
        synchronized (recorderSpanMap) {
            Span span = recorderSpanMap.get(blockName);
            span.addEvent(annotation);
            span.end();
        }
    }

    @Override
    public void endTrace(String annotation)
    {
        parentSpan.addEvent(annotation);
        endUnendedBlocks();
        parentSpan.end();
    }

    @Override
    public String getTracerId()
    {
        if (traceToken != null) {
            return traceToken;
        }
        return tracerName;
    }
}
