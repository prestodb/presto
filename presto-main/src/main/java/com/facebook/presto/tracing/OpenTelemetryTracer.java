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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.tracing.Tracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.StandardErrorCode.DISTRIBUTED_TRACING_ERROR;

public class OpenTelemetryTracer
        implements Tracer
{
    public static final OpenTelemetry OPEN_TELEMETRY = OpenTelemetryBuilder.build();
    public final io.opentelemetry.api.trace.Tracer openTelemetryTracer;
    public final String tracerName;
    public final String traceToken;
    public final Span parentSpan;

    public final Map<String, Span> spanMap = new ConcurrentHashMap<String, Span>();
    public final Map<String, Span> recorderSpanMap = new LinkedHashMap<String, Span>();

    public OpenTelemetryTracer(String tracerName, String traceToken)
    {
        openTelemetryTracer = OPEN_TELEMETRY.getTracer(tracerName);
        this.tracerName = tracerName;
        this.traceToken = traceToken;

        parentSpan = openTelemetryTracer.spanBuilder("Trace start").startSpan();
        parentSpan.setAttribute("trace_token", traceToken);

        synchronized (recorderSpanMap) {
            recorderSpanMap.put("Trace start", parentSpan);
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
        spanMap.put(blockName, span);
        synchronized (recorderSpanMap) {
            recorderSpanMap.put(blockName, span);
        }
    }

    /**
     * Add event to Open Telemetry span
     * @param blockName name of span
     * @param annotation name of event
     */
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
