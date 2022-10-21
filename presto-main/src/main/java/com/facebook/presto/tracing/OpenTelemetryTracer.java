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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.StandardErrorCode.DISTRIBUTED_TRACING_ERROR;

public class OpenTelemetryTracer
        implements Tracer
{
    public static final OpenTelemetry OPEN_TELEMETRY = OpenTelemetryBuilder.build();
    public static final io.opentelemetry.api.trace.Tracer openTelemetryTracer = OPEN_TELEMETRY.getTracer("presto", "1.0.0");

    public final Map<String, Span> spanMap = new ConcurrentHashMap<String, Span>();
    public final Map<String, Span> recorderSpanMap = new LinkedHashMap<String, Span>();

    public OpenTelemetryTracer()
    {
        addPoint("Start tracing");
    }

    /**
     * Add short span representing single point
     * @param annotation represents name of span
     */
    @Override
    public void addPoint(String annotation)
    {
        String blockName = annotation;
        startBlock(blockName, "");
        endBlock(blockName, "");
    }

    /**
     * Create new span with Open Telemetry tracer
     * @param blockName name of span
     * @param annotation unused because annotation not supported by Open Telemetry spans
     */
    @Override
    public void startBlock(String blockName, String annotation)
    {
        if (spanMap.containsKey(blockName)) {
            throw new PrestoException(DISTRIBUTED_TRACING_ERROR, "Duplicated block inserted: " + blockName);
        }
        Span span = openTelemetryTracer.spanBuilder(blockName).startSpan();
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
     * @param annotation unused because annotation not supported by Open Telemetry spans
     */
    @Override
    public void endBlock(String blockName, String annotation)
    {
        if (!spanMap.containsKey(blockName)) {
            throw new PrestoException(DISTRIBUTED_TRACING_ERROR, "Trying to end a non-existing block: " + blockName);
        }
        spanMap.remove(blockName);
        synchronized (recorderSpanMap) {
            recorderSpanMap.get(blockName).end();
        }
    }

    @Override
    public void endTrace(String annotation)
    {
        addPoint(annotation);
    }

    @Override
    public String getTracerId()
    {
        return "open_telemetry_tracer_id";
    }
}
