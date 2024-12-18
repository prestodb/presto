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
package com.facebook.presto.opentelemetry.tracing;

import com.facebook.presto.common.TelemetryConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;

import java.util.Objects;

public class TracingSpan
{
    private final Span span;
/*    public static TracingSpan getInvalid()
    {
        return (TracingSpan) Span.getInvalid();
    }

    public static TracingSpan current()
    {
        return (TracingSpan) Span.current();
    }

    public static TracingSpan fromContext(Context context)
    {
        return (TracingSpan) Span.fromContext(context);
    }*/

    public TracingSpan(Span span)
    {
        this.span = span;
    }

    public Span getSpan()
    {
        return span;
    }

    public static TracingSpan getInvalid()
    {
        return new TracingSpan(Span.getInvalid());
    }

    public static TracingSpan current()
    {
        return new TracingSpan(Span.current());
    }

    public static TracingSpan fromContext(Context context)
    {
        return new TracingSpan(Span.fromContext(context));
    }

    public TracingSpan setAttribute(String key, String value)
    {
        return new TracingSpan(span.setAttribute(key, value));
    }

    public TracingSpan setAttribute(String key, long value)
    {
        return new TracingSpan(span.setAttribute(key, value));
    }

    public boolean isRecording()
    {
        return span.isRecording();
    }

    public void end()
    {
        span.end();
    }

    public static void addEvent(TracingSpan span, String eventName)
    {
        if (TelemetryConfig.getTracingEnabled() && Objects.nonNull(span)) {
            span.getSpan().addEvent(eventName);
        }
    }
}
