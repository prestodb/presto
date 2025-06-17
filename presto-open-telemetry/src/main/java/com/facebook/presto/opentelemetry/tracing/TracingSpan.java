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

import com.facebook.presto.spi.tracing.BaseSpan;
import io.opentelemetry.api.trace.Span;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * TracingSpan implements BaseSpan and holds the actual sdk span.
 * TracingSpan can be use where we know the scope and can manually start and end.
 */
public class TracingSpan
        implements BaseSpan
{
    private final Span span;

    /**
     * Instantiates a new Tracing span.
     *
     * @param span the span
     */
    public TracingSpan(Span span)
    {
        this.span = span;
    }

    /**
     * Gets span.
     *
     * @return the span
     */
    public Span getSpan()
    {
        return span;
    }

    /**
     * Sets attribute.
     *
     * @param key   the key
     * @param value the value
     */
    public void setAttribute(String key, String value)
    {
        span.setAttribute(key, value);
    }

    @Override
    public void end()
    {
        span.end();
    }

    /**
     * Returns the span info as string.
     * @return Optional<String>
     */
    public Optional<String> spanString()
    {
        return Optional.ofNullable(span)
                .filter(s -> span.getSpanContext().isValid())
                .map(s -> toStringHelper("Span")
                        .add("spanId", span.getSpanContext().getSpanId())
                        .add("traceId", span.getSpanContext().getTraceId())
                        .toString());
    }
}
