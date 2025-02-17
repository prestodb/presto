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
import com.facebook.presto.spi.telemetry.BaseSpan;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

import java.util.Objects;

/**
 * ScopedSpan implements BaseSpan and holds the actual sdk span.
 * ScopedSpan implements AutoCloseable and can be used within a try-with-resources block.
 */
public final class ScopedSpan
        implements BaseSpan
{
    private final Span span;
    private final Scope scope;

    /**
     * Instantiates a new Scoped span.
     *
     * @param span the span
     */
    @SuppressWarnings("MustBeClosedChecker")
    public ScopedSpan(Span span)
    {
        this.span = span;
        this.scope = span.makeCurrent();
    }

    @Override
    public void close()
    {
        if (!TelemetryConfig.getTracingEnabled()) {
            return;
        }
        try {
            scope.close();
        }
        finally {
            if (!Objects.isNull(span)) {
                span.end();
            }
        }
    }

    @Override
    public void end()
    {
        span.end();
    }
}
