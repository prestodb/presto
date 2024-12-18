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
import com.facebook.presto.telemetry.TelemetryManager;
import com.google.errorprone.annotations.MustBeClosed;
import io.opentelemetry.context.Scope;

import java.util.Objects;

public final class ScopedSpan
        implements AutoCloseable
{
    private final TracingSpan span;
    private final Scope scope;

    @SuppressWarnings("MustBeClosedChecker")
    private ScopedSpan(TracingSpan span)
    {
        this.span = span;
        this.scope = span.getSpan().makeCurrent();
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

    /**
     * starts a basic span and passes it to overloaded method. This method is used for creating basic spans with no attributes.
     * @param name name of span to be created
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return
     */
    @MustBeClosed
    public static ScopedSpan scopedSpan(String name, Boolean... skipSpan)
    {
        if (!TelemetryConfig.getTracingEnabled() || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        return scopedSpan(new TracingSpan(TelemetryManager.getTracer().spanBuilder(name).startSpan()));
    }

    /**
     * creates a ScopedSpan with the current span. This method is used when we manually create spans in the classes and
     * set attributes to them before passing to the Scopedspan.
     * @param span created span instance
     * @param skipSpan optional parameter to implement span sampling by skipping the current span export
     * @return
     */
    @MustBeClosed
    public static ScopedSpan scopedSpan(TracingSpan span, Boolean... skipSpan)
    {
        if ((!TelemetryConfig.getTracingEnabled() || Objects.isNull(span)) || (skipSpan.length > 0 && TelemetryConfig.getSpanSampling())) {
            return null;
        }
        return new ScopedSpan(span);
    }
}
