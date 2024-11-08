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

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.spi.telemetry.BaseSpan;
import com.facebook.presto.spi.telemetry.TelemetryFactory;
import com.facebook.presto.spi.telemetry.TelemetryTracing;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

/**
 * The type Telemetry tracing.
 */
public class TelemetryTracingImpl
        implements TelemetryTracing<BaseSpan, BaseSpan>
{
    /**
     * The constant NAME.
     */
    public static final String NAME = "otel";

    private static final TelemetryTracingImpl INSTANCE = new TelemetryTracingImpl();

    /**
     * The type Factory.
     */
    public static class Factory
            implements TelemetryFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public TelemetryTracing create()
        {
            return INSTANCE;
        }
    }

    @Override
    public void loadConfiguredOpenTelemetry()
    {
        return;
    }

    @Override
    public Runnable getCurrentContextWrap(Runnable runnable)
    {
        return runnable;
    }

    @Override
    public boolean isRecording()
    {
        return false;
    }

    @Override
    public Map<String, String> getHeadersMap(BaseSpan span)
    {
        return ImmutableMap.of();
    }

    @Override
    public void endSpanOnError(BaseSpan querySpan, Throwable throwable)
    {
        return;
    }

    @Override
    public void addEvent(BaseSpan span, String eventName)
    {
        return;
    }

    @Override
    public void addEvent(BaseSpan querySpan, String eventName, String eventState)
    {
        return;
    }

    @Override
    public void setAttributes(BaseSpan span, Map attributes)
    {
        return;
    }

    @Override
    public void recordException(BaseSpan querySpan, String message, RuntimeException runtimeException, ErrorCode errorCode)
    {
        return;
    }

    @Override
    public void setSuccess(BaseSpan querySpan)
    {
        return;
    }

    @Override
    public BaseSpan getInvalidSpan()
    {
        return getBaseSpan();
    }

    @Override
    public BaseSpan getRootSpan()
    {
        return getBaseSpan();
    }

    @Override
    public BaseSpan getSpan(String spanName)
    {
        return getBaseSpan();
    }

    @Override
    public BaseSpan getSpan(String traceParent, String spanName)
    {
        return getBaseSpan();
    }

    @Override
    public Optional<String> spanString(BaseSpan span)
    {
        return Optional.empty();
    }

    @Override
    public BaseSpan scopedSpan(String name, Boolean... skipSpan)
    {
        return getBaseSpan();
    }

    @Override
    public BaseSpan scopedSpan(BaseSpan span, Boolean... skipSpan)
    {
        return getBaseSpan();
    }

    @Override
    public BaseSpan scopedSpan(BaseSpan parentSpan, String spanName, Boolean... skipSpan)
    {
        return getBaseSpan();
    }

    @Override
    public BaseSpan scopedSpan(String spanName, Map attributes, Boolean... skipSpan)
    {
        return getBaseSpan();
    }

    @Override
    public BaseSpan scopedSpan(BaseSpan parentSpan, String spanName, Map attributes, Boolean... skipSpan)
    {
        return getBaseSpan();
    }

    @Override
    public BaseSpan getSpan(BaseSpan parentSpan, String spanName, Map attributes)
    {
        return getBaseSpan();
    }

    private BaseSpan getBaseSpan()
    {
        return new BaseSpan()
        {
        };
    }
}
