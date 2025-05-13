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

import com.facebook.presto.spi.tracing.Tracer;
import com.facebook.presto.spi.tracing.TracerHandle;
import com.facebook.presto.spi.tracing.TracerProvider;
import com.google.inject.Inject;

import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRACE_TOKEN;

public class SimpleTracerProvider
        implements TracerProvider
{
    @Inject
    public SimpleTracerProvider()
    {
    }

    @Override
    public String getName()
    {
        return "Simple tracer provider";
    }

    @Override
    public String getTracerType()
    {
        return TracingConfig.TracerType.SIMPLE;
    }

    @Override
    public Function<Map<String, String>, TracerHandle> getHandleGenerator()
    {
        return headers -> new SimpleTracerHandle(headers.get(PRESTO_TRACE_TOKEN));
    }

    @Override
    public Tracer getNewTracer(TracerHandle handle)
    {
        SimpleTracerHandle tracerHandle = (SimpleTracerHandle) handle;
        return new SimpleTracer(tracerHandle.getTraceToken());
    }
}
