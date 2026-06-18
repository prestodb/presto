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

import com.facebook.presto.spi.tracing.TracerHandle;

public class OpenTelemetryTracerHandle
        implements TracerHandle
{
    private final String traceToken;
    private final String contextPropagator;
    private final String propagatedContext;
    private final String baggage;

    public OpenTelemetryTracerHandle(String traceToken, String contextPropagator, String propagatedContext, String baggage)
    {
        this.traceToken = traceToken;
        this.contextPropagator = contextPropagator;
        this.propagatedContext = propagatedContext;
        this.baggage = baggage;
    }

    @Override
    public String getTraceToken()
    {
        return traceToken;
    }

    public String getContextPropagator()
    {
        return contextPropagator;
    }

    public String getPropagatedContext()
    {
        return propagatedContext;
    }

    public String getBaggage()
    {
        return baggage;
    }
}
