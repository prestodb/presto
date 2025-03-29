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

import com.facebook.presto.spi.tracing.TracerProvider;
import com.google.inject.Inject;

import javax.annotation.Nullable;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TracerProviderManager
{
    private final String tracerType;
    private final boolean systemTracingEnabled;
    @Nullable
    private TracerProvider tracerProvider;

    @Inject
    public TracerProviderManager(TracingConfig config)
    {
        requireNonNull(config, "config is null");

        this.tracerType = config.getTracerType();
        boolean enableDistributedTracing = config.getEnableDistributedTracing();
        TracingConfig.DistributedTracingMode distributedTracingMode = config.getDistributedTracingMode();
        this.systemTracingEnabled = enableDistributedTracing && distributedTracingMode.name().equalsIgnoreCase(TracingConfig.DistributedTracingMode.ALWAYS_TRACE.name());
    }

    public void addTracerProviderFactory(TracerProvider provider)
    {
        if (!tracerType.equals(provider.getTracerType())) {
            throw new IllegalArgumentException(
                    format(
                            "Plugin-configured tracer provider ('%s') does not match system-configured provider ('%s').",
                            provider.getName(),
                            tracerType));
        }
        if (systemTracingEnabled) {
            if (tracerProvider != null) {
                throw new IllegalArgumentException(format("Only a single plugin should set the tracer provider ('%s').", tracerProvider.getTracerType()));
            }
            tracerProvider = provider;
        }
    }

    public void loadTracerProvider()
    {
        if (tracerProvider != null) {
            return;
        }
        // open-telemetry plugin not used / tracer provider not specified or not matching system config / tracing disabled
        // Check if SimpleTracer is configured and tracing is enabled
        // Otherwise, use Noop implementation
        if (tracerType.equals(TracingConfig.TracerType.SIMPLE) && systemTracingEnabled) {
            tracerProvider = new SimpleTracerProvider();
        }
        else {
            tracerProvider = new NoopTracerProvider();
        }
    }

    public TracerProvider getTracerProvider()
    {
        if (tracerProvider != null) {
            return tracerProvider;
        }
        return new NoopTracerProvider();
    }
}
