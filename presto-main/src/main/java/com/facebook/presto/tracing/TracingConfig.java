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

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.DISTRIBUTED_TRACING_ERROR;

public class TracingConfig
{
    public static class TracerType
    {
        public static final String NOOP = "noop";
        public static final String SIMPLE = "simple";
    }

    public enum DistributedTracingMode
    {
        NO_TRACE,
        ALWAYS_TRACE,
        SAMPLE_BASED
    }

    private String tracerType = TracerType.NOOP;
    private DistributedTracingMode tracingMode = DistributedTracingMode.NO_TRACE;
    private boolean enableDistributedTracing;

    public String getTracerType()
    {
        return this.tracerType;
    }

    @Config("tracing.tracer-type")
    public TracingConfig setTracerType(String tracerType)
    {
        this.tracerType = tracerType;
        return this;
    }

    public boolean getEnableDistributedTracing()
    {
        return enableDistributedTracing;
    }

    @Config("tracing.enable-distributed-tracing")
    public TracingConfig setEnableDistributedTracing(boolean enableDistributedTracing)
    {
        this.enableDistributedTracing = enableDistributedTracing;
        return this;
    }

    public DistributedTracingMode getDistributedTracingMode()
    {
        return tracingMode;
    }

    @Config("tracing.distributed-tracing-mode")
    public TracingConfig setDistributedTracingMode(String tracingMode)
    {
        if (tracingMode.equalsIgnoreCase(DistributedTracingMode.SAMPLE_BASED.name())) {
            throw new PrestoException(
                    DISTRIBUTED_TRACING_ERROR,
                    "SAMPLE_BASED Tracing Mode is currently not supported.");
        }
        this.tracingMode = DistributedTracingMode.valueOf(tracingMode.toUpperCase());
        return this;
    }
}
