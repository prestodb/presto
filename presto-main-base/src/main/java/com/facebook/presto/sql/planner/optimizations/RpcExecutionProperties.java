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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.plan.RPCNode;

import static java.util.Objects.requireNonNull;

/**
 * The resolved plan-time execution properties for a single RPC function call, produced by
 * {@link RpcExecutionPolicy#translateIntent} and applied by the optimizer to the
 * {@link RPCNode}.
 *
 * Immutable and built via {@link #builder}. Today it carries the resolved {@code streamingMode};
 * additional plan-time properties (e.g. dispatch parallelism / batch size) can be added here as a
 * new builder setter + getter without changing the policy method signature.
 *
 * Scope note: this is the plan-time execution axis only. Runtime concerns (error policy,
 * deadlines, model-quality/cost) ride query config read by the worker and do NOT belong here.
 */
public class RpcExecutionProperties
{
    private final RPCNode.StreamingMode streamingMode;

    private RpcExecutionProperties(RPCNode.StreamingMode streamingMode)
    {
        this.streamingMode = requireNonNull(streamingMode, "streamingMode is null");
    }

    /**
     * The resolved streaming mode. Must be concrete (PER_ROW or BATCH); never AUTOMATIC.
     */
    public RPCNode.StreamingMode getStreamingMode()
    {
        return streamingMode;
    }

    /**
     * Convenience for the common case: just a resolved streaming mode.
     */
    public static RpcExecutionProperties of(RPCNode.StreamingMode streamingMode)
    {
        return builder().setStreamingMode(streamingMode).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private RPCNode.StreamingMode streamingMode;

        public Builder setStreamingMode(RPCNode.StreamingMode streamingMode)
        {
            this.streamingMode = streamingMode;
            return this;
        }

        public RpcExecutionProperties build()
        {
            return new RpcExecutionProperties(streamingMode);
        }
    }
}
