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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The resolved plan-time execution properties for a single RPC function call, produced by
 * {@link RpcExecutionPolicy#translateIntent} and applied by the optimizer to the
 * {@link RPCNode}.
 *
 * Immutable and built via {@link #builder}. It carries the resolved {@code streamingMode} and a
 * coordinator-only recommended task count.
 *
 * Scope note: this is the plan-time execution axis only. Runtime concerns (error policy,
 * deadlines, model-quality/cost) ride query config read by the worker and do NOT belong here.
 */
public class RpcExecutionProperties
{
    private final RPCNode.StreamingMode streamingMode;
    private final int recommendedTaskCount;

    private RpcExecutionProperties(RPCNode.StreamingMode streamingMode, int recommendedTaskCount)
    {
        this.streamingMode = requireNonNull(streamingMode, "streamingMode is null");
        checkArgument(recommendedTaskCount >= 0, "recommendedTaskCount must be non-negative: %s", recommendedTaskCount);
        this.recommendedTaskCount = recommendedTaskCount;
    }

    /**
     * The resolved streaming mode. Must be concrete (PER_ROW or BATCH); never AUTOMATIC.
     */
    public RPCNode.StreamingMode getStreamingMode()
    {
        return streamingMode;
    }

    /**
     * Recommended remote task count; zero delegates to default planning.
     */
    public int getRecommendedTaskCount()
    {
        return recommendedTaskCount;
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
        private int recommendedTaskCount;

        public Builder setStreamingMode(RPCNode.StreamingMode streamingMode)
        {
            this.streamingMode = streamingMode;
            return this;
        }

        public Builder setRecommendedTaskCount(int recommendedTaskCount)
        {
            this.recommendedTaskCount = recommendedTaskCount;
            return this;
        }

        public RpcExecutionProperties build()
        {
            return new RpcExecutionProperties(streamingMode, recommendedTaskCount);
        }
    }
}
