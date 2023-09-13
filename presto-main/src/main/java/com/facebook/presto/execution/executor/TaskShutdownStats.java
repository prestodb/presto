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
package com.facebook.presto.execution.executor;

import com.facebook.drift.annotations.ThriftStruct;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class TaskShutdownStats
{
    private final OptionalLong splitsToBeRetried;
    private final String shuttingdownNode;
    private final Map<String, Long> bufferStageToTime;
    private final Map<String, Long> splitWaitStageToTime;
    private final Map<String, Long> outputBufferInfoValues;

    private TaskShutdownStats(Map<String, Long> bufferStageToTime, Map<String, Long> splitWaitStageToTime, OptionalLong splitsToBeRetried, String shuttingdownNode, Map<String, Long> outputBufferInfoValues)
    {
        this.bufferStageToTime = requireNonNull(bufferStageToTime, "bufferStageToTime is null");
        this.splitWaitStageToTime = requireNonNull(splitWaitStageToTime, "splitWaitStageToTime is null");
        this.splitsToBeRetried = requireNonNull(splitsToBeRetried, "splitsToBeRetried is null");
        this.shuttingdownNode = requireNonNull(shuttingdownNode, "shuttingdownNode is null");
        this.outputBufferInfoValues = ImmutableMap.copyOf(requireNonNull(outputBufferInfoValues, "outputBufferStates is null"));
    }

    public static Builder builder(String shuttingdownNode)
    {
        return new Builder(shuttingdownNode);
    }

    public static final class Builder
    {
        private final Map<String, Long> bufferStageToTime = new HashMap<>();
        private final Map<String, Long> splitWaitStageToTime = new HashMap<>();
        private OptionalLong splitsToBeRetried = OptionalLong.empty();
        private final String shuttingdownNode;
        private final Map<String, Long> outputBufferStates = new HashMap<>();

        private Builder(String shuttingdownNode)
        {
            this.shuttingdownNode = requireNonNull(shuttingdownNode, "shuttingdownNode is null");
        }

        public Builder setOutputBufferStage(String outputBufferState, long durationNanos)
        {
            this.bufferStageToTime.put(outputBufferState, durationNanos);
            return this;
        }

        public Builder setPendingRunningSplitState(String splitWaitState, long durationNanos)
        {
            this.splitWaitStageToTime.put(splitWaitState, durationNanos);
            return this;
        }

        public Builder setSplitsToBeRetried(long splitsToBeRetried)
        {
            this.splitsToBeRetried = OptionalLong.of(splitsToBeRetried);
            return this;
        }

        public Builder setOutputBufferInfo(String outputBufferInfoKey, Long outputBufferInfoValue)
        {
            this.outputBufferStates.put(outputBufferInfoKey, outputBufferInfoValue);
            return this;
        }

        public TaskShutdownStats build()
        {
            return new TaskShutdownStats(
                    bufferStageToTime,
                    splitWaitStageToTime,
                    splitsToBeRetried,
                    shuttingdownNode,
                    outputBufferStates);
        }
    }

    public Map<String, Long> getBufferStageToTime()
    {
        return bufferStageToTime;
    }

    public Map<String, Long> getSplitWaitStageToTime()
    {
        return splitWaitStageToTime;
    }

    public OptionalLong getSplitsToBeRetried()
    {
        return splitsToBeRetried;
    }

    public String getShuttingdownNode()
    {
        return shuttingdownNode;
    }

    public Map<String, Long> getOutputBufferInfoValues()
    {
        return outputBufferInfoValues;
    }
}
