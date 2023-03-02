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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.OptionalLong;

@ThriftStruct
public class TaskShutdownStats
{
    private final Optional<String> outputBufferState;
    private final Optional<String> pendingRunningSplitState;
    private final OptionalLong outputBufferWaitTime;
    private final OptionalLong pendingRunningSplitStateTime;

    @JsonCreator
    @ThriftConstructor
    public TaskShutdownStats(
            @JsonProperty("pendingRunningSplitState") Optional<String> pendingRunningSplitState,
            @JsonProperty("pendingRunningSplitStateTime") OptionalLong pendingRunningSplitStateTime,
            @JsonProperty("outputBufferState") Optional<String> outputBufferState,
            @JsonProperty("outputBufferWaitTime") OptionalLong outputBufferWaitTime)
    {
        this.pendingRunningSplitState = pendingRunningSplitState;
        this.pendingRunningSplitStateTime = pendingRunningSplitStateTime;
        this.outputBufferState = outputBufferState;
        this.outputBufferWaitTime = outputBufferWaitTime;
    }

    @JsonProperty
    @ThriftField(1)
    public Optional<String> getOutputBufferState()
    {
        return outputBufferState;
    }

    @JsonProperty
    @ThriftField(2)
    public Optional<String> getPendingRunningSplitState()
    {
        return pendingRunningSplitState;
    }

    @JsonProperty
    @ThriftField(3)
    public OptionalLong getOutputBufferWaitTime()
    {
        return outputBufferWaitTime;
    }

    @JsonProperty
    @ThriftField(4)
    public OptionalLong getPendingRunningSplitStateTime()
    {
        return pendingRunningSplitStateTime;
    }
}
