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
package com.facebook.presto.operator;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class NativeExecutionInfo
        implements Mergeable<NativeExecutionInfo>, OperatorInfo
{
    /// Runtime statistics received from native process.
    private final List<TaskStats> taskStats;

    @JsonCreator
    @ThriftConstructor
    public NativeExecutionInfo(@JsonProperty("taskStats") List<TaskStats> taskStats)
    {
        this.taskStats = requireNonNull(taskStats);
    }

    @JsonProperty
    @ThriftField(1)
    public List<TaskStats> getTaskStats()
    {
        return taskStats;
    }

    @Override
    public NativeExecutionInfo mergeWith(NativeExecutionInfo other)
    {
        return new NativeExecutionInfo(new ImmutableList.Builder<TaskStats>()
            .addAll(taskStats)
            .addAll(other.taskStats)
            .build());
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }
}
