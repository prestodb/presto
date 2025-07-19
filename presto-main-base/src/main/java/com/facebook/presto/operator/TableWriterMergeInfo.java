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

import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftStruct
public class TableWriterMergeInfo
        implements Mergeable<TableWriterMergeInfo>, OperatorInfo
{
    private final long statisticsWallTimeInNanos;
    private final long statisticsCpuTimeInNanos;

    @JsonCreator
    @ThriftConstructor
    public TableWriterMergeInfo(
            @JsonProperty("statisticsWallTimeInNanos") long statisticsWallTimeInNanos,
            @JsonProperty("statisticsCpuTimeInNanos") long statisticsCpuTimeInNanos)
    {
        this.statisticsWallTimeInNanos = statisticsWallTimeInNanos;
        this.statisticsCpuTimeInNanos = statisticsCpuTimeInNanos;
    }

    @JsonProperty
    @ThriftField(1)
    public long getStatisticsWallTimeInNanos()
    {
        return statisticsWallTimeInNanos;
    }

    @JsonProperty
    @ThriftField(2)
    public long getStatisticsCpuTimeInNanos()
    {
        return statisticsCpuTimeInNanos;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statisticsWallTimeInNanos", statisticsWallTimeInNanos)
                .add("statisticsCpuTimeInNanos", statisticsCpuTimeInNanos)
                .toString();
    }

    @Override
    public TableWriterMergeInfo mergeWith(TableWriterMergeInfo other)
    {
        return new TableWriterMergeInfo(
                this.statisticsWallTimeInNanos + other.statisticsWallTimeInNanos,
                this.statisticsCpuTimeInNanos + other.statisticsCpuTimeInNanos);
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }
}
