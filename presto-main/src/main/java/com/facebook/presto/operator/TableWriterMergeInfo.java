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

import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TableWriterMergeInfo
        implements Mergeable<TableWriterMergeInfo>, OperatorInfo
{
    private final Duration statisticsWallTime;
    private final Duration statisticsCpuTime;

    @JsonCreator
    public TableWriterMergeInfo(
            @JsonProperty("statisticsWallTime") Duration statisticsWallTime,
            @JsonProperty("statisticsCpuTime") Duration statisticsCpuTime)
    {
        this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
        this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
    }

    @JsonProperty
    public Duration getStatisticsWallTime()
    {
        return statisticsWallTime;
    }

    @JsonProperty
    public Duration getStatisticsCpuTime()
    {
        return statisticsCpuTime;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statisticsWallTime", statisticsWallTime)
                .add("statisticsCpuTime", statisticsCpuTime)
                .toString();
    }

    @Override
    public TableWriterMergeInfo mergeWith(TableWriterMergeInfo other)
    {
        return new TableWriterMergeInfo(
                new Duration(this.statisticsWallTime.toMillis() + other.statisticsWallTime.toMillis(), MILLISECONDS),
                new Duration(this.statisticsCpuTime.toMillis() + other.statisticsCpuTime.toMillis(), MILLISECONDS));
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }
}
