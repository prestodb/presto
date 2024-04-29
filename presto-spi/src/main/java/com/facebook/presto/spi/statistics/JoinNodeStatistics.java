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
package com.facebook.presto.spi.statistics;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class JoinNodeStatistics
{
    private static final JoinNodeStatistics EMPTY = new JoinNodeStatistics(Estimate.unknown(), Estimate.unknown(), Estimate.unknown(), Estimate.unknown());
    // Number of input rows from build side of a join which has at least one join column to be NULL
    private final Estimate nullJoinBuildKeyCount;
    // Number of input rows from build side of a join
    private final Estimate joinBuildKeyCount;
    // Number of input rows from probe side of a join which has at least one join column to be NULL
    private final Estimate nullJoinProbeKeyCount;
    // Number of input rows from probe side of a join
    private final Estimate joinProbeKeyCount;

    @JsonCreator
    @ThriftConstructor
    public JoinNodeStatistics(
            @JsonProperty("nullJoinBuildKeyCount") Estimate nullJoinBuildKeyCount,
            @JsonProperty("joinBuildKeyCount") Estimate joinBuildKeyCount,
            @JsonProperty("nullJoinProbeKeyCount") Estimate nullJoinProbeKeyCount,
            @JsonProperty("joinProbeKeyCount") Estimate joinProbeKeyCount)
    {
        this.nullJoinBuildKeyCount = requireNonNull(nullJoinBuildKeyCount, "nullJoinBuildKeyCount is null");
        this.joinBuildKeyCount = requireNonNull(joinBuildKeyCount, "joinBuildKeyCount is null");
        this.nullJoinProbeKeyCount = requireNonNull(nullJoinProbeKeyCount, "nullJoinProbeKeyCount is null");
        this.joinProbeKeyCount = requireNonNull(joinProbeKeyCount, "joinProbeKeyCount is null");
    }

    public static JoinNodeStatistics empty()
    {
        return EMPTY;
    }

    public boolean isEmpty()
    {
        return this.equals(empty());
    }

    @JsonProperty
    @ThriftField(1)
    public Estimate getNullJoinBuildKeyCount()
    {
        return nullJoinBuildKeyCount;
    }

    @JsonProperty
    @ThriftField(2)
    public Estimate getJoinBuildKeyCount()
    {
        return joinBuildKeyCount;
    }

    @JsonProperty
    @ThriftField(3)
    public Estimate getNullJoinProbeKeyCount()
    {
        return nullJoinProbeKeyCount;
    }

    @JsonProperty
    @ThriftField(4)
    public Estimate getJoinProbeKeyCount()
    {
        return joinProbeKeyCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinNodeStatistics that = (JoinNodeStatistics) o;
        return Objects.equals(nullJoinBuildKeyCount, that.nullJoinBuildKeyCount) && Objects.equals(joinBuildKeyCount, that.joinBuildKeyCount)
                && Objects.equals(nullJoinProbeKeyCount, that.nullJoinProbeKeyCount) && Objects.equals(joinProbeKeyCount, that.joinProbeKeyCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullJoinBuildKeyCount, joinBuildKeyCount, nullJoinProbeKeyCount, joinProbeKeyCount);
    }

    @Override
    public String toString()
    {
        return "JoinNodeStatistics{" +
                "nullJoinBuildKeyCount=" + nullJoinBuildKeyCount +
                ", joinBuildKeyCount=" + joinBuildKeyCount +
                ", nullJoinProbeKeyCount=" + nullJoinProbeKeyCount +
                ", joinProbeKeyCount=" + joinProbeKeyCount +
                '}';
    }
}
