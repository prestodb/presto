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
    private static final JoinNodeStatistics EMPTY = new JoinNodeStatistics(Estimate.unknown(), Estimate.unknown());
    // Number of input rows from build side of a join which has at least one join column to be NULL
    private final Estimate nullJoinBuildKeyCount;
    // Number of input rows from build side of a join
    private final Estimate joinBuildKeyCount;

    @JsonCreator
    @ThriftConstructor
    public JoinNodeStatistics(
            @JsonProperty("nullJoinBuildKeyCount") Estimate nullJoinBuildKeyCount,
            @JsonProperty("joinBuildKeyCount") Estimate joinBuildKeyCount)
    {
        this.nullJoinBuildKeyCount = requireNonNull(nullJoinBuildKeyCount, "nullJoinBuildKeyCount is null");
        this.joinBuildKeyCount = requireNonNull(joinBuildKeyCount, "joinBuildKeyCount is null");
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
        return Objects.equals(nullJoinBuildKeyCount, that.nullJoinBuildKeyCount) && Objects.equals(joinBuildKeyCount, that.joinBuildKeyCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullJoinBuildKeyCount, joinBuildKeyCount);
    }

    @Override
    public String toString()
    {
        return "JoinNodeStatistics{" +
                "nullJoinBuildKeyCount=" + nullJoinBuildKeyCount +
                ", joinBuildKeyCount=" + joinBuildKeyCount +
                '}';
    }
}
