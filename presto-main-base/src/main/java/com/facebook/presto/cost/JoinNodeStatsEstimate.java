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
package com.facebook.presto.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Double.NaN;

public class JoinNodeStatsEstimate
{
    private static final JoinNodeStatsEstimate UNKNOWN = new JoinNodeStatsEstimate(NaN, NaN, NaN, NaN);

    private final double nullJoinBuildKeyCount;
    private final double joinBuildKeyCount;
    private final double nullJoinProbeKeyCount;
    private final double joinProbeKeyCount;

    @JsonCreator
    public JoinNodeStatsEstimate(
            @JsonProperty("nullJoinBuildKeyCount") double nullJoinBuildKeyCount,
            @JsonProperty("joinBuildKeyCount") double joinBuildKeyCount,
            @JsonProperty("nullJoinProbeKeyCount") double nullJoinProbeKeyCount,
            @JsonProperty("joinProbeKeyCount") double joinProbeKeyCount)
    {
        this.nullJoinBuildKeyCount = nullJoinBuildKeyCount;
        this.joinBuildKeyCount = joinBuildKeyCount;
        this.nullJoinProbeKeyCount = nullJoinProbeKeyCount;
        this.joinProbeKeyCount = joinProbeKeyCount;
    }

    public static JoinNodeStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    @JsonProperty
    public double getNullJoinBuildKeyCount()
    {
        return nullJoinBuildKeyCount;
    }

    @JsonProperty
    public double getJoinBuildKeyCount()
    {
        return joinBuildKeyCount;
    }

    @JsonProperty
    public double getNullJoinProbeKeyCount()
    {
        return nullJoinProbeKeyCount;
    }

    @JsonProperty
    public double getJoinProbeKeyCount()
    {
        return joinProbeKeyCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nullJoinBuildKeyCount", nullJoinBuildKeyCount)
                .add("joinBuildKeyCount", joinBuildKeyCount)
                .add("nullJoinProbeKeyCount", nullJoinProbeKeyCount)
                .add("joinProbeKeyCount", joinProbeKeyCount)
                .toString();
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
        JoinNodeStatsEstimate that = (JoinNodeStatsEstimate) o;
        return Double.compare(nullJoinBuildKeyCount, that.nullJoinBuildKeyCount) == 0 &&
                Double.compare(joinBuildKeyCount, that.joinBuildKeyCount) == 0 &&
                Double.compare(nullJoinProbeKeyCount, that.nullJoinProbeKeyCount) == 0 &&
                Double.compare(joinProbeKeyCount, that.joinProbeKeyCount) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullJoinBuildKeyCount, joinBuildKeyCount, nullJoinProbeKeyCount, joinProbeKeyCount);
    }
}
