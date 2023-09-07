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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Double.NaN;

public class JoinNodeSpecificStatsEstimate
{
    private static final JoinNodeSpecificStatsEstimate UNKNOWN = new JoinNodeSpecificStatsEstimate(NaN, NaN);

    private final double nullJoinBuildKeyCount;
    private final double joinBuildKeyCount;

    public JoinNodeSpecificStatsEstimate(double nullJoinBuildKeyCount, double joinBuildKeyCount)
    {
        this.nullJoinBuildKeyCount = nullJoinBuildKeyCount;
        this.joinBuildKeyCount = joinBuildKeyCount;
    }

    public static JoinNodeSpecificStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    public double getNullJoinBuildKeyCount()
    {
        return nullJoinBuildKeyCount;
    }

    public double getJoinBuildKeyCount()
    {
        return joinBuildKeyCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nullJoinBuildKeyCount", nullJoinBuildKeyCount)
                .add("joinBuildKeyCount", joinBuildKeyCount)
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
        JoinNodeSpecificStatsEstimate that = (JoinNodeSpecificStatsEstimate) o;
        return Double.compare(nullJoinBuildKeyCount, that.nullJoinBuildKeyCount) == 0 &&
                Double.compare(joinBuildKeyCount, that.joinBuildKeyCount) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullJoinBuildKeyCount, joinBuildKeyCount);
    }
}
