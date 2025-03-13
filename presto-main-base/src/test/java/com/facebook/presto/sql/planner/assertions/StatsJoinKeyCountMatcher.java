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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;

public class StatsJoinKeyCountMatcher
        implements Matcher
{
    private final double expectedJoinBuildKeyCount;
    private final double expectedNullJoinBuildKeyCount;
    private final double expectedJoinProbeKeyCount;
    private final double expectedNullJoinProbeKeyCount;

    StatsJoinKeyCountMatcher(double expectedJoinBuildKeyCount, double expectedNullJoinBuildKeyCount, double expectedJoinProbeKeyCount, double expectedNullJoinProbeKeyCount)
    {
        this.expectedJoinBuildKeyCount = expectedJoinBuildKeyCount;
        this.expectedNullJoinBuildKeyCount = expectedNullJoinBuildKeyCount;
        this.expectedJoinProbeKeyCount = expectedJoinProbeKeyCount;
        this.expectedNullJoinProbeKeyCount = expectedNullJoinProbeKeyCount;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return true;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        return new MatchResult(Double.compare(stats.getStats(node).getJoinNodeStatsEstimate().getJoinBuildKeyCount(), expectedJoinBuildKeyCount) == 0
                && Double.compare(stats.getStats(node).getJoinNodeStatsEstimate().getNullJoinBuildKeyCount(), expectedNullJoinBuildKeyCount) == 0
                && Double.compare(stats.getStats(node).getJoinNodeStatsEstimate().getJoinProbeKeyCount(), expectedJoinProbeKeyCount) == 0
                && Double.compare(stats.getStats(node).getJoinNodeStatsEstimate().getNullJoinProbeKeyCount(), expectedNullJoinProbeKeyCount) == 0);
    }

    @Override
    public String toString()
    {
        return "expectedJoinBuildKeyCount(" + expectedJoinBuildKeyCount + ")" + " expectedNullJoinBuildKeyCount(" + expectedNullJoinBuildKeyCount + ")"
                + " expectedNullJoinProbeKeyCount(" + expectedNullJoinProbeKeyCount + ")" + " expectedNullJoinProbeKeyCount(" + expectedNullJoinProbeKeyCount + ")";
    }
}
