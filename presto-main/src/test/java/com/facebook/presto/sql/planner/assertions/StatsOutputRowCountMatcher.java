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
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;

public class StatsOutputRowCountMatcher
        implements Matcher
{
    private final boolean exactMatch;
    private final double expectedOutputRowCount;
    private final String expectedSourceInfo;

    StatsOutputRowCountMatcher(double expectedOutputRowCount)
    {
        this(expectedOutputRowCount, true, null);
    }

    StatsOutputRowCountMatcher(double expectedOutputRowCount, String expectedSourceInfo)
    {
        this(expectedOutputRowCount, true, expectedSourceInfo);
    }

    StatsOutputRowCountMatcher(boolean exactMatch, String expectedSourceInfo)
    {
        this(Double.NaN, exactMatch, expectedSourceInfo);
    }

    StatsOutputRowCountMatcher(double expectedOutputRowCount, boolean exactMatch, String expectedSourceInfo)
    {
        this.exactMatch = exactMatch;
        this.expectedOutputRowCount = expectedOutputRowCount;
        this.expectedSourceInfo = expectedSourceInfo;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return true;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        PlanNodeStatsEstimate estimate = stats.getStats(node);
        if (this.expectedSourceInfo != null && !this.expectedSourceInfo.equals(estimate.getSourceInfo().getSourceInfoName())) {
            return new MatchResult(false);
        }
        if (!exactMatch) {
            return new MatchResult(true);
        }
        return new MatchResult(Double.compare(estimate.getOutputRowCount(), expectedOutputRowCount) == 0);
    }

    @Override
    public String toString()
    {
        return new StringBuilder("expectedOutputRowCount(")
                .append(exactMatch ? expectedOutputRowCount : "not exact")
                .append(")")
                .append(expectedSourceInfo == null ? "" : "[" + expectedSourceInfo + "]")
                .toString();
    }
}
