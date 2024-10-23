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

import static com.google.common.base.Verify.verify;

public class ApproximateStatsOutputRowCountMatcher
        implements Matcher
{
    private final double expectedOutputRowCount;
    private final double error;

    ApproximateStatsOutputRowCountMatcher(double expectedOutputRowCount, double error)
    {
        verify(error >= 0.0, "error must be >= 0.0");
        verify(expectedOutputRowCount >= 0.0, "expectedOutputRowCount must be >= 0.0");
        this.expectedOutputRowCount = expectedOutputRowCount;
        this.error = error;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return true;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        return new MatchResult(Math.abs(stats.getStats(node).getOutputRowCount() - expectedOutputRowCount) < error);
    }

    @Override
    public String toString()
    {
        return "approximateExpectedOutputRowCount(" + expectedOutputRowCount + ", " + error + ")";
    }
}
