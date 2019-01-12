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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Patterns;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class AssignUniqueIdStatsRule
        implements ComposableStatsCalculator.Rule<AssignUniqueId>
{
    private static final Pattern<AssignUniqueId> PATTERN = Patterns.assignUniqueId();

    @Override
    public Pattern<AssignUniqueId> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(AssignUniqueId assignUniqueId, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(assignUniqueId.getSource());
        return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats)
                .addSymbolStatistics(assignUniqueId.getIdColumn(), SymbolStatsEstimate.builder()
                        .setDistinctValuesCount(sourceStats.getOutputRowCount())
                        .setNullsFraction(0.0)
                        .setAverageRowSize(BIGINT.getFixedSize())
                        .build())
                .build());
    }
}
