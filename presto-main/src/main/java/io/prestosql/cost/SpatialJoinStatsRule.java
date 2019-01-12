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
package io.prestosql.cost;

import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.SpatialJoinNode;

import java.util.Optional;

import static io.prestosql.sql.planner.plan.Patterns.spatialJoin;
import static java.util.Objects.requireNonNull;

public class SpatialJoinStatsRule
        extends SimpleStatsRule<SpatialJoinNode>
{
    private static final Pattern<SpatialJoinNode> PATTERN = spatialJoin();

    private final FilterStatsCalculator statsCalculator;

    public SpatialJoinStatsRule(FilterStatsCalculator statsCalculator, StatsNormalizer normalizer)
    {
        super(normalizer);
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(SpatialJoinNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate leftStats = sourceStats.getStats(node.getLeft());
        PlanNodeStatsEstimate rightStats = sourceStats.getStats(node.getRight());
        PlanNodeStatsEstimate crossJoinStats = crossJoinStats(node, leftStats, rightStats);

        switch (node.getType()) {
            case INNER:
                return Optional.of(statsCalculator.filterStats(crossJoinStats, node.getFilter(), session, types));
            case LEFT:
                return Optional.of(PlanNodeStatsEstimate.unknown());
            default:
                throw new IllegalArgumentException("Unknown spatial join type: " + node.getType());
        }
    }

    private PlanNodeStatsEstimate crossJoinStats(SpatialJoinNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(leftStats.getOutputRowCount() * rightStats.getOutputRowCount());

        node.getLeft().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, leftStats.getSymbolStatistics(symbol)));
        node.getRight().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, rightStats.getSymbolStatistics(symbol)));

        return builder.build();
    }

    @Override
    public Pattern<SpatialJoinNode> getPattern()
    {
        return PATTERN;
    }
}
