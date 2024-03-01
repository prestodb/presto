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
import com.facebook.presto.cost.ComposableStatsCalculator.Rule;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.UnnestNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.unnest;

public class UnnestStatsRule
        implements Rule<UnnestNode>
{
    private static final int UPPER_BOUND_ROW_COUNT_FOR_ESTIMATION = 1;

    @Override
    public Pattern<UnnestNode> getPattern()
    {
        return unnest();
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(UnnestNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        PlanNodeStatsEstimate.Builder calculatedStats = PlanNodeStatsEstimate.builder();
        if (sourceStats.getOutputRowCount() > UPPER_BOUND_ROW_COUNT_FOR_ESTIMATION) {
            return Optional.empty();
        }

        // Since we don't have stats for cardinality about the unnest column, we cannot estimate the row count.
        // However, when the source row count is low, the error would not matter much in query optimization.
        // Thus we'd still populate the inaccurate numbers just so stats are populated to enable optimization
        // potential.
        calculatedStats.setOutputRowCount(sourceStats.getOutputRowCount());
        for (VariableReferenceExpression variable : node.getReplicateVariables()) {
            calculatedStats.addVariableStatistics(variable, sourceStats.getVariableStatistics(variable));
        }
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getUnnestVariables().entrySet()) {
            List<VariableReferenceExpression> unnestToVariables = entry.getValue();
            VariableStatsEstimate stats = sourceStats.getVariableStatistics(entry.getKey());
            for (VariableReferenceExpression variable : unnestToVariables) {
                // This is a very conservative way on estimating stats after unnest. We assume each symbol
                // after unnest would have as much data as the symbol before unnest. This would over
                // estimate, which are more likely to mean we'd loose an optimization opportunity, but at
                // least it won't cause false optimizations.
                calculatedStats.addVariableStatistics(
                        variable,
                        VariableStatsEstimate.builder()
                                .setAverageRowSize(stats.getAverageRowSize())
                                .build());
            }
        }
        if (node.getOrdinalityVariable().isPresent()) {
            calculatedStats.addVariableStatistics(
                    node.getOrdinalityVariable().get(),
                    VariableStatsEstimate.builder()
                        .setLowValue(0)
                        .setNullsFraction(0)
                        .build());
        }
        return Optional.of(calculatedStats.build());
    }
}
