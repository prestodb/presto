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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.plan.AggregationNode.Step.INTERMEDIATE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.FACT;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class AggregationStatsRule
        extends SimpleStatsRule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    public AggregationStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(AggregationNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        if (node.getGroupingSetCount() != 1) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate estimate;

        if (node.getStep() == PARTIAL || node.getStep() == INTERMEDIATE) {
            estimate = partialGroupBy(
                    statsProvider.getStats(node.getSource()),
                    node.getGroupingKeys(),
                    node.getAggregations());
        }
        else {
            estimate = groupBy(
                    statsProvider.getStats(node.getSource()),
                    node.getGroupingKeys(),
                    node.getAggregations());
        }
        return Optional.of(estimate);
    }

    public static PlanNodeStatsEstimate groupBy(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables, Map<VariableReferenceExpression, Aggregation> aggregations)
    {
        // Used to estimate FINAL or SINGLE step aggregations
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        if (groupByVariables.isEmpty()) {
            result.setConfidence(FACT);
            result.setOutputRowCount(1);
        }
        else {
            result.addVariableStatistics(getGroupByVariablesStatistics(sourceStats, groupByVariables));
            double rowsCount = getRowsCount(sourceStats, groupByVariables);
            result.setOutputRowCount(min(rowsCount, sourceStats.getOutputRowCount()));
        }

        for (Map.Entry<VariableReferenceExpression, Aggregation> aggregationEntry : aggregations.entrySet()) {
            result.addVariableStatistics(aggregationEntry.getKey(), estimateAggregationStats(aggregationEntry.getValue(), sourceStats));
        }

        return result.build();
    }

    public static double getRowsCount(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables)
    {
        double rowsCount = 1;
        for (VariableReferenceExpression groupByVariable : groupByVariables) {
            VariableStatsEstimate symbolStatistics = sourceStats.getVariableStatistics(groupByVariable);
            int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            rowsCount *= symbolStatistics.getDistinctValuesCount() + nullRow;
        }
        return rowsCount;
    }

    private static PlanNodeStatsEstimate partialGroupBy(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables, Map<VariableReferenceExpression, Aggregation> aggregations)
    {
        // Pessimistic assumption of no reduction from PARTIAL and INTERMEDIATE aggregation, forwarding of the source statistics.
        // This makes the CBO estimates in the EXPLAIN plan output easier to understand,
        // even though partial aggregations are added after the CBO rules have been run.
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        result.setOutputRowCount(sourceStats.getOutputRowCount());
        result.addVariableStatistics(getGroupByVariablesStatistics(sourceStats, groupByVariables));
        for (Map.Entry<VariableReferenceExpression, Aggregation> aggregationEntry : aggregations.entrySet()) {
            result.addVariableStatistics(aggregationEntry.getKey(), estimateAggregationStats(aggregationEntry.getValue(), sourceStats));
        }

        return result.build();
    }

    private static Map<VariableReferenceExpression, VariableStatsEstimate> getGroupByVariablesStatistics(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables)
    {
        ImmutableMap.Builder<VariableReferenceExpression, VariableStatsEstimate> variableStatsEstimates = ImmutableMap.builder();
        for (VariableReferenceExpression groupByVariable : groupByVariables) {
            VariableStatsEstimate symbolStatistics = sourceStats.getVariableStatistics(groupByVariable);
            variableStatsEstimates.put(groupByVariable, symbolStatistics.mapNullsFraction(nullsFraction -> {
                if (nullsFraction == 0.0) {
                    return 0.0;
                }
                return 1.0 / (symbolStatistics.getDistinctValuesCount() + 1);
            }));
        }
        return variableStatsEstimates.build();
    }

    private static VariableStatsEstimate estimateAggregationStats(Aggregation aggregation, PlanNodeStatsEstimate sourceStats)
    {
        requireNonNull(aggregation, "aggregation is null");
        requireNonNull(sourceStats, "sourceStats is null");

        // TODO implement simple aggregations like: min, max, count, sum
        return VariableStatsEstimate.unknown();
    }
}
