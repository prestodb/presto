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
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getDefaultAggregateSelectivityCoefficient;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.DEFAULT_DATA_SIZE_PER_COLUMN;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static java.lang.Double.isNaN;
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

        // if source stats is unknown, aggregate stats must be unknown
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        if (isNaN(sourceStats.getOutputRowCount())) {
            return Optional.empty();
        }

        // The aggregate stats can be derived for SINGLE or FINAL stage of aggregation, but for a distributed
        // aggregate using PARTIAL or INTERMEDIATE, the aggregate stats can be derived by propagate the estimate from the source stats.
        if (!(node.getStep() == SINGLE || node.getStep() == FINAL)) {
            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
            result.setOutputRowCount(sourceStats.getOutputRowCount() * getDefaultAggregateSelectivityCoefficient(session));
            result.addVariableStatistics(sourceStats.getVariableStatistics());
            return Optional.of(result.build());
        }

        return Optional.of(groupBy(
                statsProvider.getStats(node.getSource()),
                node.getGroupingKeys(),
                node.getAggregations(),
                session));
    }

    public static PlanNodeStatsEstimate groupBy(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables, Map<VariableReferenceExpression, Aggregation> aggregations, Session session)
    {
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        for (VariableReferenceExpression groupByVariable : groupByVariables) {
            VariableStatsEstimate symbolStatistics = sourceStats.getVariableStatistics(groupByVariable);
            result.addVariableStatistics(groupByVariable, symbolStatistics.mapNullsFraction(nullsFraction -> {
                if (nullsFraction == 0.0) {
                    return 0.0;
                }
                return 1.0 / (symbolStatistics.getDistinctValuesCount() + 1);
            }));
        }

        double rowsCount = getDefaultAggregateSelectivityCoefficient(session);
        double totalSize = 0;
        for (VariableReferenceExpression groupByVariable : groupByVariables) {
            VariableStatsEstimate symbolStatistics = sourceStats.getVariableStatistics(groupByVariable);
            int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            rowsCount *= symbolStatistics.getDistinctValuesCount() + nullRow;
            if (!isNaN(symbolStatistics.getAverageRowSize())) {
                totalSize += symbolStatistics.getAverageRowSize() * rowsCount;
            }
        }
        // Set estimation boundary for aggregation:
        // If the source only return a single row, it's guaranteed the upper bound is a single row output for this agg.
        // Aggregation node cannot return output row count more than estimated row count of source node.
        // So if source stats is present, use the output row count from the source stats as the estimated output row
        // count as the upper bound limit to avoid returning an unknown output potentially breaks the join search space
        double outputRowCountFromSource = getDefaultAggregateSelectivityCoefficient(session) * sourceStats.getOutputRowCount();
        if (!isNaN(rowsCount)) {
            result.setOutputRowCount(min(rowsCount, outputRowCountFromSource));
        }
        else {
            // groupBy key may be a derived expression which may not have associated stats hence return NaN.
            // In such case, use source stats to bound the output row count
            result.setOutputRowCount(outputRowCountFromSource);
        }

        for (Map.Entry<VariableReferenceExpression, Aggregation> aggregationEntry : aggregations.entrySet()) {
            VariableStatsEstimate aggVariableStatsEstimate = estimateAggregationStats(aggregationEntry.getValue(), sourceStats, rowsCount);
            result.addVariableStatistics(aggregationEntry.getKey(), aggVariableStatsEstimate);
            totalSize += aggVariableStatsEstimate.getAverageRowSize() * aggVariableStatsEstimate.getDistinctValuesCount();
        }
        result.setTotalSize(totalSize);

        return result.build();
    }

    private static VariableStatsEstimate estimateAggregationStats(Aggregation aggregation, PlanNodeStatsEstimate sourceStats, double estimatedOutputRowCount)
    {
        requireNonNull(aggregation, "aggregation is null");
        requireNonNull(sourceStats, "sourceStats is null");
        requireNonNull(estimatedOutputRowCount, "estimatedOutputRowCount is null");

        double width;

        Type type = aggregation.getCall().getType();
        if (type instanceof FixedWidthType) {
            width = ((FixedWidthType) type).getFixedSize();
        }
        else {
            width = DEFAULT_DATA_SIZE_PER_COLUMN;
        }

        // Double.MIN_VALUE is a constant holding the smallest positive nonzero value of type double.
        // Put a -(minus sign) to become true negative smallest value to make correct cardinality estimate for a filter range expression to liberal comparison
        return new VariableStatsEstimate(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, width, estimatedOutputRowCount);
    }
}
