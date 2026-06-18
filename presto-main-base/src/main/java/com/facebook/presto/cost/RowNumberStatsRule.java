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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.Patterns;
import com.facebook.presto.sql.planner.plan.RowNumberNode;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.lang.Double.isNaN;
import static java.lang.Math.min;

public class RowNumberStatsRule
        extends SimpleStatsRule<RowNumberNode>
{
    private static final Pattern<RowNumberNode> PATTERN = Patterns.rowNumber();

    public RowNumberStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<RowNumberNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> doCalculate(RowNumberNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        if (sourceStats.isOutputRowCountUnknown()) {
            return Optional.empty();
        }
        double sourceRowsCount = sourceStats.getOutputRowCount();

        double partitionCount = 1;
        for (VariableReferenceExpression groupByVariable : node.getPartitionBy()) {
            VariableStatsEstimate variableStatistics = sourceStats.getVariableStatistics(groupByVariable);
            int nullRow = (variableStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            // assuming no correlation between grouping keys
            partitionCount *= variableStatistics.getDistinctValuesCount() + nullRow;
        }
        partitionCount = min(sourceRowsCount, partitionCount);

        if (isNaN(partitionCount)) {
            return Optional.empty();
        }

        // assuming no skew
        double rowsPerPartition = sourceRowsCount / partitionCount;
        if (node.getMaxRowCountPerPartition().isPresent()) {
            rowsPerPartition = min(rowsPerPartition, node.getMaxRowCountPerPartition().get());
        }

        double outputRowsCount = sourceRowsCount;
        if (node.getMaxRowCountPerPartition().isPresent()) {
            outputRowsCount = partitionCount * rowsPerPartition;
        }

        return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats)
                .setOutputRowCount(outputRowsCount)
                .addVariableStatistics(node.getRowNumberVariable(), VariableStatsEstimate.builder()
                        // Note: if we assume no skew, we could also estimate highValue
                        // (as rowsPerPartition), but underestimation of highValue may have
                        // more severe consequences than underestimation of distinctValuesCount
                        .setLowValue(1)
                        .setDistinctValuesCount(rowsPerPartition)
                        .setNullsFraction(0.0)
                        .setAverageRowSize(BIGINT.getFixedSize())
                        .build())
                .build());
    }
}
