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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.ColumnStatistics.Builder;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableBiMap;

import java.util.Map;

import static com.facebook.presto.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static com.facebook.presto.cost.StatsUtil.toVariableStatsEstimate;
import static java.util.Objects.requireNonNull;

public class ConnectorFilterStatsCalculatorService
        implements FilterStatsCalculatorService
{
    private final FilterStatsCalculator filterStatsCalculator;

    public ConnectorFilterStatsCalculatorService(FilterStatsCalculator filterStatsCalculator)
    {
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
    }

    @Override
    public TableStatistics filterStats(
            TableStatistics tableStatistics,
            RowExpression predicate,
            ConnectorSession session,
            Map<ColumnHandle, String> columnNames,
            Map<String, Type> columnTypes)
    {
        PlanNodeStatsEstimate tableStats = toPlanNodeStats(tableStatistics, columnNames, columnTypes);
        PlanNodeStatsEstimate filteredStats = filterStatsCalculator.filterStats(tableStats, predicate, session);

        if (filteredStats.isOutputRowCountUnknown()) {
            filteredStats = tableStats.mapOutputRowCount(sourceRowCount -> tableStats.getOutputRowCount() * UNKNOWN_FILTER_COEFFICIENT);
        }

        TableStatistics filteredStatistics = toTableStatistics(filteredStats, ImmutableBiMap.copyOf(columnNames).inverse());
        // Fill in the totalSize after filter, estimated proportional to the rowCount after versus before filter.
        TableStatistics.Builder filteredStatsWithSize = TableStatistics.builder();
        filteredStatsWithSize.setRowCount(filteredStatistics.getRowCount());
        filteredStatistics.getColumnStatistics().forEach(filteredStatsWithSize::setColumnStatistics);
        // If the rowCount before or after filter is zero, totalSize will also be zero
        if (!tableStatistics.getRowCount().isUnknown() && tableStatistics.getRowCount().getValue() == 0
                || !filteredStatistics.getRowCount().isUnknown() && filteredStatistics.getRowCount().getValue() == 0) {
            filteredStatsWithSize.setTotalSize(Estimate.of(0));
        }
        else if (!tableStatistics.getTotalSize().isUnknown()
                && !filteredStatistics.getRowCount().isUnknown()
                && !tableStatistics.getRowCount().isUnknown()) {
            double totalSizeAfterFilter = filteredStatistics.getRowCount().getValue() / tableStatistics.getRowCount().getValue() * tableStatistics.getTotalSize().getValue();
            filteredStatsWithSize.setTotalSize(Estimate.of(totalSizeAfterFilter));
        }
        return filteredStatsWithSize.build();
    }

    private static PlanNodeStatsEstimate toPlanNodeStats(
            TableStatistics tableStatistics,
            Map<ColumnHandle, String> columnNames,
            Map<String, Type> columnTypes)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(tableStatistics.getRowCount().getValue());

        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : tableStatistics.getColumnStatistics().entrySet()) {
            String columnName = columnNames.get(entry.getKey());
            VariableReferenceExpression variable = new VariableReferenceExpression(columnName, columnTypes.get(columnName));
            builder.addVariableStatistics(variable, toVariableStatsEstimate(tableStatistics, entry.getValue()));
        }
        return builder.build();
    }

    private static TableStatistics toTableStatistics(PlanNodeStatsEstimate planNodeStats, Map<String, ColumnHandle> columnByName)
    {
        TableStatistics.Builder builder = TableStatistics.builder();
        if (planNodeStats.isOutputRowCountUnknown()) {
            builder.setRowCount(Estimate.unknown());
            return builder.build();
        }

        double rowCount = planNodeStats.getOutputRowCount();
        builder.setRowCount(Estimate.of(rowCount));
        for (Map.Entry<VariableReferenceExpression, VariableStatsEstimate> entry : planNodeStats.getVariableStatistics().entrySet()) {
            builder.setColumnStatistics(columnByName.get(entry.getKey().getName()), toColumnStatistics(entry.getValue(), rowCount));
        }
        return builder.build();
    }

    private static ColumnStatistics toColumnStatistics(VariableStatsEstimate variableStatsEstimate, double rowCount)
    {
        if (variableStatsEstimate.isUnknown()) {
            return ColumnStatistics.empty();
        }

        double nullsFractionDouble = variableStatsEstimate.getNullsFraction();
        double nonNullRowsCount = rowCount * (1.0 - nullsFractionDouble);

        Builder builder = ColumnStatistics.builder();
        if (!Double.isNaN(nullsFractionDouble)) {
            builder.setNullsFraction(Estimate.of(nullsFractionDouble));
        }

        if (!Double.isNaN(variableStatsEstimate.getDistinctValuesCount())) {
            builder.setDistinctValuesCount(Estimate.of(variableStatsEstimate.getDistinctValuesCount()));
        }

        if (!Double.isNaN(variableStatsEstimate.getAverageRowSize())) {
            builder.setDataSize(Estimate.of(variableStatsEstimate.getAverageRowSize() * nonNullRowsCount));
        }

        if (!Double.isNaN(variableStatsEstimate.getLowValue()) && !Double.isNaN(variableStatsEstimate.getHighValue())) {
            builder.setRange(new DoubleRange(variableStatsEstimate.getLowValue(), variableStatsEstimate.getHighValue()));
        }
        return builder.build();
    }
}
