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
package com.facebook.presto.tpch.statistics;

import com.facebook.presto.util.Types;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.tpch.CustomerColumn;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.OrderColumn;
import io.airlift.tpch.PartColumn;
import io.airlift.tpch.PartSupplierColumn;
import io.airlift.tpch.SupplierColumn;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.tpch.util.Optionals.checkPresent;
import static com.facebook.presto.tpch.util.Optionals.combine;
import static com.facebook.presto.tpch.util.Optionals.withBoth;
import static com.facebook.presto.util.Types.checkSameType;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Optional.empty;

public class StatisticsEstimator
{
    //These columns are unsupported because scaling them would require sophisticated logic
    private static final Set<TpchColumn<?>> UNSUPPORTED_COLUMNS = ImmutableSet.of(
            CustomerColumn.ACCOUNT_BALANCE,
            LineItemColumn.EXTENDED_PRICE,
            OrderColumn.TOTAL_PRICE,
            PartColumn.RETAIL_PRICE,
            PartSupplierColumn.AVAILABLE_QUANTITY,
            SupplierColumn.ACCOUNT_BALANCE
    );

    private final TableStatisticsDataRepository tableStatisticsDataRepository;

    public StatisticsEstimator(TableStatisticsDataRepository tableStatisticsDataRepository)
    {
        this.tableStatisticsDataRepository = tableStatisticsDataRepository;
    }

    public TableStatisticsData estimateStats(TpchTable<?> tpchTable, Map<TpchColumn<?>, List<Object>> columnValuesRestrictions, double scaleFactor)
    {
        TableStatisticsData bigStatistics = readStatistics(tpchTable, columnValuesRestrictions, "sf1");
        TableStatisticsData smallStatistics = readStatistics(tpchTable, columnValuesRestrictions, "tiny");
        double rescalingFactor = smallStatistics.getRowCount() == bigStatistics.getRowCount() ? 1 : scaleFactor;
        return new TableStatisticsData(
                (long) (bigStatistics.getRowCount() * rescalingFactor),
                rescale(tpchTable, bigStatistics, smallStatistics, scaleFactor)
        );
    }

    private Map<String, ColumnStatisticsData> rescale(TpchTable<?> table, TableStatisticsData bigStatistics, TableStatisticsData smallStatistics, double scaleFactor)
    {
        return bigStatistics.getColumns().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> {
                    String columnName = entry.getKey();
                    TpchColumn<?> column = table.getColumn(columnName);
                    ColumnStatisticsData bigColumnStatistics = entry.getValue();
                    ColumnStatisticsData smallColumnStatistics = smallStatistics.getColumns().get(columnName);
                    return rescale(column, bigColumnStatistics, smallColumnStatistics, scaleFactor);
                }
        ));
    }

    private ColumnStatisticsData rescale(TpchColumn<?> column, ColumnStatisticsData big, ColumnStatisticsData small, double scaleFactor)
    {
        if (UNSUPPORTED_COLUMNS.contains(column)) {
            return ColumnStatisticsData.empty();
        }
        else {
            return rescale(big, small, scaleFactor);
        }
    }

    private ColumnStatisticsData rescale(ColumnStatisticsData big, ColumnStatisticsData small, double scaleFactor)
    {
        if (columnDoesNotScale(big, small)) {
            return new ColumnStatisticsData(
                    big.getDistinctValuesCount(),
                    big.getNullsCount(),
                    checkPresent(withBoth(big.getMin(), small.getMin(), this::checkClose)),
                    checkPresent(withBoth(big.getMax(), small.getMax(), this::checkClose))
            );
        }
        else {
            Function<Number, Double> rescale = value -> value.doubleValue() * scaleFactor;
            return new ColumnStatisticsData(
                    big.getDistinctValuesCount().map(rescale).map(Number::longValue),
                    big.getNullsCount().map(rescale).map(Number::longValue),
                    Types.tryCast(big.getMin(), Number.class),
                    Types.tryCast(big.getMax(), Number.class).map(rescale));
        }
    }

    private boolean columnDoesNotScale(ColumnStatisticsData big, ColumnStatisticsData small)
    {
        return withBoth(small.getMin(), big.getMin(), this::areClose)
                .flatMap(lowerBoundsClose ->
                        withBoth(small.getMax(), big.getMax(), this::areClose)
                                .map(upperBoundsClose -> lowerBoundsClose && upperBoundsClose))
                .orElse(false);
    }

    private Object checkClose(Object leftValue, Object rightValue)
    {
        checkArgument(
                areClose(leftValue, rightValue),
                format("Values must be close to each other, got [%s] and [%s]", leftValue, rightValue));
        return leftValue;
    }

    private boolean areClose(Object leftValue, Object rightValue)
    {
        checkSameType(leftValue, rightValue);
        if (leftValue instanceof String) {
            return leftValue.equals(rightValue);
        }
        else {
            Number left = checkType(leftValue, Number.class);
            Number right = checkType(rightValue, Number.class);
            return areClose(left.doubleValue(), right.doubleValue());
        }
    }

    private boolean areClose(double left, double right)
    {
        return abs(right - left) <= abs(left) * 0.01;
    }

    private TableStatisticsData readStatistics(TpchTable<?> table, Map<TpchColumn<?>, List<Object>> columnValuesRestrictions, String schemaName)
    {
        if (columnValuesRestrictions.isEmpty()) {
            return tableStatisticsDataRepository.load(schemaName, table, empty(), empty());
        }
        else if (columnValuesRestrictions.values().stream().allMatch(List::isEmpty)) {
            return zeroStatistics(table);
        }
        else {
            checkArgument(columnValuesRestrictions.size() <= 1, "Can only estimate stats when at most one column has value restrictions");
            TpchColumn<?> partitionColumn = getOnlyElement(columnValuesRestrictions.keySet());
            List<Object> partitionValues = columnValuesRestrictions.get(partitionColumn);
            TableStatisticsData result = zeroStatistics(table);
            for (Object partitionValue : partitionValues) {
                Slice value = checkType(partitionValue, Slice.class, "Only string (Slice) partition values supported for now");
                TableStatisticsData tableStatisticsData = tableStatisticsDataRepository
                        .load(schemaName, table, Optional.of(partitionColumn), Optional.of(value.toStringUtf8()));
                result = addPartitionStats(result, tableStatisticsData, partitionColumn);
            }
            return result;
        }
    }

    private TableStatisticsData addPartitionStats(TableStatisticsData left, TableStatisticsData right, TpchColumn<?> partitionColumn)
    {
        return new TableStatisticsData(
                left.getRowCount() + right.getRowCount(),
                addPartitionStats(left.getColumns(), right.getColumns(), partitionColumn)
        );
    }

    private Map<String, ColumnStatisticsData> addPartitionStats(Map<String, ColumnStatisticsData> leftColumns, Map<String, ColumnStatisticsData> rightColumns, TpchColumn<?> partitionColumn)
    {
        return leftColumns.entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> {
                    String columnName = entry.getKey();
                    ColumnStatisticsData leftStats = entry.getValue();
                    ColumnStatisticsData rightStats = rightColumns.get(columnName);
                    return new ColumnStatisticsData(
                            combineUniqueValuesCount(partitionColumn, columnName, leftStats, rightStats),
                            combine(leftStats.getNullsCount(), rightStats.getNullsCount(), (a, b) -> a + b),
                            combine(leftStats.getMin(), rightStats.getMin(), this::min),
                            combine(leftStats.getMax(), rightStats.getMax(), this::max)
                    );
                }));
    }

    private Optional<Long> combineUniqueValuesCount(TpchColumn<?> partitionColumn, String columnName, ColumnStatisticsData leftStats, ColumnStatisticsData rightStats)
    {
        //unique values count can't be added between different partitions
        //for columns other than the partition column (because almost certainly there are duplicates)
        return combine(leftStats.getDistinctValuesCount(), rightStats.getDistinctValuesCount(), (a, b) -> a + b)
                .filter(v -> columnName.equals(partitionColumn.getColumnName()));
    }

    @SuppressWarnings("unchecked")
    private Object min(Object l, Object r)
    {
        checkSameType(l, r);
        Comparable left = checkType(l, Comparable.class);
        Comparable right = checkType(r, Comparable.class);
        return left.compareTo(right) < 0 ? left : right;
    }

    @SuppressWarnings("unchecked")
    private Object max(Object l, Object r)
    {
        checkSameType(l, r);
        Comparable left = checkType(l, Comparable.class);
        Comparable right = checkType(r, Comparable.class);
        return left.compareTo(right) > 0 ? left : right;
    }

    private TableStatisticsData zeroStatistics(TpchTable<?> table)
    {
        return new TableStatisticsData(0, table.getColumns().stream().collect(toImmutableMap(
                TpchColumn::getColumnName,
                column -> ColumnStatisticsData.zero()
        )));
    }
}
