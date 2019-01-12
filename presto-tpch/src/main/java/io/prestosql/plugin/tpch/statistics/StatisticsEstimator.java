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
package io.prestosql.plugin.tpch.statistics;

import io.airlift.slice.Slice;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.tpch.util.Optionals.combine;
import static io.prestosql.plugin.tpch.util.Types.checkSameType;
import static io.prestosql.plugin.tpch.util.Types.checkType;

public class StatisticsEstimator
{
    private final TableStatisticsDataRepository tableStatisticsDataRepository;

    public StatisticsEstimator(TableStatisticsDataRepository tableStatisticsDataRepository)
    {
        this.tableStatisticsDataRepository = tableStatisticsDataRepository;
    }

    public Optional<TableStatisticsData> estimateStats(TpchTable<?> tpchTable, Map<TpchColumn<?>, List<Object>> columnValuesRestrictions, double scaleFactor)
    {
        String schemaName = "sf" + scaleFactor;
        if (columnValuesRestrictions.isEmpty()) {
            return tableStatisticsDataRepository.load(schemaName, tpchTable, Optional.empty(), Optional.empty());
        }
        else if (columnValuesRestrictions.values().stream().allMatch(List::isEmpty)) {
            return Optional.of(zeroStatistics(tpchTable));
        }
        else {
            checkArgument(columnValuesRestrictions.size() <= 1, "Can only estimate stats when at most one column has value restrictions");
            TpchColumn<?> partitionColumn = getOnlyElement(columnValuesRestrictions.keySet());
            List<Object> partitionValues = columnValuesRestrictions.get(partitionColumn);
            TableStatisticsData result = zeroStatistics(tpchTable);
            for (Object partitionValue : partitionValues) {
                Slice value = checkType(partitionValue, Slice.class, "Only string (Slice) partition values supported for now");
                Optional<TableStatisticsData> tableStatisticsData = tableStatisticsDataRepository
                        .load(schemaName, tpchTable, Optional.of(partitionColumn), Optional.of(value.toStringUtf8()));
                if (!tableStatisticsData.isPresent()) {
                    return Optional.empty();
                }
                result = addPartitionStats(result, tableStatisticsData.get(), partitionColumn);
            }
            return Optional.of(result);
        }
    }

    private TableStatisticsData addPartitionStats(TableStatisticsData left, TableStatisticsData right, TpchColumn<?> partitionColumn)
    {
        Map<String, ColumnStatisticsData> combinedColumnStats = left.getColumns().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> {
                    String columnName = entry.getKey();
                    ColumnStatisticsData leftStats = entry.getValue();
                    ColumnStatisticsData rightStats = right.getColumns().get(columnName);
                    Optional<Long> ndv = addDistinctValuesCount(partitionColumn, columnName, leftStats, rightStats);
                    Optional<Object> min = combine(leftStats.getMin(), rightStats.getMin(), this::min);
                    Optional<Object> max = combine(leftStats.getMax(), rightStats.getMax(), this::max);
                    // Sum data sizes only if both known
                    Optional<Long> dataSize = leftStats.getDataSize()
                            .flatMap(leftDataSize -> rightStats.getDataSize().map(rightDataSize -> leftDataSize + rightDataSize));
                    return new ColumnStatisticsData(ndv, min, max, dataSize);
                }));

        return new TableStatisticsData(
                left.getRowCount() + right.getRowCount(),
                combinedColumnStats);
    }

    private Optional<Long> addDistinctValuesCount(TpchColumn<?> partitionColumn, String columnName, ColumnStatisticsData leftStats, ColumnStatisticsData rightStats)
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
                column -> ColumnStatisticsData.zero())));
    }
}
