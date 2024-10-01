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
package com.facebook.presto.iceberg.util;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.TableStatisticsMaker;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.LOW;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.function.Function.identity;

public final class StatisticsUtil
{
    private StatisticsUtil()
    {
    }

    public static final Set<ColumnStatisticType> SUPPORTED_MERGE_FLAGS = ImmutableSet.<ColumnStatisticType>builder()
            .add(NUMBER_OF_DISTINCT_VALUES)
            .add(TOTAL_SIZE_IN_BYTES)
            .build();

    /**
     * Attempts to merge statistics from Iceberg and Hive tables.
     * <br>
     * Statistics from Hive are only merged if the corresponding flag exists in
     * the {@code mergeFlags}.
     */
    public static TableStatistics mergeHiveStatistics(TableStatistics icebergStatistics, PartitionStatistics hiveStatistics, EnumSet<ColumnStatisticType> mergeFlags, PartitionSpec spec)
    {
        if (spec.isPartitioned()) {
            return icebergStatistics;
        }
        // We really only need to merge in NDVs and data size from the column statistics in hive's
        // stats
        //
        // Always take iceberg row count, nulls, and min/max statistics over hive's as they are
        // always up-to-date since they are computed and stored in manifests on writes
        Map<String, HiveColumnStatistics> columnStats = hiveStatistics.getColumnStatistics();
        TableStatistics.Builder statsBuilder = TableStatistics.builder();
        statsBuilder.setTotalSize(icebergStatistics.getTotalSize());
        statsBuilder.setRowCount(icebergStatistics.getRowCount());
        icebergStatistics.getColumnStatistics().forEach((columnHandle, icebergColumnStats) -> {
            HiveColumnStatistics hiveColumnStats = columnStats.get(((IcebergColumnHandle) columnHandle).getName());
            ColumnStatistics.Builder mergedStats = ColumnStatistics.builder()
                    .setDataSize(icebergColumnStats.getDataSize())
                    .setDistinctValuesCount(icebergColumnStats.getDistinctValuesCount())
                    .setRange(icebergColumnStats.getRange())
                    .setNullsFraction(icebergColumnStats.getNullsFraction())
                    .setDistinctValuesCount(icebergColumnStats.getDistinctValuesCount())
                    .setRange(icebergColumnStats.getRange());
            if (hiveColumnStats != null) {
                // NDVs
                if (mergeFlags.contains(NUMBER_OF_DISTINCT_VALUES)) {
                    hiveColumnStats.getDistinctValuesCount().ifPresent(ndvs -> mergedStats.setDistinctValuesCount(Estimate.of(ndvs)));
                }
                // data size
                if (mergeFlags.contains(ColumnStatisticType.TOTAL_SIZE_IN_BYTES)) {
                    hiveColumnStats.getTotalSizeInBytes().ifPresent(size -> mergedStats.setDataSize(Estimate.of(size)));
                }
            }
            statsBuilder.setColumnStatistics(columnHandle, mergedStats.build());
        });
        return calculateAndSetTableSize(statsBuilder).setConfidenceLevel(icebergStatistics.getConfidence()).build();
    }

    public static EnumSet<ColumnStatisticType> decodeMergeFlags(String input)
    {
        return Optional.of(Arrays.stream((input).trim().split(","))
                        .filter(value -> !value.isEmpty())
                        .map(ColumnStatisticType::valueOf)
                        .peek(statistic -> {
                            if (!SUPPORTED_MERGE_FLAGS.contains(statistic)) {
                                throw new PrestoException(INVALID_ARGUMENTS, "merge flags may only include " + SUPPORTED_MERGE_FLAGS);
                            }
                        })
                        .collect(Collectors.toSet()))
                .filter(set -> !set.isEmpty())
                .map(EnumSet::copyOf)
                .orElse(EnumSet.noneOf(ColumnStatisticType.class));
    }

    public static String encodeMergeFlags(EnumSet<ColumnStatisticType> flags)
    {
        return Joiner.on(",").join(flags.stream().peek(statistic -> {
            if (!SUPPORTED_MERGE_FLAGS.contains(statistic)) {
                throw new PrestoException(INVALID_ARGUMENTS, "merge flags may only include " + SUPPORTED_MERGE_FLAGS);
            }
        }).map(Enum::name).iterator());
    }

    public static TableStatistics.Builder calculateAndSetTableSize(TableStatistics.Builder builder)
    {
        return builder.setTotalSize(builder.getRowCount().flatMap(rowCount -> builder.getColumnStatistics().entrySet().stream().map(entry -> {
            IcebergColumnHandle columnHandle = (IcebergColumnHandle) entry.getKey();
            ColumnStatistics stats = entry.getValue();
            return stats.getDataSize().or(() -> {
                if (columnHandle.getType() instanceof FixedWidthType) {
                    return stats.getNullsFraction().map(nulls -> rowCount * (1 - nulls) * ((FixedWidthType) columnHandle.getType()).getFixedSize());
                }
                else {
                    return Estimate.unknown();
                }
            });
        }).reduce(Estimate.of(0.0), (currentSize, newSize) -> currentSize.flatMap(current -> newSize.map(add -> current + add)))));
    }

    /**
     * Used to determine the column set which should be passed to the {@link com.facebook.presto.iceberg.TableStatisticsMaker}.
     *
     * @param selectedColumns the columns selected by the query before the {@code FROM} clause.
     * @param layoutHandle handle containing the predicate on the table scan.
     * @return the entire set of columns which should be passed to the {@link com.facebook.presto.iceberg.TableStatisticsMaker}
     */
    public static List<IcebergColumnHandle> combineSelectedAndPredicateColumns(List<IcebergColumnHandle> selectedColumns, Optional<IcebergTableLayoutHandle> layoutHandle)
    {
        Set<Integer> columnIds = selectedColumns.stream()
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        return ImmutableList.<IcebergColumnHandle>builder()
                .addAll(selectedColumns)
                .addAll(layoutHandle
                        .map(IcebergTableLayoutHandle.class::cast)
                        .flatMap(handle -> Optional.of(handle.getPredicateColumns().values()))
                        .orElse(Collections.emptyList())
                        .stream()
                        .filter(predicateHandle -> !columnIds.contains(predicateHandle.getId()))
                        .iterator())
                .addAll(layoutHandle
                        .filter(IcebergTableLayoutHandle::isPushdownFilterEnabled)
                        .map(IcebergTableLayoutHandle::getPartitionColumnPredicate)
                        .flatMap(TupleDomain::getDomains)
                        .map(Map::keySet)
                        .orElse(ImmutableSet.of())
                        .stream()
                        .map(IcebergColumnHandle.class::cast)
                        .filter(predicateColumn -> !columnIds.contains(predicateColumn.getId()))
                        .iterator())
                .build();
    }

    public static String formatIdentifier(String s)
    {
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    /**
     * Calculates statistics for a table without considering filters which are pushed down onto a table
     *
     * @param metadata connector metadata
     * @param typeManager type manager
     * @param session current session
     * @param tableHandle table handle
     * @param tableLayoutHandle layout for current table handle
     * @param columnHandles desired columns
     * @param constraint constraints
     * @return statistics from Iceberg table without using the {@link com.facebook.presto.spi.plan.FilterStatsCalculatorService}
     */
    public static TableStatistics calculateBaseTableStatistics(
            ConnectorMetadata metadata,
            TypeManager typeManager,
            ConnectorSession session,
            StatisticsFileCache statisticsFileCache,
            IcebergTableHandle tableHandle,
            Optional<ConnectorTableLayoutHandle> tableLayoutHandle,
            List<ColumnHandle> columnHandles,
            Constraint<ColumnHandle> constraint)
    {
        Table icebergTable = getIcebergTable(metadata, session, tableHandle.getSchemaTableName());
        List<IcebergColumnHandle> handles = combineSelectedAndPredicateColumns(
                columnHandles.stream()
                        .map(IcebergColumnHandle.class::cast)
                        .collect(toImmutableList()),
                tableLayoutHandle.map(IcebergTableLayoutHandle.class::cast));
        return TableStatisticsMaker.getTableStatistics(session, typeManager,
                statisticsFileCache,
                tableLayoutHandle
                        .map(IcebergTableLayoutHandle.class::cast)
                        .map(IcebergTableLayoutHandle::getValidPredicate),
                constraint, tableHandle, icebergTable, handles);
    }

    public static TableStatistics calculateStatisticsConsideringLayout(
            FilterStatsCalculatorService filterStatsCalculatorService,
            RowExpressionService rowExpressionService,
            TableStatistics baseStatistics,
            ConnectorSession session,
            Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        return tableLayoutHandle.map(IcebergTableLayoutHandle.class::cast)
                .filter(unused -> isPushdownFilterEnabled(session))
                .map(layoutHandle -> {
                    TupleDomain<VariableReferenceExpression> predicate = layoutHandle.getValidPredicate()
                            .transform(columnHandle -> new VariableReferenceExpression(Optional.empty(), columnHandle.getName(), columnHandle.getType()));
                    RowExpression translatedPredicate = rowExpressionService.getDomainTranslator().toPredicate(predicate);
                    RowExpression combinedPredicate = and(layoutHandle.getRemainingPredicate(), translatedPredicate);
                    TableStatistics.Builder filteredStatsBuilder = TableStatistics.builder()
                            .setRowCount(baseStatistics.getRowCount());
                    Map<ColumnHandle, ColumnStatistics> fullColumnSet = baseStatistics.getColumnStatistics();
                    for (ColumnHandle colHandle : fullColumnSet.keySet()) {
                        IcebergColumnHandle icebergHandle = (IcebergColumnHandle) colHandle;
                        if (fullColumnSet.containsKey(icebergHandle)) {
                            ColumnStatistics stats = fullColumnSet.get(icebergHandle);
                            filteredStatsBuilder.setColumnStatistics(icebergHandle, stats);
                        }
                    }
                    return filterStatsCalculatorService.filterStats(
                            calculateAndSetTableSize(filteredStatsBuilder).setConfidenceLevel(LOW).build(),
                            combinedPredicate,
                            session,
                            fullColumnSet.keySet()
                                    .stream()
                                    .map(IcebergColumnHandle.class::cast).collect(toImmutableMap(
                                            identity(),
                                            IcebergColumnHandle::getName)),
                            fullColumnSet.keySet()
                                    .stream()
                                    .map(IcebergColumnHandle.class::cast).collect(toImmutableMap(
                                            IcebergColumnHandle::getName,
                                            IcebergColumnHandle::getType)));
                }).orElse(baseStatistics);
    }
}
