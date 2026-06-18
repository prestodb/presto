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

import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;

class TableStatisticsRecorder
{
    <E extends TpchEntity> TableStatisticsData recordStatistics(TpchTable<E> tpchTable, Predicate<E> constraint, double scaleFactor)
    {
        int parallelPartsToProcess = Runtime.getRuntime().availableProcessors();
        if (tpchTable.equals(TpchTable.NATION) || tpchTable.equals(TpchTable.REGION)) {
            //These tables have too few rows to benefit from parallel processing
            parallelPartsToProcess = 1;
        }

        ArrayList<CompletableFuture<TablePartStatistics>> statsRecorders = new ArrayList<>();

        for (int i = 0; i < parallelPartsToProcess; i++) {
            //Generate a part of data
            Iterable<E> rows = tpchTable.createGenerator(scaleFactor, i + 1, parallelPartsToProcess);
            //Record its statistics on a separate thread
            statsRecorders.add(CompletableFuture.supplyAsync(() -> recordStatistics(rows, tpchTable.getColumns(), constraint)));
        }
        try {
            //Wait for all parts to finish processing
            CompletableFuture.allOf(statsRecorders.toArray(new CompletableFuture[0])).get();
        }
        catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        Optional<TablePartStatistics> combinedStatistics = statsRecorders.stream().map(CompletableFuture::join).reduce((x, y) -> {
            x.setRowCount(x.getRowCount() + y.getRowCount());
            x.setRawColStats(Stream.of(x.getRawColStats(), y.getRawColStats())
                    .flatMap(m -> m.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, ColumnStatisticsRecorder::mergeWith)));
            return x;
        });

        checkState(combinedStatistics.isPresent(), "combinedStatistics empty, no stats were produced");

        final Map<String, ColumnStatisticsData> combinedTableStatsRecording = combinedStatistics.get()
                .getRawColStats().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, y -> y.getValue().getRecording()));

        return new TableStatisticsData(combinedStatistics.get().getRowCount(), combinedTableStatsRecording);
    }

    private <E extends TpchEntity> TablePartStatistics recordStatistics(Iterable<E> rows, List<TpchColumn<E>> columns, Predicate<E> constraint)
    {
        Map<String, ColumnStatisticsRecorder> statisticsRecorders = createStatisticsRecorders(columns);
        long rowCount = 0;

        for (E row : rows) {
            if (constraint.test(row)) {
                rowCount++;
                for (TpchColumn<E> column : columns) {
                    Comparable<?> value = getTpchValue(row, column);
                    statisticsRecorders.get(column.getColumnName()).record(value);
                }
            }
        }

        return new TablePartStatistics(rowCount, statisticsRecorders);
    }

    private <E extends TpchEntity> Map<String, ColumnStatisticsRecorder> createStatisticsRecorders(List<TpchColumn<E>> columns)
    {
        return columns.stream()
                .collect(toImmutableMap(TpchColumn::getColumnName, (column) -> new ColumnStatisticsRecorder(column.getType())));
    }

    private <E extends TpchEntity> Comparable<?> getTpchValue(E row, TpchColumn<E> column)
    {
        TpchColumnType.Base baseType = column.getType().getBase();
        switch (baseType) {
            case IDENTIFIER:
                return column.getIdentifier(row);
            case INTEGER:
                return column.getInteger(row);
            case DATE:
                return column.getDate(row);
            case DOUBLE:
                return column.getDouble(row);
            case VARCHAR:
                return column.getString(row);
        }
        throw new UnsupportedOperationException(format("Unsupported TPCH base type [%s]", baseType));
    }

    private static class TablePartStatistics
    {
        long rowCount;
        Map<String, ColumnStatisticsRecorder> rawColStats;

        public TablePartStatistics(long rowCount, Map<String, ColumnStatisticsRecorder> rawColStats)
        {
            this.rowCount = rowCount;
            this.rawColStats = rawColStats;
        }

        public Map<String, ColumnStatisticsRecorder> getRawColStats()
        {
            return rawColStats;
        }

        public void setRawColStats(Map<String, ColumnStatisticsRecorder> rawColStats)
        {
            this.rawColStats = rawColStats;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public void setRowCount(long rowCount)
        {
            this.rowCount = rowCount;
        }
    }
}
