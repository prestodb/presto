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

import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.function.Function.identity;

class TableStatisticsRecorder
{
    <E extends TpchEntity> TableStatisticsData recordStatistics(TpchTable<E> tpchTable, Predicate<E> constraint, double scaleFactor)
    {
        Iterable<E> rows = tpchTable.createGenerator(scaleFactor, 1, 1);
        return recordStatistics(rows, tpchTable.getColumns(), constraint);
    }

    private <E extends TpchEntity> TableStatisticsData recordStatistics(Iterable<E> rows, List<TpchColumn<E>> columns, Predicate<E> constraint)
    {
        Map<TpchColumn<E>, ColumnStatisticsRecorder> statisticsRecorders = createStatisticsRecorders(columns);
        long rowCount = 0;

        for (E row : rows) {
            if (constraint.test(row)) {
                rowCount++;
                for (TpchColumn<E> column : columns) {
                    Comparable<?> value = getTpchValue(row, column);
                    statisticsRecorders.get(column).record(value);
                }
            }
        }

        Map<String, ColumnStatisticsData> columnSampleStatistics = statisticsRecorders.entrySet().stream()
                .collect(toImmutableMap(
                        e -> e.getKey().getColumnName(),
                        e -> e.getValue().getRecording()));
        return new TableStatisticsData(rowCount, columnSampleStatistics);
    }

    private <E extends TpchEntity> Map<TpchColumn<E>, ColumnStatisticsRecorder> createStatisticsRecorders(List<TpchColumn<E>> columns)
    {
        return columns.stream()
                .collect(toImmutableMap(identity(), (column) -> new ColumnStatisticsRecorder(column.getType())));
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
}
