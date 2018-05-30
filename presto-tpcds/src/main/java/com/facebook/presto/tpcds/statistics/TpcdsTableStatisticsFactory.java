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

package com.facebook.presto.tpcds.statistics;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.tpcds.TpcdsColumnHandle;
import com.teradata.tpcds.Table;
import io.airlift.slice.Slices;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;

public class TpcdsTableStatisticsFactory
{
    private final TableStatisticsDataRepository statisticsDataRepository = new TableStatisticsDataRepository();

    public TableStatistics create(String schemaName, Table table, Map<String, ColumnHandle> columnHandles)
    {
        Optional<TableStatisticsData> statisticsDataOptional = statisticsDataRepository.load(schemaName, table);
        return statisticsDataOptional.map(statisticsData -> toTableStatistics(columnHandles, statisticsData))
                .orElse(TableStatistics.EMPTY_STATISTICS);
    }

    private TableStatistics toTableStatistics(Map<String, ColumnHandle> columnHandles, TableStatisticsData statisticsData)
    {
        long rowCount = statisticsData.getRowCount();
        TableStatistics.Builder tableStatistics = TableStatistics.builder()
                .setRowCount(new Estimate(rowCount));

        if (rowCount > 0) {
            Map<String, ColumnStatisticsData> columnsData = statisticsData.getColumns();
            for (Map.Entry<String, ColumnHandle> entry : columnHandles.entrySet()) {
                TpcdsColumnHandle columnHandle = (TpcdsColumnHandle) entry.getValue();
                tableStatistics.setColumnStatistics(entry.getValue(), toColumnStatistics(columnsData.get(entry.getKey()), columnHandle.getType(), rowCount));
            }
        }

        return tableStatistics.build();
    }

    private ColumnStatistics toColumnStatistics(ColumnStatisticsData columnStatisticsData, Type type, long rowCount)
    {
        ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
        long nullCount = columnStatisticsData.getNullsCount();
        columnStatistics.setNullsFraction(new Estimate((double) nullCount / rowCount));
        columnStatistics.addRange(builder -> builder
                .setLowValue(
                        columnStatisticsData.getMin()
                                .map(value -> toPrestoValue(value, type)))
                .setHighValue(
                        columnStatisticsData.getMax()
                                .map(value -> toPrestoValue(value, type)))
                .setDistinctValuesCount(new Estimate(columnStatisticsData.getDistinctValuesCount()))
                .setFraction(new Estimate(((double) rowCount - nullCount) / rowCount))
                .build());

        return columnStatistics.build();
    }

    private Object toPrestoValue(Object tpcdsValue, Type type)
    {
        if (type instanceof VarcharType) {
            return Slices.utf8Slice((String) tpcdsValue);
        }
        else if (type instanceof CharType) {
            return truncateToLengthAndTrimSpaces(Slices.utf8Slice((String) tpcdsValue), type);
        }
        else if (tpcdsValue instanceof String && type.equals(DATE)) {
            return LocalDate.parse((CharSequence) tpcdsValue).toEpochDay();
        }
        else if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(DATE) || (type instanceof DecimalType && isShortDecimal(type))) {
            return ((Number) tpcdsValue).longValue();
        }
        else if (type.equals(DOUBLE)) {
            return ((Number) tpcdsValue).doubleValue();
        }
        else if (type.equals(TimeType.TIME)) {
            return ((Number) tpcdsValue).longValue();
        }
        throw new IllegalArgumentException("unsupported column type " + type);
    }
}
