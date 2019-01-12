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

package io.prestosql.plugin.tpcds.statistics;

import com.teradata.tpcds.Table;
import io.airlift.slice.Slice;
import io.prestosql.plugin.tpcds.TpcdsColumnHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimeType.TIME;
import static java.lang.Double.parseDouble;

public class TpcdsTableStatisticsFactory
{
    private final TableStatisticsDataRepository statisticsDataRepository = new TableStatisticsDataRepository();

    public TableStatistics create(String schemaName, Table table, Map<String, ColumnHandle> columnHandles)
    {
        Optional<TableStatisticsData> statisticsDataOptional = statisticsDataRepository.load(schemaName, table);
        return statisticsDataOptional.map(statisticsData -> toTableStatistics(columnHandles, statisticsData))
                .orElse(TableStatistics.empty());
    }

    private TableStatistics toTableStatistics(Map<String, ColumnHandle> columnHandles, TableStatisticsData statisticsData)
    {
        long rowCount = statisticsData.getRowCount();
        TableStatistics.Builder tableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(rowCount));

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
        columnStatistics.setNullsFraction(Estimate.of((double) nullCount / rowCount));
        columnStatistics.setRange(toRange(columnStatisticsData.getMin(), columnStatisticsData.getMax(), type));
        columnStatistics.setDistinctValuesCount(Estimate.of(columnStatisticsData.getDistinctValuesCount()));
        columnStatistics.setDataSize(columnStatisticsData.getDataSize().map(Estimate::of).orElse(Estimate.unknown()));
        return columnStatistics.build();
    }

    private static Optional<DoubleRange> toRange(Optional<Object> min, Optional<Object> max, Type columnType)
    {
        if (columnType instanceof VarcharType || columnType instanceof CharType || columnType.equals(TIME)) {
            return Optional.empty();
        }
        if (!min.isPresent() || !max.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(new DoubleRange(toDouble(min.get(), columnType), toDouble(max.get(), columnType)));
    }

    private static double toDouble(Object value, Type type)
    {
        if (value instanceof String && type.equals(DATE)) {
            return LocalDate.parse((CharSequence) value).toEpochDay();
        }
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(DATE)) {
            return ((Number) value).doubleValue();
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (isShortDecimal(decimalType)) {
                return parseDouble(Decimals.toString(((Number) value).longValue(), decimalType.getScale()));
            }
            if (isLongDecimal(decimalType)) {
                return parseDouble(Decimals.toString((Slice) value, decimalType.getScale()));
            }
            throw new IllegalArgumentException("Unexpected decimal type: " + decimalType);
        }
        if (type.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        throw new IllegalArgumentException("unsupported column type " + type);
    }
}
