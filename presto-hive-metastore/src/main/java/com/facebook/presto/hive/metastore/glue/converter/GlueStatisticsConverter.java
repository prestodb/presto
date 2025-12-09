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
package com.facebook.presto.hive.metastore.glue.converter;

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.glue.model.BinaryColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.BooleanColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalNumber;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.fromMetastoreDistinctValuesCount;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreNullsCount;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.getAverageColumnLength;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.getTotalSizeInBytes;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreDistinctValuesCount;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

/**
 * Utility class for converting between Hive column statistics and AWS Glue column statistics.
 * This class handles bidirectional conversion:
 * - Hive → Glue: for writing statistics to AWS Glue
 * - Glue → Hive: for reading statistics from AWS Glue
 * <p>
 * This converter is stateless and contains only pure conversion logic.
 * It does not interact with AWS Glue APIs.
 */
public final class GlueStatisticsConverter
{
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

    private GlueStatisticsConverter() {}

    /**
     * Convert Hive column statistics to AWS Glue ColumnStatistics for a table.
     *
     * @param table the table containing the columns
     * @param hiveColumnStats map of column name to Hive column statistics
     * @param rowCount optional row count for the table
     * @return list of AWS Glue ColumnStatistics objects
     */
    public static List<ColumnStatistics> toGlueColumnStatistics(
            Table table,
            Map<String, HiveColumnStatistics> hiveColumnStats,
            OptionalLong rowCount)
    {
        return hiveColumnStats.entrySet().stream()
                .map(entry -> {
                    Column column = table.getColumn(entry.getKey())
                            .orElseThrow(() -> new IllegalArgumentException("Column not found: " + entry.getKey()));
                    return toGlueColumnStatistics(column, entry.getValue(), rowCount);
                })
                .filter(GlueStatisticsConverter::isGlueWritable)
                .collect(toImmutableList());
    }

    /**
     * Convert Hive column statistics to AWS Glue ColumnStatistics for a partition.
     *
     * @param partition the partition containing the columns
     * @param hiveColumnStats map of column name to Hive column statistics
     * @param rowCount optional row count for the partition
     * @return list of AWS Glue ColumnStatistics objects
     */
    public static List<ColumnStatistics> toGlueColumnStatistics(
            Partition partition,
            Map<String, HiveColumnStatistics> hiveColumnStats,
            OptionalLong rowCount)
    {
        return partition.getColumns().stream()
                .filter(column -> hiveColumnStats.containsKey(column.getName()))
                .map(column -> toGlueColumnStatistics(column, hiveColumnStats.get(column.getName()), rowCount))
                .filter(GlueStatisticsConverter::isGlueWritable)
                .collect(toImmutableList());
    }

    /**
     * Convert AWS Glue ColumnStatistics to a map of Hive column statistics.
     *
     * @param glueColumnStats list of AWS Glue ColumnStatistics
     * @param rowCount optional row count for computing derived statistics
     * @return map of column name to Hive column statistics
     */
    public static Map<String, HiveColumnStatistics> fromGlueColumnStatistics(
            List<ColumnStatistics> glueColumnStats,
            OptionalLong rowCount)
    {
        ImmutableMap.Builder<String, HiveColumnStatistics> result = ImmutableMap.builder();
        for (ColumnStatistics columnStats : glueColumnStats) {
            result.put(
                    columnStats.columnName(),
                    fromGlueColumnStatistics(columnStats.statisticsData(), rowCount));
        }
        return result.build();
    }

    /**
     * Convert AWS Glue ColumnStatistics to Hive column statistics for a table.
     *
     * @param table the table to get statistics for
     * @param glueColumnStats list of AWS Glue ColumnStatistics
     * @return map of column name to Hive column statistics
     */
    public static Map<String, HiveColumnStatistics> fromGlueColumnStatisticsForTable(
            Table table,
            List<ColumnStatistics> glueColumnStats)
    {
        HiveBasicStatistics tableStats = getHiveBasicStatistics(table.getParameters());
        return fromGlueColumnStatistics(glueColumnStats, tableStats.getRowCount());
    }

    /**
     * Convert AWS Glue ColumnStatistics to Hive column statistics for a partition.
     *
     * @param partition the partition to get statistics for
     * @param glueColumnStats list of AWS Glue ColumnStatistics
     * @return map of column name to Hive column statistics
     */
    public static Map<String, HiveColumnStatistics> fromGlueColumnStatisticsForPartition(
            Partition partition,
            List<ColumnStatistics> glueColumnStats)
    {
        HiveBasicStatistics partitionStats = getHiveBasicStatistics(partition.getParameters());
        return fromGlueColumnStatistics(glueColumnStats, partitionStats.getRowCount());
    }

    private static ColumnStatistics toGlueColumnStatistics(
            Column column,
            HiveColumnStatistics hiveStats,
            OptionalLong rowCount)
    {
        HiveType columnType = column.getType();
        ColumnStatisticsData glueStatsData = toGlueColumnStatisticsData(hiveStats, columnType, rowCount);

        return ColumnStatistics.builder()
                .columnName(column.getName())
                .columnType(columnType.toString())
                .statisticsData(glueStatsData)
                .analyzedTime(Instant.now())
                .build();
    }

    private static HiveColumnStatistics fromGlueColumnStatistics(
            ColumnStatisticsData glueStatsData,
            OptionalLong rowCount)
    {
        ColumnStatisticsType type = glueStatsData.type();
        switch (type) {
            case BINARY: {
                BinaryColumnStatisticsData data = glueStatsData.binaryColumnStatisticsData();
                OptionalLong max = OptionalLong.of(data.maximumLength());
                OptionalDouble avg = OptionalDouble.of(data.averageLength());
                OptionalLong nulls = fromMetastoreNullsCount(data.numberOfNulls());
                return createBinaryColumnStatistics(
                        max,
                        getTotalSizeInBytes(avg, rowCount, nulls),
                        nulls);
            }
            case BOOLEAN: {
                BooleanColumnStatisticsData data = glueStatsData.booleanColumnStatisticsData();
                return createBooleanColumnStatistics(
                        OptionalLong.of(data.numberOfTrues()),
                        OptionalLong.of(data.numberOfFalses()),
                        fromMetastoreNullsCount(data.numberOfNulls()));
            }
            case DATE: {
                DateColumnStatisticsData data = glueStatsData.dateColumnStatisticsData();
                Optional<LocalDate> min = instantToLocalDate(data.minimumValue());
                Optional<LocalDate> max = instantToLocalDate(data.maximumValue());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                return createDateColumnStatistics(
                        min,
                        max,
                        nullsCount,
                        fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case DECIMAL: {
                DecimalColumnStatisticsData data = glueStatsData.decimalColumnStatisticsData();
                Optional<BigDecimal> min = glueDecimalToBigDecimal(data.minimumValue());
                Optional<BigDecimal> max = glueDecimalToBigDecimal(data.maximumValue());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                return createDecimalColumnStatistics(
                        min,
                        max,
                        nullsCount,
                        fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case DOUBLE: {
                DoubleColumnStatisticsData data = glueStatsData.doubleColumnStatisticsData();
                OptionalDouble min = OptionalDouble.of(data.minimumValue());
                OptionalDouble max = OptionalDouble.of(data.maximumValue());
                OptionalLong nulls = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                return createDoubleColumnStatistics(
                        min,
                        max,
                        nulls,
                        fromMetastoreDistinctValuesCount(distinctValues, nulls, rowCount));
            }
            case LONG: {
                LongColumnStatisticsData data = glueStatsData.longColumnStatisticsData();
                OptionalLong min = OptionalLong.of(data.minimumValue());
                OptionalLong max = OptionalLong.of(data.maximumValue());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                return createIntegerColumnStatistics(
                        min,
                        max,
                        nullsCount,
                        fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case STRING: {
                StringColumnStatisticsData data = glueStatsData.stringColumnStatisticsData();
                OptionalLong max = OptionalLong.of(data.maximumLength());
                OptionalDouble avg = OptionalDouble.of(data.averageLength());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                return createStringColumnStatistics(
                        max,
                        getTotalSizeInBytes(avg, rowCount, nullsCount),
                        nullsCount,
                        fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case UNKNOWN_TO_SDK_VERSION: {
                throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + glueStatsData);
            }
        }

        throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + glueStatsData);
    }

    private static ColumnStatisticsData toGlueColumnStatisticsData(
            HiveColumnStatistics hiveStats,
            HiveType columnType,
            OptionalLong rowCount)
    {
        TypeInfo typeInfo = columnType.getTypeInfo();
        checkArgument(typeInfo.getCategory() == PRIMITIVE, "Unsupported statistics type: %s", columnType);

        ColumnStatisticsData.Builder glueStatsData = ColumnStatisticsData.builder();

        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN: {
                BooleanColumnStatisticsData.Builder data = BooleanColumnStatisticsData.builder();
                hiveStats.getNullsCount().ifPresent(data::numberOfNulls);
                hiveStats.getBooleanStatistics().ifPresent(booleanStats -> {
                    booleanStats.getFalseCount().ifPresent(data::numberOfFalses);
                    booleanStats.getTrueCount().ifPresent(data::numberOfTrues);
                });
                glueStatsData.type(ColumnStatisticsType.BOOLEAN.toString());
                glueStatsData.booleanColumnStatisticsData(data.build());
                break;
            }
            case BINARY: {
                BinaryColumnStatisticsData.Builder data = BinaryColumnStatisticsData.builder();
                hiveStats.getNullsCount().ifPresent(data::numberOfNulls);
                data.maximumLength(hiveStats.getMaxValueSizeInBytes().orElse(0));
                data.averageLength(getAverageColumnLength(
                        hiveStats.getTotalSizeInBytes(),
                        rowCount,
                        hiveStats.getNullsCount()).orElse(0));
                glueStatsData.type(ColumnStatisticsType.BINARY.toString());
                glueStatsData.binaryColumnStatisticsData(data.build());
                break;
            }
            case DATE: {
                DateColumnStatisticsData.Builder data = DateColumnStatisticsData.builder();
                hiveStats.getDateStatistics().ifPresent(dateStats -> {
                    dateStats.getMin().ifPresent(value -> data.minimumValue(localDateToInstant(value)));
                    dateStats.getMax().ifPresent(value -> data.maximumValue(localDateToInstant(value)));
                });
                hiveStats.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(
                        hiveStats.getDistinctValuesCount(),
                        hiveStats.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                glueStatsData.type(ColumnStatisticsType.DATE.toString());
                glueStatsData.dateColumnStatisticsData(data.build());
                break;
            }
            case DECIMAL: {
                DecimalColumnStatisticsData.Builder data = DecimalColumnStatisticsData.builder();
                hiveStats.getDecimalStatistics().ifPresent(decimalStats -> {
                    decimalStats.getMin().ifPresent(value -> data.minimumValue(bigDecimalToGlueDecimal(value)));
                    decimalStats.getMax().ifPresent(value -> data.maximumValue(bigDecimalToGlueDecimal(value)));
                });
                hiveStats.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(
                        hiveStats.getDistinctValuesCount(),
                        hiveStats.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                glueStatsData.type(ColumnStatisticsType.DECIMAL.toString());
                glueStatsData.decimalColumnStatisticsData(data.build());
                break;
            }
            case FLOAT:
            case DOUBLE: {
                DoubleColumnStatisticsData.Builder data = DoubleColumnStatisticsData.builder();
                hiveStats.getDoubleStatistics().ifPresent(doubleStats -> {
                    doubleStats.getMin().ifPresent(data::minimumValue);
                    doubleStats.getMax().ifPresent(data::maximumValue);
                });
                hiveStats.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(
                        hiveStats.getDistinctValuesCount(),
                        hiveStats.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                glueStatsData.type(ColumnStatisticsType.DOUBLE.toString());
                glueStatsData.doubleColumnStatisticsData(data.build());
                break;
            }
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP: {
                LongColumnStatisticsData.Builder data = LongColumnStatisticsData.builder();
                hiveStats.getIntegerStatistics().ifPresent(intStats -> {
                    intStats.getMin().ifPresent(data::minimumValue);
                    intStats.getMax().ifPresent(data::maximumValue);
                });
                hiveStats.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(
                        hiveStats.getDistinctValuesCount(),
                        hiveStats.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                glueStatsData.type(ColumnStatisticsType.LONG.toString());
                glueStatsData.longColumnStatisticsData(data.build());
                break;
            }
            case VARCHAR:
            case CHAR:
            case STRING: {
                StringColumnStatisticsData.Builder data = StringColumnStatisticsData.builder();
                hiveStats.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(
                        hiveStats.getDistinctValuesCount(),
                        hiveStats.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                data.maximumLength(hiveStats.getMaxValueSizeInBytes().orElse(0));
                data.averageLength(getAverageColumnLength(
                        hiveStats.getTotalSizeInBytes(),
                        rowCount,
                        hiveStats.getNullsCount()).orElse(0));
                glueStatsData.type(ColumnStatisticsType.STRING.toString());
                glueStatsData.stringColumnStatisticsData(data.build());
                break;
            }
            default:
                throw new PrestoException(
                        HIVE_INVALID_METADATA,
                        "Invalid column statistics type: " + ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
        }
        return glueStatsData.build();
    }

    /**
     * Check if the Glue ColumnStatistics is writable to AWS Glue.
     * Glue will accept null as min/max values but return 0 when reading.
     * To avoid incorrect stats, we skip writes for column statistics that have min/max null.
     * This can be removed once Glue fixes this behavior.
     */
    private static boolean isGlueWritable(ColumnStatistics stats)
    {
        ColumnStatisticsData statisticsData = stats.statisticsData();
        ColumnStatisticsType columnType = statisticsData.type();

        if (columnType.equals(ColumnStatisticsType.DATE)) {
            DateColumnStatisticsData data = statisticsData.dateColumnStatisticsData();
            return data.maximumValue() != null && data.minimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.DECIMAL)) {
            DecimalColumnStatisticsData data = statisticsData.decimalColumnStatisticsData();
            return data.maximumValue() != null && data.minimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.DOUBLE)) {
            DoubleColumnStatisticsData data = statisticsData.doubleColumnStatisticsData();
            return data.maximumValue() != null && data.minimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.LONG)) {
            LongColumnStatisticsData data = statisticsData.longColumnStatisticsData();
            return data.maximumValue() != null && data.minimumValue() != null;
        }
        return true;
    }

    private static DecimalNumber bigDecimalToGlueDecimal(BigDecimal decimal)
    {
        Decimal hiveDecimal = new Decimal(
                (short) decimal.scale(),
                ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
        return DecimalNumber.builder()
                .unscaledValue(SdkBytes.fromByteArray(hiveDecimal.getUnscaled()))
                .scale((int) hiveDecimal.getScale())
                .build();
    }

    private static Optional<BigDecimal> glueDecimalToBigDecimal(DecimalNumber glueDecimal)
    {
        if (glueDecimal == null) {
            return Optional.empty();
        }
        Decimal decimal = new Decimal();
        decimal.setUnscaled(glueDecimal.unscaledValue().asByteBuffer());
        decimal.setScale(glueDecimal.scale().shortValue());
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    private static Optional<LocalDate> instantToLocalDate(Instant date)
    {
        if (date == null) {
            return Optional.empty();
        }
        long daysSinceEpoch = date.toEpochMilli() / MILLIS_PER_DAY;
        return Optional.of(LocalDate.ofEpochDay(daysSinceEpoch));
    }

    private static Instant localDateToInstant(LocalDate date)
    {
        long millisecondsSinceEpoch = date.toEpochDay() * MILLIS_PER_DAY;
        return Instant.ofEpochMilli(millisecondsSinceEpoch);
    }
}
