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

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
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
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreNullsCount;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.getAverageColumnLength;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.getTotalSizeInBytes;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreDistinctValuesCount;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

public class GlueStatConverter
{
    private GlueStatConverter() {}

    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

    public static List<ColumnStatistics> toGlueColumnStatistics(
            Partition partition,
            Map<String,
                    HiveColumnStatistics> prestoColumnStats,
            OptionalLong rowCount)
    {
        return partition.getColumns().stream()
                .filter(column -> prestoColumnStats.containsKey(column.getName()))
                .map(c -> toColumnStatistics(c, prestoColumnStats.get(c.getName()), rowCount))
                .collect(toImmutableList());
    }

    public static List<ColumnStatistics> toGlueColumnStatistics(
            Table table,
            Map<String, HiveColumnStatistics> prestoColumnStats,
            OptionalLong rowCount)
    {
        return prestoColumnStats.entrySet().stream()
                .map(e -> toColumnStatistics(table.getColumn(e.getKey()).get(), e.getValue(), rowCount))
                .collect(toImmutableList());
    }

    private static ColumnStatistics toColumnStatistics(Column column, HiveColumnStatistics statistics, OptionalLong rowCount)
    {
        HiveType columnType = column.getType();
        ColumnStatisticsData catalogColumnStatisticsData = toGlueColumnStatisticsData(statistics, columnType, rowCount);
        ColumnStatistics columnStatistics = ColumnStatistics.builder()
                .columnName(column.getName())
                .columnType(columnType.toString())
                .statisticsData(catalogColumnStatisticsData)
                .analyzedTime(Instant.now())
                .build();
        return columnStatistics;
    }

    public static HiveColumnStatistics fromGlueColumnStatistics(ColumnStatisticsData catalogColumnStatisticsData, OptionalLong rowCount)
    {
        ColumnStatisticsType type = catalogColumnStatisticsData.type();
        switch (type) {
            case BINARY: {
                BinaryColumnStatisticsData data = catalogColumnStatisticsData.binaryColumnStatisticsData();
                OptionalLong max = OptionalLong.of(data.maximumLength());
                OptionalDouble avg = OptionalDouble.of(data.averageLength());
                OptionalLong nulls = fromMetastoreNullsCount(data.numberOfNulls());
                return createBinaryColumnStatistics(
                        max,
                        getTotalSizeInBytes(avg, rowCount, nulls),
                        nulls);
            }
            case BOOLEAN: {
                BooleanColumnStatisticsData catalogBooleanData = catalogColumnStatisticsData.booleanColumnStatisticsData();
                return createBooleanColumnStatistics(
                        OptionalLong.of(catalogBooleanData.numberOfTrues()),
                        OptionalLong.of(catalogBooleanData.numberOfFalses()),
                        fromMetastoreNullsCount(catalogBooleanData.numberOfNulls()));
            }
            case DATE: {
                DateColumnStatisticsData data = catalogColumnStatisticsData.dateColumnStatisticsData();
                Optional<LocalDate> min = instantToLocalDate(data.minimumValue());
                Optional<LocalDate> max = instantToLocalDate(data.minimumValue());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                return createDateColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case DECIMAL: {
                DecimalColumnStatisticsData data = catalogColumnStatisticsData.decimalColumnStatisticsData();
                Optional<BigDecimal> min = glueDecimalToBigDecimal(data.minimumValue());
                Optional<BigDecimal> max = glueDecimalToBigDecimal(data.maximumValue());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                return createDecimalColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case DOUBLE: {
                DoubleColumnStatisticsData data = catalogColumnStatisticsData.doubleColumnStatisticsData();
                OptionalDouble min = OptionalDouble.of(data.minimumValue());
                OptionalDouble max = OptionalDouble.of(data.maximumValue());
                OptionalLong nulls = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                return createDoubleColumnStatistics(min, max, nulls, fromMetastoreDistinctValuesCount(distinctValues, nulls, rowCount));
            }
            case LONG: {
                LongColumnStatisticsData data = catalogColumnStatisticsData.longColumnStatisticsData();
                OptionalLong min = OptionalLong.of(data.minimumValue());
                OptionalLong max = OptionalLong.of(data.maximumValue());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.numberOfDistinctValues());
                return createIntegerColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case STRING: {
                StringColumnStatisticsData data = catalogColumnStatisticsData.stringColumnStatisticsData();
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
                throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + catalogColumnStatisticsData);
            }
        }

        throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + catalogColumnStatisticsData);
    }

    private static ColumnStatisticsData toGlueColumnStatisticsData(HiveColumnStatistics statistics, HiveType columnType, OptionalLong rowCount)
    {
        TypeInfo typeInfo = columnType.getTypeInfo();
        checkArgument(typeInfo.getCategory() == PRIMITIVE, "Unsupported statistics type: %s", columnType);

        ColumnStatisticsData.Builder catalogColumnStatisticsData = ColumnStatisticsData.builder();

        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN: {
                BooleanColumnStatisticsData.Builder data = BooleanColumnStatisticsData.builder();
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                statistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
                    booleanStatistics.getFalseCount().ifPresent(data::numberOfFalses);
                    booleanStatistics.getTrueCount().ifPresent(data::numberOfTrues);
                });
                catalogColumnStatisticsData.type(ColumnStatisticsType.BOOLEAN.toString());
                catalogColumnStatisticsData.booleanColumnStatisticsData(data.build());
                break;
            }
            case BINARY: {
                BinaryColumnStatisticsData.Builder data = BinaryColumnStatisticsData.builder();
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                data.maximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                data.averageLength(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
                catalogColumnStatisticsData.type(ColumnStatisticsType.BINARY.toString());
                catalogColumnStatisticsData.binaryColumnStatisticsData(data.build());
                break;
            }
            case DATE: {
                DateColumnStatisticsData.Builder data = DateColumnStatisticsData.builder();
                statistics.getDateStatistics().ifPresent(dateStatistics -> {
                    dateStatistics.getMin().ifPresent(value -> data.minimumValue(localDateToInstant(value)));
                    dateStatistics.getMax().ifPresent(value -> data.maximumValue(localDateToInstant(value)));
                });
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                catalogColumnStatisticsData.type(ColumnStatisticsType.DATE.toString());
                catalogColumnStatisticsData.dateColumnStatisticsData(data.build());
                break;
            }
            case DECIMAL: {
                DecimalColumnStatisticsData.Builder data = DecimalColumnStatisticsData.builder();
                statistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
                    decimalStatistics.getMin().ifPresent(value -> data.minimumValue(bigDecimalToGlueDecimal(value)));
                    decimalStatistics.getMax().ifPresent(value -> data.maximumValue(bigDecimalToGlueDecimal(value)));
                });
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                catalogColumnStatisticsData.type(ColumnStatisticsType.DECIMAL.toString());
                catalogColumnStatisticsData.decimalColumnStatisticsData(data.build());
                break;
            }
            case FLOAT:
            case DOUBLE: {
                DoubleColumnStatisticsData.Builder data = DoubleColumnStatisticsData.builder();
                statistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
                    doubleStatistics.getMin().ifPresent(data::minimumValue);
                    doubleStatistics.getMax().ifPresent(data::maximumValue);
                });
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                catalogColumnStatisticsData.type(ColumnStatisticsType.DOUBLE.toString());
                catalogColumnStatisticsData.doubleColumnStatisticsData(data.build());
                break;
            }
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP: {
                LongColumnStatisticsData.Builder data = LongColumnStatisticsData.builder();
                statistics.getIntegerStatistics().ifPresent(stats -> {
                    stats.getMin().ifPresent(data::minimumValue);
                    stats.getMax().ifPresent(data::maximumValue);
                });
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                catalogColumnStatisticsData.type(ColumnStatisticsType.LONG.toString());
                catalogColumnStatisticsData.longColumnStatisticsData(data.build());
                break;
            }
            case VARCHAR:
            case CHAR:
            case STRING: {
                StringColumnStatisticsData.Builder data = StringColumnStatisticsData.builder();
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::numberOfDistinctValues);
                data.maximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                data.averageLength(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
                catalogColumnStatisticsData.type(ColumnStatisticsType.STRING.toString());
                catalogColumnStatisticsData.stringColumnStatisticsData(data.build());
                break;
            }
            default:
                throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics type: " + ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
        }
        return catalogColumnStatisticsData.build();
    }

    private static DecimalNumber bigDecimalToGlueDecimal(BigDecimal decimal)
    {
        Decimal hiveDecimal = new Decimal((short) decimal.scale(), ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
        return DecimalNumber.builder()
                .unscaledValue(SdkBytes.fromByteArray(hiveDecimal.getUnscaled()))
                .scale((int) hiveDecimal.getScale())
                .build();
    }

    private static Optional<BigDecimal> glueDecimalToBigDecimal(DecimalNumber catalogDecimal)
    {
        if (catalogDecimal == null) {
            return Optional.empty();
        }
        Decimal decimal = new Decimal();
        decimal.setUnscaled(catalogDecimal.unscaledValue().asByteBuffer());
        decimal.setScale(catalogDecimal.scale().shortValue());
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
