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

import com.amazonaws.services.glue.model.BinaryColumnStatisticsData;
import com.amazonaws.services.glue.model.BooleanColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalNumber;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.StringColumnStatisticsData;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Date;
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
                    HiveColumnStatistics> trinoColumnStats,
            OptionalLong rowCount)
    {
        return partition.getColumns().stream()
                .filter(column -> trinoColumnStats.containsKey(column.getName()))
                .map(c -> toColumnStatistics(c, trinoColumnStats.get(c.getName()), rowCount))
                .collect(toImmutableList());
    }

    public static List<ColumnStatistics> toGlueColumnStatistics(
            Table table,
            Map<String, HiveColumnStatistics> trinoColumnStats,
            OptionalLong rowCount)
    {
        return trinoColumnStats.entrySet().stream()
                .map(e -> toColumnStatistics(table.getColumn(e.getKey()).get(), e.getValue(), rowCount))
                .collect(toImmutableList());
    }

    private static ColumnStatistics toColumnStatistics(Column column, HiveColumnStatistics statistics, OptionalLong rowCount)
    {
        ColumnStatistics columnStatistics = new ColumnStatistics();
        HiveType columnType = column.getType();
        columnStatistics.setColumnName(column.getName());
        columnStatistics.setColumnType(columnType.toString());
        ColumnStatisticsData catalogColumnStatisticsData = toGlueColumnStatisticsData(statistics, columnType, rowCount);
        columnStatistics.setStatisticsData(catalogColumnStatisticsData);
        columnStatistics.setAnalyzedTime(new Date());
        return columnStatistics;
    }

    public static HiveColumnStatistics fromGlueColumnStatistics(ColumnStatisticsData catalogColumnStatisticsData, OptionalLong rowCount)
    {
        ColumnStatisticsType type = ColumnStatisticsType.fromValue(catalogColumnStatisticsData.getType());
        switch (type) {
            case BINARY: {
                BinaryColumnStatisticsData data = catalogColumnStatisticsData.getBinaryColumnStatisticsData();
                OptionalLong max = OptionalLong.of(data.getMaximumLength());
                OptionalDouble avg = OptionalDouble.of(data.getAverageLength());
                OptionalLong nulls = fromMetastoreNullsCount(data.getNumberOfNulls());
                return createBinaryColumnStatistics(
                        max,
                        getTotalSizeInBytes(avg, rowCount, nulls),
                        nulls);
            }
            case BOOLEAN: {
                BooleanColumnStatisticsData catalogBooleanData = catalogColumnStatisticsData.getBooleanColumnStatisticsData();
                return createBooleanColumnStatistics(
                        OptionalLong.of(catalogBooleanData.getNumberOfTrues()),
                        OptionalLong.of(catalogBooleanData.getNumberOfFalses()),
                        fromMetastoreNullsCount(catalogBooleanData.getNumberOfNulls()));
            }
            case DATE: {
                DateColumnStatisticsData data = catalogColumnStatisticsData.getDateColumnStatisticsData();
                Optional<LocalDate> min = dateToLocalDate(data.getMinimumValue());
                Optional<LocalDate> max = dateToLocalDate(data.getMaximumValue());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.getNumberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.getNumberOfDistinctValues());
                return createDateColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case DECIMAL: {
                DecimalColumnStatisticsData data = catalogColumnStatisticsData.getDecimalColumnStatisticsData();
                Optional<BigDecimal> min = glueDecimalToBigDecimal(data.getMinimumValue());
                Optional<BigDecimal> max = glueDecimalToBigDecimal(data.getMaximumValue());
                OptionalLong distinctValues = OptionalLong.of(data.getNumberOfDistinctValues());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.getNumberOfNulls());
                return createDecimalColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case DOUBLE: {
                DoubleColumnStatisticsData data = catalogColumnStatisticsData.getDoubleColumnStatisticsData();
                OptionalDouble min = OptionalDouble.of(data.getMinimumValue());
                OptionalDouble max = OptionalDouble.of(data.getMaximumValue());
                OptionalLong nulls = fromMetastoreNullsCount(data.getNumberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.getNumberOfDistinctValues());
                return createDoubleColumnStatistics(min, max, nulls, fromMetastoreDistinctValuesCount(distinctValues, nulls, rowCount));
            }
            case LONG: {
                LongColumnStatisticsData data = catalogColumnStatisticsData.getLongColumnStatisticsData();
                OptionalLong min = OptionalLong.of(data.getMinimumValue());
                OptionalLong max = OptionalLong.of(data.getMaximumValue());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.getNumberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.getNumberOfDistinctValues());
                return createIntegerColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
            case STRING: {
                StringColumnStatisticsData data = catalogColumnStatisticsData.getStringColumnStatisticsData();
                OptionalLong max = OptionalLong.of(data.getMaximumLength());
                OptionalDouble avg = OptionalDouble.of(data.getAverageLength());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.getNumberOfNulls());
                OptionalLong distinctValues = OptionalLong.of(data.getNumberOfDistinctValues());
                return createStringColumnStatistics(
                        max,
                        getTotalSizeInBytes(avg, rowCount, nullsCount),
                        nullsCount,
                        fromMetastoreDistinctValuesCount(distinctValues, nullsCount, rowCount));
            }
        }

        throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + catalogColumnStatisticsData);
    }

    private static ColumnStatisticsData toGlueColumnStatisticsData(HiveColumnStatistics statistics, HiveType columnType, OptionalLong rowCount)
    {
        TypeInfo typeInfo = columnType.getTypeInfo();
        checkArgument(typeInfo.getCategory() == PRIMITIVE, "Unsupported statistics type: %s", columnType);

        ColumnStatisticsData catalogColumnStatisticsData = new ColumnStatisticsData();

        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN: {
                BooleanColumnStatisticsData data = new BooleanColumnStatisticsData();
                statistics.getNullsCount().ifPresent(data::setNumberOfNulls);
                statistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
                    booleanStatistics.getFalseCount().ifPresent(data::setNumberOfFalses);
                    booleanStatistics.getTrueCount().ifPresent(data::setNumberOfTrues);
                });
                catalogColumnStatisticsData.setType(ColumnStatisticsType.BOOLEAN.toString());
                catalogColumnStatisticsData.setBooleanColumnStatisticsData(data);
                break;
            }
            case BINARY: {
                BinaryColumnStatisticsData data = new BinaryColumnStatisticsData();
                statistics.getNullsCount().ifPresent(data::setNumberOfNulls);
                data.setMaximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                data.setAverageLength(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
                catalogColumnStatisticsData.setType(ColumnStatisticsType.BINARY.toString());
                catalogColumnStatisticsData.setBinaryColumnStatisticsData(data);
                break;
            }
            case DATE: {
                DateColumnStatisticsData data = new DateColumnStatisticsData();
                statistics.getDateStatistics().ifPresent(dateStatistics -> {
                    dateStatistics.getMin().ifPresent(value -> data.setMinimumValue(localDateToDate(value)));
                    dateStatistics.getMax().ifPresent(value -> data.setMaximumValue(localDateToDate(value)));
                });
                statistics.getNullsCount().ifPresent(data::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DATE.toString());
                catalogColumnStatisticsData.setDateColumnStatisticsData(data);
                break;
            }
            case DECIMAL: {
                DecimalColumnStatisticsData data = new DecimalColumnStatisticsData();
                statistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
                    decimalStatistics.getMin().ifPresent(value -> data.setMinimumValue(bigDecimalToGlueDecimal(value)));
                    decimalStatistics.getMax().ifPresent(value -> data.setMaximumValue(bigDecimalToGlueDecimal(value)));
                });
                statistics.getNullsCount().ifPresent(data::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DECIMAL.toString());
                catalogColumnStatisticsData.setDecimalColumnStatisticsData(data);
                break;
            }
            case FLOAT:
            case DOUBLE: {
                DoubleColumnStatisticsData data = new DoubleColumnStatisticsData();
                statistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
                    doubleStatistics.getMin().ifPresent(data::setMinimumValue);
                    doubleStatistics.getMax().ifPresent(data::setMaximumValue);
                });
                statistics.getNullsCount().ifPresent(data::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DOUBLE.toString());
                catalogColumnStatisticsData.setDoubleColumnStatisticsData(data);
                break;
            }
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP: {
                LongColumnStatisticsData data = new LongColumnStatisticsData();
                statistics.getIntegerStatistics().ifPresent(stats -> {
                    stats.getMin().ifPresent(data::setMinimumValue);
                    stats.getMax().ifPresent(data::setMaximumValue);
                });
                statistics.getNullsCount().ifPresent(data::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.LONG.toString());
                catalogColumnStatisticsData.setLongColumnStatisticsData(data);
                break;
            }
            case VARCHAR:
            case CHAR:
            case STRING: {
                StringColumnStatisticsData data = new StringColumnStatisticsData();
                statistics.getNullsCount().ifPresent(data::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumberOfDistinctValues);
                data.setMaximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                data.setAverageLength(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
                catalogColumnStatisticsData.setType(ColumnStatisticsType.STRING.toString());
                catalogColumnStatisticsData.setStringColumnStatisticsData(data);
                break;
            }
            default:
                throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics type: " + ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
        }
        return catalogColumnStatisticsData;
    }

    private static DecimalNumber bigDecimalToGlueDecimal(BigDecimal decimal)
    {
        Decimal hiveDecimal = new Decimal((short) decimal.scale(), ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
        DecimalNumber catalogDecimal = new DecimalNumber();
        catalogDecimal.setUnscaledValue(ByteBuffer.wrap(hiveDecimal.getUnscaled()));
        catalogDecimal.setScale((int) hiveDecimal.getScale());
        return catalogDecimal;
    }

    private static Optional<BigDecimal> glueDecimalToBigDecimal(DecimalNumber catalogDecimal)
    {
        if (catalogDecimal == null) {
            return Optional.empty();
        }
        Decimal decimal = new Decimal();
        decimal.setUnscaled(catalogDecimal.getUnscaledValue());
        decimal.setScale(catalogDecimal.getScale().shortValue());
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    private static Optional<LocalDate> dateToLocalDate(Date date)
    {
        if (date == null) {
            return Optional.empty();
        }
        long daysSinceEpoch = date.getTime() / MILLIS_PER_DAY;
        return Optional.of(LocalDate.ofEpochDay(daysSinceEpoch));
    }

    private static Date localDateToDate(LocalDate date)
    {
        long millisecondsSinceEpoch = date.toEpochDay() * MILLIS_PER_DAY;
        return new Date(millisecondsSinceEpoch);
    }
}
