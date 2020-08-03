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
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.StringColumnStatisticsData;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.getAverageColumnLength;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreDistinctValuesCount;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

public class PrestoStatToGlueStatConverter
{
    private PrestoStatToGlueStatConverter() {}

    private static final long DAY_TO_MILLISECOND_FACTOR = TimeUnit.DAYS.toMillis(1);

    public static List<com.amazonaws.services.glue.model.ColumnStatistics> convertPrestoColumnStatToGlueColumnStat(Table table, Map<String, com.facebook.presto.hive.metastore.HiveColumnStatistics> prestoColumnStats, OptionalLong rowCount)
    {
        List<com.amazonaws.services.glue.model.ColumnStatistics> catalogColumnStatisticsList = new ArrayList<>();

        prestoColumnStats.forEach((columnName, statistics) -> {
            HiveType columnType = table.getColumn(columnName).get().getType();
            com.amazonaws.services.glue.model.ColumnStatistics catalogColumnStatistics = new com.amazonaws.services.glue.model.ColumnStatistics();

            catalogColumnStatistics.setColumnName(columnName);
            catalogColumnStatistics.setColumnType(columnType.toString());
            com.amazonaws.services.glue.model.ColumnStatisticsData catalogColumnStatisticsData = getGlueColumnStatisticsData(statistics, columnType, rowCount);
            catalogColumnStatistics.setStatisticsData(catalogColumnStatisticsData);
            catalogColumnStatistics.setAnalyzedTime(new Date());
            catalogColumnStatisticsList.add(catalogColumnStatistics);
        });

        return catalogColumnStatisticsList;
    }

    private static com.amazonaws.services.glue.model.ColumnStatisticsData getGlueColumnStatisticsData(com.facebook.presto.hive.metastore.HiveColumnStatistics statistics, HiveType columnType, OptionalLong rowCount)
    {
        TypeInfo typeInfo = columnType.getTypeInfo();
        checkArgument(typeInfo.getCategory() == PRIMITIVE, "unsupported type: %s", columnType);

        com.amazonaws.services.glue.model.ColumnStatisticsData catalogColumnStatisticsData = new com.amazonaws.services.glue.model.ColumnStatisticsData();

        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN:
                BooleanColumnStatisticsData catalogBooleanData = new BooleanColumnStatisticsData();
                statistics.getNullsCount().ifPresent(catalogBooleanData::setNumberOfNulls);
                statistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
                    booleanStatistics.getFalseCount().ifPresent(catalogBooleanData::setNumberOfFalses);
                    booleanStatistics.getTrueCount().ifPresent(catalogBooleanData::setNumberOfTrues);
                });
                catalogColumnStatisticsData.setType(ColumnStatisticsType.BOOLEAN.toString());
                catalogColumnStatisticsData.setBooleanColumnStatisticsData(catalogBooleanData);
                break;
            case BINARY:
                BinaryColumnStatisticsData catalogBinaryData = new BinaryColumnStatisticsData();
                statistics.getNullsCount().ifPresent(catalogBinaryData::setNumberOfNulls);
                catalogBinaryData.setMaximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                catalogBinaryData.setAverageLength(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
                catalogColumnStatisticsData.setType(ColumnStatisticsType.BINARY.toString());
                catalogColumnStatisticsData.setBinaryColumnStatisticsData(catalogBinaryData);
                break;
            case DATE:
                DateColumnStatisticsData catalogDateData = new DateColumnStatisticsData();
                statistics.getDateStatistics().ifPresent(dateStatistics -> {
                    dateStatistics.getMin().ifPresent(value -> catalogDateData.setMinimumValue(localDateToDate(value)));
                    dateStatistics.getMax().ifPresent(value -> catalogDateData.setMaximumValue(localDateToDate(value)));
                });
                statistics.getNullsCount().ifPresent(catalogDateData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogDateData::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DATE.toString());
                catalogColumnStatisticsData.setDateColumnStatisticsData(catalogDateData);
                break;
            case DECIMAL:
                DecimalColumnStatisticsData catalogDecimalData = new DecimalColumnStatisticsData();
                statistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
                    decimalStatistics.getMin().ifPresent(value -> catalogDecimalData.setMinimumValue(bigDecimalToGlueDecimal(value)));
                    decimalStatistics.getMax().ifPresent(value -> catalogDecimalData.setMaximumValue(bigDecimalToGlueDecimal(value)));
                });
                statistics.getNullsCount().ifPresent(catalogDecimalData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogDecimalData::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DECIMAL.toString());
                catalogColumnStatisticsData.setDecimalColumnStatisticsData(catalogDecimalData);
                break;
            case FLOAT:
            case DOUBLE:
                DoubleColumnStatisticsData catalogDoubleData = new DoubleColumnStatisticsData();
                statistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
                    doubleStatistics.getMin().ifPresent(catalogDoubleData::setMinimumValue);
                    doubleStatistics.getMax().ifPresent(catalogDoubleData::setMaximumValue);
                });
                statistics.getNullsCount().ifPresent(catalogDoubleData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogDoubleData::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DOUBLE.toString());
                catalogColumnStatisticsData.setDoubleColumnStatisticsData(catalogDoubleData);
                break;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP:
                LongColumnStatisticsData catalogLongData = new LongColumnStatisticsData();
                statistics.getIntegerStatistics().ifPresent(integerStatistics -> {
                    integerStatistics.getMin().ifPresent(catalogLongData::setMinimumValue);
                    integerStatistics.getMax().ifPresent(catalogLongData::setMaximumValue);
                });
                statistics.getNullsCount().ifPresent(catalogLongData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogLongData::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.LONG.toString());
                catalogColumnStatisticsData.setLongColumnStatisticsData(catalogLongData);
                break;
            case VARCHAR:
            case CHAR:
            case STRING:
                StringColumnStatisticsData catalogStringData = new StringColumnStatisticsData();
                statistics.getNullsCount().ifPresent(catalogStringData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogStringData::setNumberOfDistinctValues);
                catalogStringData.setMaximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                catalogStringData.setAverageLength(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
                catalogColumnStatisticsData.setType(ColumnStatisticsType.STRING.toString());
                catalogColumnStatisticsData.setStringColumnStatisticsData(catalogStringData);
                break;
            default:
                throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics type: " + statistics);
        }
        return catalogColumnStatisticsData;
    }

    private static com.amazonaws.services.glue.model.DecimalNumber bigDecimalToGlueDecimal(BigDecimal decimal)
    {
        Decimal hiveDecimal = new Decimal(ByteBuffer.wrap(decimal.unscaledValue().toByteArray()), (short) decimal.scale());
        com.amazonaws.services.glue.model.DecimalNumber catalogDecimal =
                new com.amazonaws.services.glue.model.DecimalNumber();
        catalogDecimal.setUnscaledValue(ByteBuffer.wrap(hiveDecimal.getUnscaled()));
        catalogDecimal.setScale((int) hiveDecimal.getScale());
        return catalogDecimal;
    }

    private static Date localDateToDate(LocalDate date)
    {
        long millisecondsSinceEpoch = date.toEpochDay() * DAY_TO_MILLISECOND_FACTOR;
        return new Date(millisecondsSinceEpoch);
    }
}
