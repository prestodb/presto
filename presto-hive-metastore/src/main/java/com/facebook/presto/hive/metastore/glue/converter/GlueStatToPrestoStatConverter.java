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
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.StringColumnStatisticsData;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.hive.metastore.api.Decimal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
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
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.getTotalSizeInBytes;

public class GlueStatToPrestoStatConverter
{
    private GlueStatToPrestoStatConverter() {}

    private static final long MILLISECOND_TO_DAY_FACTOR = TimeUnit.DAYS.toMillis(1);

    public static Map<String, com.facebook.presto.hive.metastore.HiveColumnStatistics> convertGlueColumnStatToPrestoColumnStat(final List<ColumnStatistics> catatlogColumnStatisticsList, OptionalLong rowCount)
    {
        Map<String, com.facebook.presto.hive.metastore.HiveColumnStatistics> prestoColumnStat = new HashMap<>();
        for (ColumnStatistics catalogColumnStatistics : catatlogColumnStatisticsList) {
            prestoColumnStat.put(catalogColumnStatistics.getColumnName(), getPrestoColumnStat(catalogColumnStatistics.getStatisticsData(), rowCount));
        }
        return prestoColumnStat;
    }

    private static HiveColumnStatistics getPrestoColumnStat(com.amazonaws.services.glue.model.ColumnStatisticsData catalogColumnStatisticsData, OptionalLong rowCount)
    {
        ColumnStatisticsType type = ColumnStatisticsType.fromValue(catalogColumnStatisticsData.getType());
        switch (type) {
            case BINARY:
                BinaryColumnStatisticsData catalogBinaryData = catalogColumnStatisticsData.getBinaryColumnStatisticsData();
                OptionalLong maxColumnLengthOfBinary = OptionalLong.of(catalogBinaryData.getMaximumLength());
                OptionalDouble averageColumnLengthOfBinary = OptionalDouble.of(catalogBinaryData.getAverageLength());
                OptionalLong nullsCountOfBinary = fromMetastoreNullsCount(catalogBinaryData.getNumberOfNulls());
                return createBinaryColumnStatistics(
                        maxColumnLengthOfBinary,
                        getTotalSizeInBytes(averageColumnLengthOfBinary, rowCount, nullsCountOfBinary),
                        nullsCountOfBinary);

            case BOOLEAN:
                BooleanColumnStatisticsData catalogBooleanData = catalogColumnStatisticsData.getBooleanColumnStatisticsData();
                return createBooleanColumnStatistics(
                        OptionalLong.of(catalogBooleanData.getNumberOfTrues()),
                        OptionalLong.of(catalogBooleanData.getNumberOfFalses()),
                        fromMetastoreNullsCount(catalogBooleanData.getNumberOfNulls()));

            case DATE:
                DateColumnStatisticsData catalogDateData = catalogColumnStatisticsData.getDateColumnStatisticsData();
                Optional<LocalDate> minOfDate = dateToLocalDate(catalogDateData.getMinimumValue());
                Optional<LocalDate> maxOfDate = dateToLocalDate(catalogDateData.getMaximumValue());
                OptionalLong nullsCountOfDate = fromMetastoreNullsCount(catalogDateData.getNumberOfNulls());
                OptionalLong distinctValuesCountOfDate = OptionalLong.of(catalogDateData.getNumberOfDistinctValues());
                return createDateColumnStatistics(minOfDate, maxOfDate, nullsCountOfDate, fromMetastoreDistinctValuesCount(distinctValuesCountOfDate, nullsCountOfDate, rowCount));

            case DECIMAL:
                DecimalColumnStatisticsData catalogDecimalData = catalogColumnStatisticsData.getDecimalColumnStatisticsData();
                Optional<BigDecimal> minOfDecimal = glueDecimalToBigDecimal(catalogDecimalData.getMinimumValue());
                Optional<BigDecimal> maxOfDecimal = glueDecimalToBigDecimal(catalogDecimalData.getMaximumValue());
                OptionalLong distinctValuesCountOfDecimal = OptionalLong.of(catalogDecimalData.getNumberOfDistinctValues());
                OptionalLong nullsCountOfDecimal = fromMetastoreNullsCount(catalogDecimalData.getNumberOfNulls());
                return createDecimalColumnStatistics(minOfDecimal, maxOfDecimal, nullsCountOfDecimal, fromMetastoreDistinctValuesCount(distinctValuesCountOfDecimal, nullsCountOfDecimal, rowCount));

            case DOUBLE:
                DoubleColumnStatisticsData catalogDoubleData = catalogColumnStatisticsData.getDoubleColumnStatisticsData();
                OptionalDouble minOfDouble = OptionalDouble.of(catalogDoubleData.getMinimumValue());
                OptionalDouble maxOfDouble = OptionalDouble.of(catalogDoubleData.getMaximumValue());
                OptionalLong nullsCountOfDouble = fromMetastoreNullsCount(catalogDoubleData.getNumberOfNulls());
                OptionalLong distinctValuesCountOfDouble = OptionalLong.of(catalogDoubleData.getNumberOfDistinctValues());
                return createDoubleColumnStatistics(minOfDouble, maxOfDouble, nullsCountOfDouble, fromMetastoreDistinctValuesCount(distinctValuesCountOfDouble, nullsCountOfDouble, rowCount));

            case LONG:
                LongColumnStatisticsData catalogLongData = catalogColumnStatisticsData.getLongColumnStatisticsData();
                OptionalLong minOfLong = OptionalLong.of(catalogLongData.getMinimumValue());
                OptionalLong maxOfLong = OptionalLong.of(catalogLongData.getMaximumValue());
                OptionalLong nullsCountOfLong = fromMetastoreNullsCount(catalogLongData.getNumberOfNulls());
                OptionalLong distinctValuesCountOfLong = OptionalLong.of(catalogLongData.getNumberOfDistinctValues());
                return createIntegerColumnStatistics(minOfLong, maxOfLong, nullsCountOfLong, fromMetastoreDistinctValuesCount(distinctValuesCountOfLong, nullsCountOfLong, rowCount));

            case STRING:
                StringColumnStatisticsData catalogStringData = catalogColumnStatisticsData.getStringColumnStatisticsData();
                OptionalLong maxColumnLengthOfString = OptionalLong.of(catalogStringData.getMaximumLength());
                OptionalDouble averageColumnLengthOfString = OptionalDouble.of(catalogStringData.getAverageLength());
                OptionalLong nullsCountOfString = fromMetastoreNullsCount(catalogStringData.getNumberOfNulls());
                OptionalLong distinctValuesCountOfString = OptionalLong.of(catalogStringData.getNumberOfDistinctValues());

                return createStringColumnStatistics(
                        maxColumnLengthOfString,
                        getTotalSizeInBytes(averageColumnLengthOfString, rowCount, nullsCountOfString),
                        nullsCountOfString,
                        fromMetastoreDistinctValuesCount(distinctValuesCountOfString, nullsCountOfString, rowCount));
        }
        throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + catalogColumnStatisticsData);
    }

    private static Optional<BigDecimal> glueDecimalToBigDecimal(com.amazonaws.services.glue.model.DecimalNumber catalogDecimal)
    {
        Decimal decimal = new Decimal();
        decimal.setUnscaled(catalogDecimal.getUnscaledValue());
        decimal.setScale(catalogDecimal.getScale().shortValue());
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    private static Optional<LocalDate> dateToLocalDate(Date date)
    {
        long daysSinceEpoch = date.getTime() / MILLISECOND_TO_DAY_FACTOR;
        return Optional.of(LocalDate.ofEpochDay(daysSinceEpoch));
    }
}
