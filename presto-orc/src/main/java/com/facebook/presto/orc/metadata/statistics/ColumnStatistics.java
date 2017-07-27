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
package com.facebook.presto.orc.metadata.statistics;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.orc.metadata.statistics.BooleanStatisticsBuilder.mergeBooleanStatistics;
import static com.facebook.presto.orc.metadata.statistics.DateStatisticsBuilder.mergeDateStatistics;
import static com.facebook.presto.orc.metadata.statistics.DoubleStatisticsBuilder.mergeDoubleStatistics;
import static com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder.mergeIntegerStatistics;
import static com.facebook.presto.orc.metadata.statistics.LongDecimalStatisticsBuilder.mergeDecimalStatistics;
import static com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder.mergeStringStatistics;
import static com.facebook.presto.orc.metadata.statistics.TimestampStatisticsBuilder.mergeTimestampStatistics;
import static com.google.common.base.MoreObjects.toStringHelper;

public class ColumnStatistics
{
    private final Long numberOfValues;
    private final BooleanStatistics booleanStatistics;
    private final IntegerStatistics integerStatistics;
    private final DoubleStatistics doubleStatistics;
    private final StringStatistics stringStatistics;
    private final DateStatistics dateStatistics;
    private final DecimalStatistics decimalStatistics;
    private final TimestampStatistics timestampStatistics;
    private final HiveBloomFilter bloomFilter;

    public ColumnStatistics(
            Long numberOfValues,
            BooleanStatistics booleanStatistics,
            IntegerStatistics integerStatistics,
            DoubleStatistics doubleStatistics,
            StringStatistics stringStatistics,
            DateStatistics dateStatistics,
            TimestampStatistics timestampStatistics,
            DecimalStatistics decimalStatistics,
            HiveBloomFilter bloomFilter)
    {
        this.numberOfValues = numberOfValues;
        this.booleanStatistics = booleanStatistics;
        this.integerStatistics = integerStatistics;
        this.doubleStatistics = doubleStatistics;
        this.stringStatistics = stringStatistics;
        this.dateStatistics = dateStatistics;
        this.decimalStatistics = decimalStatistics;
        this.timestampStatistics = timestampStatistics;
        this.bloomFilter = bloomFilter;
    }

    public boolean hasNumberOfValues()
    {
        return numberOfValues != null;
    }

    public long getNumberOfValues()
    {
        return numberOfValues == null ? 0 : numberOfValues;
    }

    public BooleanStatistics getBooleanStatistics()
    {
        return booleanStatistics;
    }

    public DateStatistics getDateStatistics()
    {
        return dateStatistics;
    }

    public DoubleStatistics getDoubleStatistics()
    {
        return doubleStatistics;
    }

    public IntegerStatistics getIntegerStatistics()
    {
        return integerStatistics;
    }

    public StringStatistics getStringStatistics()
    {
        return stringStatistics;
    }

    public DecimalStatistics getDecimalStatistics()
    {
        return decimalStatistics;
    }

    public TimestampStatistics getTimestampStatistics()
    {
        return timestampStatistics;
    }

    public HiveBloomFilter getBloomFilter()
    {
        return bloomFilter;
    }

    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new ColumnStatistics(
                numberOfValues,
                booleanStatistics,
                integerStatistics,
                doubleStatistics,
                stringStatistics,
                dateStatistics,
                timestampStatistics,
                decimalStatistics,
                bloomFilter);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnStatistics that = (ColumnStatistics) o;
        return Objects.equals(numberOfValues, that.numberOfValues) &&
                Objects.equals(booleanStatistics, that.booleanStatistics) &&
                Objects.equals(integerStatistics, that.integerStatistics) &&
                Objects.equals(doubleStatistics, that.doubleStatistics) &&
                Objects.equals(stringStatistics, that.stringStatistics) &&
                Objects.equals(dateStatistics, that.dateStatistics) &&
                Objects.equals(timestampStatistics, that.timestampStatistics) &&
                Objects.equals(decimalStatistics, that.decimalStatistics) &&
                Objects.equals(bloomFilter, that.bloomFilter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(numberOfValues, booleanStatistics, integerStatistics, doubleStatistics, stringStatistics, dateStatistics, decimalStatistics, bloomFilter);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("numberOfValues", numberOfValues)
                .add("booleanStatistics", booleanStatistics)
                .add("integerStatistics", integerStatistics)
                .add("doubleStatistics", doubleStatistics)
                .add("stringStatistics", stringStatistics)
                .add("dateStatistics", dateStatistics)
                .add("decimalStatistics", decimalStatistics)
                .add("bloomFilter", bloomFilter)
                .toString();
    }

    public static ColumnStatistics mergeColumnStatistics(List<ColumnStatistics> stats)
    {
        long numberOfRows = stats.stream()
                .mapToLong(ColumnStatistics::getNumberOfValues)
                .sum();

        return new ColumnStatistics(
                numberOfRows,
                mergeBooleanStatistics(stats).orElse(null),
                mergeIntegerStatistics(stats).orElse(null),
                mergeDoubleStatistics(stats).orElse(null),
                mergeStringStatistics(stats).orElse(null),
                mergeDateStatistics(stats).orElse(null),
                mergeTimestampStatistics(stats).orElse(null),
                mergeDecimalStatistics(stats).orElse(null),
                null);
    }
}
