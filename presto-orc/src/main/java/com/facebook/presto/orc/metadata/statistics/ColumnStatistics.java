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

import com.facebook.presto.orc.metadata.statistics.StatisticsHasher.Hashable;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.orc.metadata.statistics.BinaryStatisticsBuilder.mergeBinaryStatistics;
import static com.facebook.presto.orc.metadata.statistics.BooleanStatisticsBuilder.mergeBooleanStatistics;
import static com.facebook.presto.orc.metadata.statistics.DateStatisticsBuilder.mergeDateStatistics;
import static com.facebook.presto.orc.metadata.statistics.DoubleStatisticsBuilder.mergeDoubleStatistics;
import static com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder.mergeIntegerStatistics;
import static com.facebook.presto.orc.metadata.statistics.LongDecimalStatisticsBuilder.mergeDecimalStatistics;
import static com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder.mergeStringStatistics;
import static com.google.common.base.MoreObjects.toStringHelper;

public class ColumnStatistics
        implements Hashable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ColumnStatistics.class).instanceSize();

    private final boolean hasNumberOfValues;
    private final long numberOfValues;
    private final long minAverageValueSizeInBytes;
    private final BooleanStatistics booleanStatistics;
    private final IntegerStatistics integerStatistics;
    private final DoubleStatistics doubleStatistics;
    private final StringStatistics stringStatistics;
    private final DateStatistics dateStatistics;
    private final DecimalStatistics decimalStatistics;
    private final BinaryStatistics binaryStatistics;
    private final HiveBloomFilter bloomFilter;

    private ColumnStatistics(
            Long numberOfValues,
            long minAverageValueSizeInBytes,
            BooleanStatistics booleanStatistics,
            IntegerStatistics integerStatistics,
            DoubleStatistics doubleStatistics,
            StringStatistics stringStatistics,
            DateStatistics dateStatistics,
            DecimalStatistics decimalStatistics,
            BinaryStatistics binaryStatistics,
            HiveBloomFilter bloomFilter)
    {
        this.hasNumberOfValues = numberOfValues != null;
        this.numberOfValues = hasNumberOfValues ? numberOfValues : 0;
        this.minAverageValueSizeInBytes = minAverageValueSizeInBytes;
        this.booleanStatistics = booleanStatistics;
        this.integerStatistics = integerStatistics;
        this.doubleStatistics = doubleStatistics;
        this.stringStatistics = stringStatistics;
        this.dateStatistics = dateStatistics;
        this.decimalStatistics = decimalStatistics;
        this.binaryStatistics = binaryStatistics;
        this.bloomFilter = bloomFilter;
    }

    public boolean hasNumberOfValues()
    {
        return hasNumberOfValues;
    }

    public long getNumberOfValues()
    {
        return hasNumberOfValues ? numberOfValues : 0;
    }

    public boolean hasMinAverageValueSizeInBytes()
    {
        return hasNumberOfValues() && numberOfValues > 0;
    }

    /**
     * The minimum average value sizes.
     * The actual average value size is no less than the return value.
     * It provides a lower bound of the size of data to be loaded
     */
    public long getMinAverageValueSizeInBytes()
    {
        // it is ok to return 0 if the size does not exist given it is a lower bound
        return minAverageValueSizeInBytes;
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

    public BinaryStatistics getBinaryStatistics()
    {
        return binaryStatistics;
    }

    public HiveBloomFilter getBloomFilter()
    {
        return bloomFilter;
    }

    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return createColumnStatistics(
                getNumberOfValues(),
                minAverageValueSizeInBytes,
                getBooleanStatistics(),
                getIntegerStatistics(),
                getDoubleStatistics(),
                getStringStatistics(),
                getDateStatistics(),
                getDecimalStatistics(),
                getBinaryStatistics(),
                bloomFilter);
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
        if (getBooleanStatistics() != null) {
            retainedSizeInBytes += getBooleanStatistics().getRetainedSizeInBytes();
        }
        if (getIntegerStatistics() != null) {
            retainedSizeInBytes += getIntegerStatistics().getRetainedSizeInBytes();
        }
        if (getDoubleStatistics() != null) {
            retainedSizeInBytes += getDoubleStatistics().getRetainedSizeInBytes();
        }
        if (getStringStatistics() != null) {
            retainedSizeInBytes += getStringStatistics().getRetainedSizeInBytes();
        }
        if (getDateStatistics() != null) {
            retainedSizeInBytes += getDateStatistics().getRetainedSizeInBytes();
        }
        if (getDecimalStatistics() != null) {
            retainedSizeInBytes += getDecimalStatistics().getRetainedSizeInBytes();
        }
        if (getBinaryStatistics() != null) {
            retainedSizeInBytes += getBinaryStatistics().getRetainedSizeInBytes();
        }
        if (getBloomFilter() != null) {
            retainedSizeInBytes += getBloomFilter().getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
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
        return Objects.equals(hasNumberOfValues, that.hasNumberOfValues) &&
                Objects.equals(getNumberOfValues(), that.getNumberOfValues()) &&
                Objects.equals(getBooleanStatistics(), that.getBooleanStatistics()) &&
                Objects.equals(getIntegerStatistics(), that.getIntegerStatistics()) &&
                Objects.equals(getDoubleStatistics(), that.getDoubleStatistics()) &&
                Objects.equals(getStringStatistics(), that.getStringStatistics()) &&
                Objects.equals(getDateStatistics(), that.getDateStatistics()) &&
                Objects.equals(getDecimalStatistics(), that.getDecimalStatistics()) &&
                Objects.equals(getBinaryStatistics(), that.getBinaryStatistics()) &&
                Objects.equals(getBloomFilter(), that.getBloomFilter());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                hasNumberOfValues,
                getNumberOfValues(),
                getBooleanStatistics(),
                getIntegerStatistics(),
                getDoubleStatistics(),
                getStringStatistics(),
                getDateStatistics(),
                getDecimalStatistics(),
                getBinaryStatistics(),
                getBloomFilter());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("numberOfValues", getNumberOfValues())
                .add("booleanStatistics", getBooleanStatistics())
                .add("integerStatistics", getIntegerStatistics())
                .add("doubleStatistics", getDoubleStatistics())
                .add("stringStatistics", getStringStatistics())
                .add("dateStatistics", getDateStatistics())
                .add("decimalStatistics", getDecimalStatistics())
                .add("binaryStatistics", getBinaryStatistics())
                .add("bloomFilter", getBloomFilter())
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putOptionalLong(hasNumberOfValues, numberOfValues)
                .putOptionalHashable(getBooleanStatistics())
                .putOptionalHashable(getIntegerStatistics())
                .putOptionalHashable(getDoubleStatistics())
                .putOptionalHashable(getStringStatistics())
                .putOptionalHashable(getDateStatistics())
                .putOptionalHashable(getDecimalStatistics())
                .putOptionalHashable(getBinaryStatistics())
                .putOptionalHashable(getBloomFilter());
    }

    public static ColumnStatistics mergeColumnStatistics(List<ColumnStatistics> stats)
    {
        long numberOfRows = stats.stream()
                .mapToLong(ColumnStatistics::getNumberOfValues)
                .sum();

        long minAverageValueBytes = 0;
        if (numberOfRows > 0) {
            minAverageValueBytes = stats.stream()
                    .mapToLong(s -> s.getMinAverageValueSizeInBytes() * s.getNumberOfValues())
                    .sum() / numberOfRows;
        }

        return createColumnStatistics(
                numberOfRows,
                minAverageValueBytes,
                mergeBooleanStatistics(stats).orElse(null),
                mergeIntegerStatistics(stats).orElse(null),
                mergeDoubleStatistics(stats).orElse(null),
                mergeStringStatistics(stats).orElse(null),
                mergeDateStatistics(stats).orElse(null),
                mergeDecimalStatistics(stats).orElse(null),
                mergeBinaryStatistics(stats).orElse(null),
                null);
    }

    public static ColumnStatistics createColumnStatistics(
            Long numberOfValues,
            long minAverageValueSizeInBytes,
            BooleanStatistics booleanStatistics,
            IntegerStatistics integerStatistics,
            DoubleStatistics doubleStatistics,
            StringStatistics stringStatistics,
            DateStatistics dateStatistics,
            DecimalStatistics decimalStatistics,
            BinaryStatistics binaryStatistics,
            HiveBloomFilter bloomFilter)
    {
        return new ColumnStatistics(
                numberOfValues,
                minAverageValueSizeInBytes,
                booleanStatistics,
                integerStatistics,
                doubleStatistics,
                stringStatistics,
                dateStatistics,
                decimalStatistics,
                binaryStatistics,
                bloomFilter);
    }
}
