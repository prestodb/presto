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
import com.google.common.base.MoreObjects.ToStringHelper;
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
    private final HiveBloomFilter bloomFilter;

    public ColumnStatistics(
            Long numberOfValues,
            HiveBloomFilter bloomFilter)
    {
        this.hasNumberOfValues = numberOfValues != null;
        this.numberOfValues = hasNumberOfValues ? numberOfValues : 0;
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
        return 0;
    }

    public BooleanStatistics getBooleanStatistics()
    {
        return null;
    }

    public DateStatistics getDateStatistics()
    {
        return null;
    }

    public DoubleStatistics getDoubleStatistics()
    {
        return null;
    }

    public IntegerStatistics getIntegerStatistics()
    {
        return null;
    }

    public StringStatistics getStringStatistics()
    {
        return null;
    }

    public DecimalStatistics getDecimalStatistics()
    {
        return null;
    }

    public BinaryStatistics getBinaryStatistics()
    {
        return null;
    }

    public HiveBloomFilter getBloomFilter()
    {
        return bloomFilter;
    }

    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new ColumnStatistics(getNumberOfValues(), bloomFilter);
    }

    protected final long getMembersSizeInBytes()
    {
        return bloomFilter == null ? 0 : bloomFilter.getRetainedSizeInBytes();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getMembersSizeInBytes();
    }

    protected final boolean equalsInternal(ColumnStatistics that)
    {
        return hasNumberOfValues == that.hasNumberOfValues &&
                numberOfValues == that.numberOfValues &&
                Objects.equals(getBloomFilter(), that.getBloomFilter());
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
        return equalsInternal(that);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                hasNumberOfValues,
                getNumberOfValues(),
                getBloomFilter());
    }

    protected ToStringHelper getToStringHelper()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("numberOfValues", getNumberOfValues())
                .add("bloomFilter", getBloomFilter());
    }

    @Override
    public String toString()
    {
        return getToStringHelper().toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putOptionalLong(hasNumberOfValues, numberOfValues)
                .putOptionalHashable(getBloomFilter());
    }

    public static ColumnStatistics mergeColumnStatistics(List<ColumnStatistics> stats)
    {
        if (stats.size() == 0) {
            return new ColumnStatistics(0L, null);
        }

        long numberOfRows = stats.stream()
                .mapToLong(ColumnStatistics::getNumberOfValues)
                .sum();

        return createColumnStatistics(
                numberOfRows,
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
            BooleanStatistics booleanStatistics,
            IntegerStatistics integerStatistics,
            DoubleStatistics doubleStatistics,
            StringStatistics stringStatistics,
            DateStatistics dateStatistics,
            DecimalStatistics decimalStatistics,
            BinaryStatistics binaryStatistics,
            HiveBloomFilter bloomFilter)
    {
        if (booleanStatistics != null) {
            return new BooleanColumnStatistics(numberOfValues, bloomFilter, booleanStatistics);
        }

        if (integerStatistics != null) {
            return new IntegerColumnStatistics(numberOfValues, bloomFilter, integerStatistics);
        }

        if (doubleStatistics != null) {
            return new DoubleColumnStatistics(numberOfValues, bloomFilter, doubleStatistics);
        }

        if (stringStatistics != null) {
            return new StringColumnStatistics(numberOfValues, bloomFilter, stringStatistics);
        }

        if (dateStatistics != null) {
            return new DateColumnStatistics(numberOfValues, bloomFilter, dateStatistics);
        }

        if (decimalStatistics != null) {
            return new DecimalColumnStatistics(numberOfValues, bloomFilter, decimalStatistics);
        }

        if (binaryStatistics != null) {
            return new BinaryColumnStatistics(numberOfValues, bloomFilter, binaryStatistics);
        }

        return new ColumnStatistics(numberOfValues, bloomFilter);
    }
}
