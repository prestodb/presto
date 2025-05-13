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
import com.facebook.presto.orc.proto.DwrfProto;
import com.google.common.base.MoreObjects.ToStringHelper;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.orc.metadata.statistics.BinaryStatisticsBuilder.mergeBinaryStatistics;
import static com.facebook.presto.orc.metadata.statistics.BooleanStatisticsBuilder.mergeBooleanStatistics;
import static com.facebook.presto.orc.metadata.statistics.DateStatisticsBuilder.mergeDateStatistics;
import static com.facebook.presto.orc.metadata.statistics.DoubleStatisticsBuilder.mergeDoubleStatistics;
import static com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder.mergeIntegerStatistics;
import static com.facebook.presto.orc.metadata.statistics.LongDecimalStatisticsBuilder.mergeDecimalStatistics;
import static com.facebook.presto.orc.metadata.statistics.MapColumnStatisticsBuilder.mergeMapStatistics;
import static com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder.mergeStringStatistics;
import static com.google.common.base.MoreObjects.toStringHelper;

public class ColumnStatistics
        implements Hashable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ColumnStatistics.class).instanceSize();

    private final boolean hasNumberOfValues;
    private final long numberOfValues;
    private final boolean hasRawSize;
    private final long rawSize;
    private final boolean hasStorageSize;
    private final long storageSize;
    private final HiveBloomFilter bloomFilter;

    public ColumnStatistics(
            Long numberOfValues,
            HiveBloomFilter bloomFilter,
            Long rawSize,
            Long storageSize)
    {
        this.hasNumberOfValues = numberOfValues != null;
        this.numberOfValues = hasNumberOfValues ? numberOfValues : 0;

        this.hasRawSize = rawSize != null;
        this.rawSize = hasRawSize ? rawSize : 0;

        this.hasStorageSize = storageSize != null;
        this.storageSize = hasStorageSize ? storageSize : 0;

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

    public boolean hasRawSize()
    {
        return hasRawSize;
    }

    public long getRawSize()
    {
        return hasRawSize() ? rawSize : 0;
    }

    public boolean hasStorageSize()
    {
        return hasStorageSize;
    }

    public long getStorageSize()
    {
        return hasStorageSize() ? storageSize : 0;
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
    public long getTotalValueSizeInBytes()
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

    public MapStatistics getMapStatistics()
    {
        return null;
    }

    public HiveBloomFilter getBloomFilter()
    {
        return bloomFilter;
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
                hasRawSize == that.hasRawSize &&
                rawSize == that.rawSize &&
                hasStorageSize == that.hasStorageSize &&
                storageSize == that.storageSize &&
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
                hasRawSize(),
                getRawSize(),
                hasStorageSize(),
                getStorageSize(),
                getBloomFilter());
    }

    protected ToStringHelper getToStringHelper()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("numberOfValues", getNumberOfValues())
                .add("rawSize", getRawSize())
                .add("storageSize", getStorageSize())
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
        // This hashing function is used for ORC writer file validation.
        // Fields rawSize and storageSize are not included because they cannot
        // be calculated by the reader during the file validation.
        hasher.putOptionalLong(hasNumberOfValues, numberOfValues)
                .putOptionalHashable(getBloomFilter());
    }

    public static ColumnStatistics mergeColumnStatistics(List<ColumnStatistics> stats)
    {
        return mergeColumnStatistics(stats, null, null);
    }

    public static ColumnStatistics mergeColumnStatistics(List<ColumnStatistics> stats, Long extraStorageSize, Object2LongMap<DwrfProto.KeyInfo> mapKeySizes)
    {
        if (stats.isEmpty()) {
            return new ColumnStatistics(0L, null, null, null);
        }

        long numberOfRows = 0;
        long rawSize = 0;
        long storageSize = 0;
        boolean hasRawSize = false;
        boolean hasStorageSize = false;

        if (extraStorageSize != null) {
            hasStorageSize = true;
            storageSize = extraStorageSize;
        }

        for (ColumnStatistics stat : stats) {
            numberOfRows += stat.getNumberOfValues();
            if (stat.hasRawSize()) {
                rawSize += stat.getRawSize();
                hasRawSize = true;
            }
            if (stat.hasStorageSize()) {
                storageSize += stat.getStorageSize();
                hasStorageSize = true;
            }
        }

        return createColumnStatistics(
                numberOfRows,
                hasRawSize ? rawSize : null,
                hasStorageSize ? storageSize : null,
                mergeBooleanStatistics(stats).orElse(null),
                mergeIntegerStatistics(stats).orElse(null),
                mergeDoubleStatistics(stats).orElse(null),
                mergeStringStatistics(stats).orElse(null),
                mergeDateStatistics(stats).orElse(null),
                mergeDecimalStatistics(stats).orElse(null),
                mergeBinaryStatistics(stats).orElse(null),
                mergeMapStatistics(stats, mapKeySizes).orElse(null),
                null);
    }

    public static ColumnStatistics createColumnStatistics(
            Long numberOfValues,
            Long rawSize,
            Long storageSize,
            BooleanStatistics booleanStatistics,
            IntegerStatistics integerStatistics,
            DoubleStatistics doubleStatistics,
            StringStatistics stringStatistics,
            DateStatistics dateStatistics,
            DecimalStatistics decimalStatistics,
            BinaryStatistics binaryStatistics,
            MapStatistics mapStatistics,
            HiveBloomFilter bloomFilter)
    {
        if (booleanStatistics != null) {
            return new BooleanColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize, booleanStatistics);
        }

        if (integerStatistics != null) {
            return new IntegerColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize, integerStatistics);
        }

        if (doubleStatistics != null) {
            return new DoubleColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize, doubleStatistics);
        }

        if (stringStatistics != null) {
            return new StringColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize, stringStatistics);
        }

        if (dateStatistics != null) {
            return new DateColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize, dateStatistics);
        }

        if (decimalStatistics != null) {
            return new DecimalColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize, decimalStatistics);
        }

        if (binaryStatistics != null) {
            return new BinaryColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize, binaryStatistics);
        }

        if (mapStatistics != null) {
            return new MapColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize, mapStatistics);
        }

        return new ColumnStatistics(numberOfValues, bloomFilter, rawSize, storageSize);
    }
}
