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

package com.facebook.presto.hive.statistics;

import io.airlift.slice.Slice;

import java.lang.reflect.Type;
import java.time.chrono.ChronoLocalDate;
import java.util.Objects;
import java.util.OptionalLong;

import static java.util.Objects.hash;

/**
 * A mutable POJO for storing/merging column level stats
 */
public class ColumnQuickStats<T extends Comparable<T>>
{
    private final String columnName;
    private final Type statType;
    private long nullsCount;

    private T minValue;
    private T maxValue;

    public ColumnQuickStats(String columnName, Type statType)
    {
        this.columnName = columnName;
        this.statType = statType;
    }

    public Type getStatType()
    {
        return statType;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public T getMinValue()
    {
        return minValue;
    }

    public void setMinValue(T minValue)
    {
        this.minValue = this.minValue == null ? minValue : this.minValue.compareTo(minValue) < 0 ? this.minValue : minValue;
    }

    public T getMaxValue()
    {
        return maxValue;
    }

    public void setMaxValue(T maxValue)
    {
        this.maxValue = this.maxValue == null ? maxValue : this.maxValue.compareTo(maxValue) > 0 ? this.maxValue : maxValue;
    }

    public long getNullsCount()
    {
        return nullsCount;
    }

    public void addToNullsCount(long nullsCount)
    {
        this.nullsCount = this.nullsCount + nullsCount;
    }

    /**
     * @param nonNullCount an upper bound on the number of non-null values of this column in the partition
     */
    public OptionalLong getDistinctValuesCount(long nonNullCount)
    {
        // No non-null values means no distinct values, for every type.
        if (nonNullCount <= 0) {
            return OptionalLong.of(0);
        }

        if (statType.equals(Boolean.class)) {
            return OptionalLong.of(Math.min(nonNullCount, 2));
        }
        if (statType.equals(Slice.class)) {
            // No min/max is collected for VARCHAR/VARBINARY, so there is nothing to bound by.
            return OptionalLong.empty();
        }
        if (statType.equals(Float.class)) {
            // PartitionQuickStats#convertToPartitionStatistics does not emit NDV for FLOAT.
            return OptionalLong.empty();
        }
        if (minValue == null || maxValue == null) {
            return OptionalLong.empty();
        }

        // Bound NDV by the size of the observed [min, max] range, capped at the non-null row count.
        // This avoids an unknown NDV, which leaves the optimizer to fall back to default selectivity.
        OptionalLong rangeCardinality = estimateRangeCardinality();
        long bound = rangeCardinality.isPresent() ? Math.min(nonNullCount, rangeCardinality.getAsLong()) : nonNullCount;
        return OptionalLong.of(bound);
    }

    /**
     * {@code maxValue - minValue + 1} for integral and date types; empty for continuous types such as
     * DOUBLE, or when the range overflows a {@code long}.
     */
    private OptionalLong estimateRangeCardinality()
    {
        long min;
        long max;
        if (statType.equals(Integer.class)) {
            min = (Integer) minValue;
            max = (Integer) maxValue;
        }
        else if (statType.equals(Long.class)) {
            min = (Long) minValue;
            max = (Long) maxValue;
        }
        else if (statType.equals(ChronoLocalDate.class)) {
            min = ((ChronoLocalDate) minValue).toEpochDay();
            max = ((ChronoLocalDate) maxValue).toEpochDay();
        }
        else {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Math.addExact(Math.subtractExact(max, min), 1));
        }
        catch (ArithmeticException e) {
            return OptionalLong.empty();
        }
    }

    @Override
    public String toString()
    {
        return "ColumnQuickStats{" +
                "columnName='" + columnName + '\'' +
                ", statType=" + statType +
                ", nullsCount=" + nullsCount +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                '}';
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
        ColumnQuickStats<?> that = (ColumnQuickStats<?>) o;
        return nullsCount == that.nullsCount &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(statType, that.statType) &&
                Objects.equals(minValue, that.minValue) &&
                Objects.equals(maxValue, that.maxValue);
    }

    @Override
    public int hashCode()
    {
        return hash(columnName, statType, nullsCount, minValue, maxValue);
    }
}
