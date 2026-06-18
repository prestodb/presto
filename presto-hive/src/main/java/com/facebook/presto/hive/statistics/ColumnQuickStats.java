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

import java.lang.reflect.Type;
import java.util.Objects;

import static java.util.Objects.hash;

/**
 * A mutable POJO for storing/merging column level stats
 */
public class ColumnQuickStats<T extends Comparable<T>>
{
    private final String columnName;
    private final Type statType;
    private long rowCount;
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

    public long getRowCount()
    {
        return rowCount;
    }

    public void addToRowCount(long rowCount)
    {
        this.rowCount = this.rowCount + rowCount;
    }

    public long getNullsCount()
    {
        return nullsCount;
    }

    public void addToNullsCount(long nullsCount)
    {
        this.nullsCount = this.nullsCount + nullsCount;
    }

    @Override
    public String toString()
    {
        return "ColumnQuickStats{" +
                "columnName='" + columnName + '\'' +
                ", statType=" + statType +
                ", rowCount=" + rowCount +
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
        return rowCount == that.rowCount && nullsCount == that.nullsCount &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(statType, that.statType) &&
                Objects.equals(minValue, that.minValue) &&
                Objects.equals(maxValue, that.maxValue);
    }

    @Override
    public int hashCode()
    {
        return hash(columnName, statType, rowCount, nullsCount, minValue, maxValue);
    }
}
