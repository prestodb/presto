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

    /**
     * Returns a conservative (upper-bound) estimate of the number of distinct values (NDV) for
     * this column, derived from the already-merged rowCount/nullsCount/min/max state.
     * <p>
     * Parquet footers do not carry an exact or approximate distinct count, so rather than emitting
     * nothing (which surfaces as {@code NaN} to the cost-based optimizer and forces a cross-join x
     * default-selectivity fallback for equi-joins on this column), we bound NDV by:
     * <ul>
     *     <li>the number of non-null values seen ({@code rowCount - nullsCount}), which is always a
     *     valid upper bound, and</li>
     *     <li>for types with a countable ordered range (integral numeric and date types), the size
     *     of the {@code [minValue, maxValue]} range, i.e. {@code maxValue - minValue + 1}.</li>
     * </ul>
     * This is intentionally called after all row-group/file merges for this column have already
     * been folded into rowCount/nullsCount/minValue/maxValue (via {@link #addToRowCount},
     * {@link #addToNullsCount}, {@link #setMinValue}, {@link #setMaxValue}), so the bound below is
     * computed once against the fully-merged stats rather than accumulated incrementally -- NDV
     * does not add across row groups/files/partitions, so summing partial per-row-group NDVs would
     * over-count. Types with no sensible bound (e.g. VARCHAR/VARBINARY, for which no min/max is
     * collected) return {@link OptionalLong#empty()}, preserving prior behavior.
     * <p>
     * An over-estimate of NDV is the direction we deliberately prefer here: for equi-joins it makes
     * selectivity estimates smaller/more selective, which is optimistic and can under-estimate join
     * output, but is far safer than the catastrophic cross-join fallback triggered by an unknown/NaN
     * NDV. Note this is not universally "safe": the same over-estimate makes GROUP BY cardinality
     * estimates larger, not smaller.
     * <p>
     * TODO: exact/approximate NDV via HLL (behind a flag), for cases where this bound is loose.
     */
    public OptionalLong getDistinctValuesCount()
    {
        // A column with zero observed non-null values trivially has zero distinct values,
        // regardless of type -- this is an exact fact, not an estimate, so report it even for
        // types (e.g. VARCHAR/VARBINARY) that otherwise have no sensible NDV bound.
        long nonNullCount = rowCount - nullsCount;
        if (nonNullCount <= 0) {
            return OptionalLong.of(0);
        }

        if (statType.equals(Boolean.class)) {
            // Boolean quick-stats do not currently track true/false counts (see the TODO in
            // ParquetQuickStatsBuilder), so bound NDV purely by the number of non-null values.
            return OptionalLong.of(Math.min(nonNullCount, 2));
        }
        if (statType.equals(Slice.class)) {
            // VARCHAR/VARBINARY/BINARY: no min/max is collected for these today, so there is no
            // sensible bound available. Leave unset, unchanged from prior behavior.
            return OptionalLong.empty();
        }
        if (statType.equals(Float.class)) {
            // PartitionQuickStats#convertToPartitionStatistics has no FLOAT branch today (FLOAT
            // columns fall into the catch-all branch, which does not emit distinctValuesCount, or
            // any other stat besides nullsCount) -- avoid computing a bound that can never be
            // consumed. Revisit together if/when a FLOAT branch is added there.
            return OptionalLong.empty();
        }
        if (minValue == null || maxValue == null) {
            // No range was ever observed for this column despite having non-null rows (should not
            // happen for the types handled by estimateRangeCardinality(), but guard defensively).
            return OptionalLong.empty();
        }

        OptionalLong rangeCardinality = estimateRangeCardinality();
        long bound = rangeCardinality.isPresent() ? Math.min(nonNullCount, rangeCardinality.getAsLong()) : nonNullCount;
        return OptionalLong.of(Math.max(0, bound));
    }

    /**
     * Computes {@code maxValue - minValue + 1} for types with a countable, ordered range
     * (integral numeric and date types). Returns empty for types where the range is not countable
     * (e.g. DOUBLE, which is continuous -- FLOAT is short-circuited before reaching here) or where
     * the range would overflow a {@code long}.
     */
    private OptionalLong estimateRangeCardinality()
    {
        try {
            if (statType.equals(Integer.class)) {
                long min = (Integer) minValue;
                long max = (Integer) maxValue;
                return OptionalLong.of(Math.addExact(Math.subtractExact(max, min), 1));
            }
            if (statType.equals(Long.class)) {
                long min = (Long) minValue;
                long max = (Long) maxValue;
                return OptionalLong.of(Math.addExact(Math.subtractExact(max, min), 1));
            }
            if (statType.equals(ChronoLocalDate.class)) {
                long min = ((ChronoLocalDate) minValue).toEpochDay();
                long max = ((ChronoLocalDate) maxValue).toEpochDay();
                return OptionalLong.of(Math.addExact(Math.subtractExact(max, min), 1));
            }
        }
        catch (ArithmeticException e) {
            // Range too large to represent as a long (e.g. min close to Long.MIN_VALUE and max
            // close to Long.MAX_VALUE); fall back to bounding by non-null-count only.
            return OptionalLong.empty();
        }
        // DOUBLE: the value range is continuous, so [min, max] does not bound cardinality; the
        // non-null-count bound applied by the caller is the only sound conservative bound.
        return OptionalLong.empty();
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
