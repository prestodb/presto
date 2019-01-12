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
package io.prestosql.cost;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StatisticRange
{
    private static final double INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.25;
    private static final double INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.5;

    // TODO unify field and method names with SymbolStatsEstimate
    /**
     * {@code NaN} represents empty range ({@code high} must be {@code NaN} too)
     */
    private final double low;
    /**
     * {@code NaN} represents empty range ({@code low} must be {@code NaN} too)
     */
    private final double high;

    private final double distinctValues;

    public StatisticRange(double low, double high, double distinctValues)
    {
        checkArgument(
                low <= high || (isNaN(low) && isNaN(high)),
                "low value must be less than or equal to high value or both values have to be NaN, got %s and %s respectively",
                low,
                high);
        this.low = low;
        this.high = high;

        checkArgument(distinctValues >= 0 || isNaN(distinctValues), "Distinct values count should be non-negative, got: %s", distinctValues);
        this.distinctValues = distinctValues;
    }

    public static StatisticRange empty()
    {
        return new StatisticRange(NaN, NaN, 0);
    }

    public static StatisticRange from(SymbolStatsEstimate estimate)
    {
        return new StatisticRange(estimate.getLowValue(), estimate.getHighValue(), estimate.getDistinctValuesCount());
    }

    public double getLow()
    {
        return low;
    }

    public double getHigh()
    {
        return high;
    }

    public double getDistinctValuesCount()
    {
        return distinctValues;
    }

    public double length()
    {
        return high - low;
    }

    public boolean isEmpty()
    {
        return isNaN(low) && isNaN(high);
    }

    public double overlapPercentWith(StatisticRange other)
    {
        requireNonNull(other, "other is null");

        if (this.isEmpty() || other.isEmpty() || this.distinctValues == 0 || other.distinctValues == 0) {
            return 0.0; // zero is better than NaN as it will behave properly for calculating row count
        }

        if (this.equals(other)) {
            return 1.0;
        }

        double lengthOfIntersect = min(this.high, other.high) - max(this.low, other.low);
        if (isInfinite(lengthOfIntersect)) {
            if (isFinite(this.distinctValues) && isFinite(other.distinctValues)) {
                return min(other.distinctValues / this.distinctValues, 1);
            }
            return INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR;
        }
        if (lengthOfIntersect == 0) {
            return 1 / max(this.distinctValues, 1);
        }
        if (lengthOfIntersect < 0) {
            return 0;
        }
        if (isInfinite(length()) && isFinite(lengthOfIntersect)) {
            return INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR;
        }
        if (lengthOfIntersect > 0) {
            return lengthOfIntersect / length();
        }

        return NaN;
    }

    private double overlappingDistinctValues(StatisticRange other)
    {
        double overlapPercentOfLeft = overlapPercentWith(other);
        double overlapPercentOfRight = other.overlapPercentWith(this);
        double overlapDistinctValuesLeft = overlapPercentOfLeft * distinctValues;
        double overlapDistinctValuesRight = overlapPercentOfRight * other.distinctValues;
        double minInputDistinctValues = minExcludeNaN(this.distinctValues, other.distinctValues);

        return minExcludeNaN(minInputDistinctValues,
                maxExcludeNaN(overlapDistinctValuesLeft, overlapDistinctValuesRight));
    }

    public StatisticRange intersect(StatisticRange other)
    {
        double newLow = max(low, other.low);
        double newHigh = min(high, other.high);
        if (newLow <= newHigh) {
            return new StatisticRange(newLow, newHigh, overlappingDistinctValues(other));
        }
        return empty();
    }

    public StatisticRange addAndSumDistinctValues(StatisticRange other)
    {
        double newDistinctValues = distinctValues + other.distinctValues;
        return new StatisticRange(minExcludeNaN(low, other.low), maxExcludeNaN(high, other.high), newDistinctValues);
    }

    public StatisticRange addAndMaxDistinctValues(StatisticRange other)
    {
        double newDistinctValues = max(distinctValues, other.distinctValues);
        return new StatisticRange(minExcludeNaN(low, other.low), maxExcludeNaN(high, other.high), newDistinctValues);
    }

    public StatisticRange addAndCollapseDistinctValues(StatisticRange other)
    {
        double overlapPercentOfThis = this.overlapPercentWith(other);
        double overlapPercentOfOther = other.overlapPercentWith(this);
        double overlapDistinctValuesThis = overlapPercentOfThis * distinctValues;
        double overlapDistinctValuesOther = overlapPercentOfOther * other.distinctValues;
        double maxOverlappingValues = max(overlapDistinctValuesThis, overlapDistinctValuesOther);
        double newDistinctValues = maxOverlappingValues + (1 - overlapPercentOfThis) * distinctValues + (1 - overlapPercentOfOther) * other.distinctValues;

        return new StatisticRange(minExcludeNaN(low, other.low), maxExcludeNaN(high, other.high), newDistinctValues);
    }

    private static double minExcludeNaN(double v1, double v2)
    {
        if (isNaN(v1)) {
            return v2;
        }
        if (isNaN(v2)) {
            return v1;
        }
        return min(v1, v2);
    }

    private static double maxExcludeNaN(double v1, double v2)
    {
        if (isNaN(v1)) {
            return v2;
        }
        if (isNaN(v2)) {
            return v1;
        }
        return max(v1, v2);
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
        StatisticRange that = (StatisticRange) o;
        return Double.compare(that.low, low) == 0 &&
                Double.compare(that.high, high) == 0 &&
                Double.compare(that.distinctValues, distinctValues) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(low, high, distinctValues);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("range", format("[%s-%s]", low, high))
                .add("ndv", distinctValues)
                .toString();
    }
}
