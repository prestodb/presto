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
package com.facebook.presto.cost;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class StatisticRange
{
    private static final double INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.25;
    private static final double INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR = 0.5;

    private final double low;
    private final double high;
    private final double distinctValues;

    public StatisticRange(double low, double high, double distinctValues)
    {
        checkState(low <= high || (isNaN(low) && isNaN(high)), "Low must be smaller or equal to high or range must be empty (NaN, NaN)");
        checkState(distinctValues >= 0 || isNaN(distinctValues), "Distinct values count cannot be negative");
        this.low = low;
        this.high = high;
        this.distinctValues = distinctValues;
    }

    public static StatisticRange empty()
    {
        return new StatisticRange(NaN, NaN, 0);
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
        if (this.equals(other)) {
            return 1.0;
        }

        if (isEmpty() || other.isEmpty()) {
            return 0.0; // zero is better than NaN as it will behave properly for calculating row count
        }

        double lengthOfIntersect = min(high, other.high) - max(low, other.low);
        if (isInfinite(lengthOfIntersect)) {
            return INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR;
        }
        if (lengthOfIntersect == 0) {
            return 1 / distinctValues;
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

        return maxExcludeNaN(overlapDistinctValuesLeft, overlapDistinctValuesRight);
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

    public StatisticRange add(StatisticRange other)
    {
        double newDistinctValues = distinctValues + other.distinctValues;
        return new StatisticRange(minExcludeNaN(low, other.low), maxExcludeNaN(high, other.high), newDistinctValues);
    }

    public StatisticRange subtract(StatisticRange rightRange)
    {
        StatisticRange intersect = intersect(rightRange);
        double newLow = getLow();
        double newHigh = getHigh();
        if (intersect.getLow() == getLow()) {
            newLow = intersect.getHigh();
        }
        if (intersect.getHigh() == getHigh()) {
            newHigh = intersect.getLow();
        }
        if (newLow > newHigh) {
            newLow = NaN;
            newHigh = NaN;
        }

        return new StatisticRange(newLow, newHigh, max(getDistinctValuesCount(), rightRange.getDistinctValuesCount()) - intersect.getDistinctValuesCount());
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
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StatisticRange)) {
            return false;
        }
        StatisticRange other = (StatisticRange) obj;
        return low == other.low &&
                high == other.high &&
                distinctValues == other.distinctValues;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(low, high, distinctValues);
    }
}
