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

package com.facebook.presto.spi.statistics;

import com.facebook.presto.common.predicate.Range;

import static com.facebook.presto.spi.statistics.ColumnStatistics.INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR;
import static com.facebook.presto.spi.statistics.ColumnStatistics.INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.min;

public class HistogramCalculator
{
    private HistogramCalculator()
    {}

    /**
     * Calculates the "filter factor" corresponding to the overlap between the statistic range
     * and the histogram distribution.
     * <br>
     * The filter factor is a fractional value in [0.0, 1.0] that represents the proportion of
     * tuples in the source column that would be included in the result of a filter where the valid
     * values in the filter are represented by the {@code range} parameter of this function.
     *
     * @param range the intersecting range with the histogram
     * @param histogram the source histogram
     * @param totalDistinctValues the total number of distinct values in the domain of the histogram
     * @param useHeuristics whether to return heuristic values based on constants and/or distinct
     * value counts. If false, {@link Estimate#unknown()} will be returned in any case a
     * heuristic would have been used
     * @return an estimate, x, where 0.0 <= x <= 1.0.
     */
    public static Estimate calculateFilterFactor(Range range, double rangeDistinctValues, ConnectorHistogram histogram, Estimate totalDistinctValues, boolean useHeuristics)
    {
        boolean openHigh = !range.isHighInclusive();
        boolean openLow = !range.isLowInclusive();
        Estimate min = histogram.inverseCumulativeProbability(0.0);
        Estimate max = histogram.inverseCumulativeProbability(1.0);
        double rangeLow = range.getLowValue().map(Double.class::cast).orElse(NEGATIVE_INFINITY);
        double rangeHigh = range.getHighValue().map(Double.class::cast).orElse(POSITIVE_INFINITY);
        double rangeLength = rangeHigh - rangeLow;

        // range is either above or below histogram
        if ((!max.isUnknown() && (openHigh ? max.getValue() <= rangeLow : max.getValue() < rangeLow))
                || (!min.isUnknown() && (openLow ? min.getValue() >= rangeHigh : min.getValue() > rangeHigh))) {
            return Estimate.of(0.0);
        }

        // one of the max/min bounds can't be determined
        if ((max.isUnknown() && !min.isUnknown()) || (!max.isUnknown() && min.isUnknown())) {
            // when the range length is 0, the filter factor should be 1/distinct value count
            if (!useHeuristics) {
                return Estimate.unknown();
            }

            if (rangeLength == 0.0) {
                return totalDistinctValues.map(distinct -> 1.0 / distinct);
            }

            if (isFinite(rangeLength)) {
                return Estimate.of(INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR);
            }
            return Estimate.of(INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR);
        }

        // we know the bounds are both known, so calculate the percentile for each bound
        // The inclusivity arguments can be derived from the open-ness of the interval we're
        // calculating the filter factor for
        // e.g. given a variable with values in [0, 10] to calculate the filter of
        // [1, 9) (openness: false, true) we need the percentile from
        // [0.0 to 1.0) (openness: false, true) and from [0.0, 9.0) (openness: false, true)
        // thus for the "lowPercentile" calculation we should pass "false" to be non-inclusive
        // (same as openness) however, on the high-end we want the inclusivity to be the opposite
        // of the openness since if it's open, we _don't_ want to include the bound.
        Estimate lowPercentile = histogram.cumulativeProbability(rangeLow, openLow);
        Estimate highPercentile = histogram.cumulativeProbability(rangeHigh, !openHigh);

        // both bounds are probably infinity, use the infinite-infinite heuristic
        if (lowPercentile.isUnknown() || highPercentile.isUnknown()) {
            if (!useHeuristics) {
                return Estimate.unknown();
            }
            // in the case the histogram has no values
            if (totalDistinctValues.equals(Estimate.zero()) || rangeDistinctValues == 0.0) {
                return Estimate.of(0.0);
            }

            // in the case only one is unknown
            if (((lowPercentile.isUnknown() && !highPercentile.isUnknown()) ||
                    (!lowPercentile.isUnknown() && highPercentile.isUnknown())) &&
                    isFinite(rangeLength)) {
                return Estimate.of(INFINITE_TO_FINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR);
            }

            if (rangeLength == 0.0) {
                return totalDistinctValues.map(distinct -> 1.0 / distinct);
            }

            if (!isNaN(rangeDistinctValues)) {
                return totalDistinctValues.map(distinct -> min(1.0, rangeDistinctValues / distinct));
            }

            return Estimate.of(INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR);
        }

        // in the case the range is a single value, this can occur if the input
        // filter range is a single value (low == high) OR in the case that the
        // bounds of the filter or this histogram are infinite.
        // in the case of infinite bounds, we should return an estimate that
        // correlates to the overlapping distinct values.
        if (lowPercentile.equals(highPercentile)) {
            if (!useHeuristics) {
                return Estimate.zero();
            }

            return totalDistinctValues.map(distinct -> 1.0 / distinct);
        }

        // in the case that we return the entire range, the returned factor percent should be
        // proportional to the number of distinct values in the range
        if (lowPercentile.equals(Estimate.zero()) && highPercentile.equals(Estimate.of(1.0)) && min.isUnknown() && max.isUnknown()) {
            if (!useHeuristics) {
                return Estimate.unknown();
            }

            return totalDistinctValues.flatMap(totalDistinct -> {
                if (fuzzyEquals(totalDistinct, 0.0, 1E-6)) {
                    return Estimate.of(1.0);
                }
                return Estimate.of(min(1.0, rangeDistinctValues / totalDistinct));
            })
                    // in the case totalDistinct is NaN or 0
                    .or(() -> Estimate.of(INFINITE_TO_INFINITE_RANGE_INTERSECT_OVERLAP_HEURISTIC_FACTOR));
        }

        return lowPercentile.flatMap(lowPercent -> highPercentile.map(highPercent -> highPercent - lowPercent));
    }

    private static boolean fuzzyEquals(double a, double b, double tolerance)
    {
        return Math.copySign(a - b, 1.0) <= tolerance
                // copySign(x, 1.0) is a branch-free version of abs(x), but with different NaN semantics
                || (a == b) // needed to ensure that infinities equal themselves
                || (Double.isNaN(a) && Double.isNaN(b));
    }
}
