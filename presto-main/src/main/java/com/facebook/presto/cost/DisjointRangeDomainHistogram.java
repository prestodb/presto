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

import com.facebook.presto.spi.statistics.ConnectorHistogram;
import com.facebook.presto.spi.statistics.Estimate;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Suppliers;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.openjdk.jol.info.ClassLayout;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.cost.HistogramCalculator.calculateFilterFactor;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a set of disjoint ranges that span an input domain.
 * Each range is used to represent filters over the domain of an original
 * "source" histogram.
 * <br>
 * For example, assume a source histogram represents a uniform distribution
 * over the range [0, 100]. Next, assume a query with multiple filters such as
 * <code>x < 10 OR x > 85</code>. This translates to two disjoint ranges over
 * the histogram of [0, 10) and (85, 100], representing roughly 35% of the
 * values in the original dataset. Using the example above, a cumulative
 * probability for value 5 represents 5% of the original dataset, but 20% (1/5)
 * of the range of constrained dataset. Similarly, all values in [10, 85] should
 * compute their cumulative probability as 40% (2/5).
 * <br>
 * The goal of this class is to implement the {@link ConnectorHistogram} API
 * given a source histogram whose domain has been constrained by a set of filter
 * ranges.
 * <br>
 * This class is intended to be immutable. Changing the set of ranges should
 * result in a new copy being created.
 */
public class DisjointRangeDomainHistogram
        implements ConnectorHistogram
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DisjointRangeDomainHistogram.class).instanceSize();
    private static final long RANGE_SIZE = ClassLayout.parseClass(Range.class).instanceSize();

    private final ConnectorHistogram source;
    // use RangeSet as the internal representation of the ranges, but the constructor arguments
    // use StatisticRange to support serialization and deserialization.
    private final Supplier<RangeSet<Double>> rangeSet;
    private final Set<Range<Double>> ranges;

    @JsonCreator
    public DisjointRangeDomainHistogram(@JsonProperty("source") ConnectorHistogram source, @JsonProperty("ranges") Collection<StatisticRange> ranges)
    {
        this(source, ranges.stream().map(StatisticRange::toRange).collect(toImmutableSet()));
    }

    public DisjointRangeDomainHistogram(ConnectorHistogram source, Set<Range<Double>> ranges)
    {
        this.source = requireNonNull(source, "source is null");
        this.ranges = requireNonNull(ranges, "ranges is null");
        this.rangeSet = Suppliers.memoize(() -> {
            RangeSet<Double> rangeSet = TreeRangeSet.create();
            rangeSet.addAll(ranges);
            return rangeSet.subRangeSet(getSourceSpan(this.source));
        });
    }

    private static Range<Double> getSourceSpan(ConnectorHistogram source)
    {
        return Range.closed(
                source.inverseCumulativeProbability(0.0).orElse(() -> NEGATIVE_INFINITY),
                source.inverseCumulativeProbability(1.0).orElse(() -> POSITIVE_INFINITY));
    }

    @JsonProperty
    public ConnectorHistogram getSource()
    {
        return source;
    }

    @JsonProperty
    public Set<StatisticRange> getRanges()
    {
        return rangeSet.get().asRanges().stream().map(StatisticRange::fromRange).collect(toImmutableSet());
    }

    public DisjointRangeDomainHistogram(ConnectorHistogram source)
    {
        this(source, ImmutableSet.<Range<Double>>of());
    }

    @Override
    public Estimate cumulativeProbability(double value, boolean inclusive)
    {
        // 1. compute the total probability for every existing range on the source
        // 2. find the range, r, where `value` falls
        // 3. compute the cumulative probability across all ranges that intersect [min, value]
        // 4. divide the result from (3) by the result from (1) to get the true cumulative
        //    probability of the disjoint domains over the original histogram
        if (Double.isNaN(value)) {
            return Estimate.unknown();
        }
        Optional<Range<Double>> optionalSpan = getSpan();
        if (!optionalSpan.isPresent()) {
            return Estimate.of(0.0);
        }
        Range<Double> span = optionalSpan.get();
        if (value <= span.lowerEndpoint()) {
            return Estimate.of(0.0);
        }
        Range<Double> input = Range.range(span.lowerEndpoint(), span.lowerBoundType(), value, inclusive ? BoundType.CLOSED : BoundType.OPEN);
        Estimate fullSetOverlap = calculateRangeSetOverlap(rangeSet.get());
        RangeSet<Double> spanned = rangeSet.get().subRangeSet(input);
        Estimate spannedOverlap = calculateRangeSetOverlap(spanned);

        return spannedOverlap.flatMap(spannedProbability ->
                fullSetOverlap.map(fullSetProbability -> {
                    if (fullSetProbability == 0.0) {
                        return 0.0;
                    }
                    return min(spannedProbability / fullSetProbability, 1.0);
                }));
    }

    private Estimate calculateRangeSetOverlap(RangeSet<Double> ranges)
    {
        // we require knowing bounds on all ranges
        double cumulativeTotal = 0.0;
        for (Range<Double> range : ranges.asRanges()) {
            Estimate rangeProbability = getRangeProbability(range);
            if (rangeProbability.isUnknown()) {
                return Estimate.unknown();
            }
            cumulativeTotal += rangeProbability.getValue();
        }
        return Estimate.of(cumulativeTotal);
    }

    /**
     * Calculates the percent of the source distribution that {@code range}
     * spans.
     *
     * @param range the range over the source domain
     * @return estimate of the total probability the range covers in the source
     */
    private Estimate getRangeProbability(Range<Double> range)
    {
        return calculateFilterFactor(StatisticRange.fromRange(range), source, Estimate.unknown(), false);
    }

    @Override
    public Estimate inverseCumulativeProbability(double percentile)
    {
        checkArgument(percentile >= 0.0 && percentile <= 1.0, "percentile must fall in [0.0, 1.0]");
        // 1. compute the probability for each range on the source in order until reaching a range
        // where the cumulative total exceeds the percentile argument (totalCumulative)
        // 2. compute the source probability of the left endpoint of the given range (percentileLow)
        // 3. compute the "true" source percentile:
        //    rangedPercentile = percentile - percentileLow
        //
        // percentileLow + (rangedPercentile * rangePercentileLength)
        Optional<Range<Double>> optionalSpan = getSpan();
        if (!optionalSpan.isPresent()) {
            return Estimate.unknown();
        }
        Range<Double> span = optionalSpan.get();
        if (percentile == 0.0 && isFinite(span.lowerEndpoint())) {
            return source.inverseCumulativeProbability(0.0).map(sourceMin -> max(span.lowerEndpoint(), sourceMin));
        }

        if (percentile == 1.0 && isFinite(span.upperEndpoint())) {
            return source.inverseCumulativeProbability(1.0).map(sourceMax -> min(span.upperEndpoint(), sourceMax));
        }

        Estimate totalCumulativeEstimate = calculateRangeSetOverlap(rangeSet.get());
        if (totalCumulativeEstimate.isUnknown()) {
            return Estimate.unknown();
        }
        double totalCumulativeProbabilitySourceDomain = totalCumulativeEstimate.getValue();
        if (totalCumulativeProbabilitySourceDomain == 0.0) {
            // calculations will fail with NaN
            return Estimate.unknown();
        }
        double cumulativeProbabilityNewDomain = 0.0;
        double lastRangeEstimateSourceDomain = 0.0;
        Range<Double> currentRange = null;
        // find the range where the percentile falls
        for (Range<Double> range : rangeSet.get().asRanges()) {
            Estimate rangeEstimate = getRangeProbability(range);
            if (rangeEstimate.isUnknown()) {
                return Estimate.unknown();
            }
            currentRange = range;
            lastRangeEstimateSourceDomain = rangeEstimate.getValue();
            cumulativeProbabilityNewDomain += lastRangeEstimateSourceDomain / totalCumulativeProbabilitySourceDomain;
            if (cumulativeProbabilityNewDomain >= percentile) {
                break;
            }
        }
        if (currentRange == null) {
            // no ranges to iterate over. Did a constraint cut the entire domain of values?
            return Estimate.unknown();
        }
        Estimate rangeLeftSourceEstimate = source.cumulativeProbability(currentRange.lowerEndpoint(), currentRange.lowerBoundType() == BoundType.OPEN);
        if (rangeLeftSourceEstimate.isUnknown()) {
            return Estimate.unknown();
        }
        double rangeLeftSource = rangeLeftSourceEstimate.getValue();
        double lastRangeProportionalProbability = lastRangeEstimateSourceDomain / totalCumulativeProbabilitySourceDomain;
        double percentileLeftFromNewDomain = percentile - cumulativeProbabilityNewDomain + lastRangeProportionalProbability;
        double percentilePoint = lastRangeEstimateSourceDomain * percentileLeftFromNewDomain / lastRangeProportionalProbability;
        double finalPercentile = rangeLeftSource + percentilePoint;

        return source.inverseCumulativeProbability(min(max(finalPercentile, 0.0), 1.0));
    }

    /**
     * Adds a new domain (logical disjunction) to the existing set.
     *
     * @param other the new range to add to the set.
     * @return a new {@link DisjointRangeDomainHistogram}
     */
    public DisjointRangeDomainHistogram addDisjunction(StatisticRange other)
    {
        Set<Range<Double>> ranges = ImmutableSet.<Range<Double>>builder()
                .addAll(this.ranges)
                .add(other.toRange())
                .build();
        return new DisjointRangeDomainHistogram(source, ranges);
    }

    /**
     * Adds a constraint (logical conjunction). This will constrain all ranges
     * in the set to ones that are contained by the argument range.
     *
     * @param other the range that should enclose the set.
     * @return a new {@link DisjointRangeDomainHistogram} where
     */
    public DisjointRangeDomainHistogram addConjunction(StatisticRange other)
    {
        return new DisjointRangeDomainHistogram(source, rangeSet.get().subRangeSet(other.toRange()).asRanges());
    }

    /**
     * Adds a new range to the available ranges that this histogram computes over
     * <br>
     * e.g. if the source histogram represents values [0, 100], and an existing
     * range in the set constrains it to [0, 25], and this method is called with
     * a range of [50, 75], then it will attempt to push [50, 75] down onto the
     * existing histogram to expand the set of intervals that are used to
     * computed probabilities to [[0, 25], [50, 75]].
     * <br>
     * This method should be called for cases where we want to calculate plan
     * statistics for queries that have multiple filters combined with OR.
     *
     * @param histogram the source histogram to add the range conjunction
     * @param range the range representing the conjunction to add
     * @return a new histogram with the conjunction applied.
     */
    public static ConnectorHistogram addDisjunction(ConnectorHistogram histogram, StatisticRange range)
    {
        if (histogram instanceof DisjointRangeDomainHistogram) {
            return ((DisjointRangeDomainHistogram) histogram).addDisjunction(range);
        }

        return new DisjointRangeDomainHistogram(histogram, ImmutableSet.of(range.toRange()));
    }

    /**
     * Similar to {@link #addDisjunction(ConnectorHistogram, StatisticRange)} this method constrains
     * the entire domain such that <em>all ranges</em> in the set intersect with the given range
     * argument to this method.
     * <br>
     * This should be used when an AND clause is present in the query and all tuples MUST satisfy
     * the condition.
     *
     * @param histogram the source histogram
     * @param range the range of values that the entire histogram's domain must fall within
     * @return a histogram with the new range constraint
     */
    public static ConnectorHistogram addConjunction(ConnectorHistogram histogram, StatisticRange range)
    {
        if (histogram instanceof DisjointRangeDomainHistogram) {
            return ((DisjointRangeDomainHistogram) histogram).addConjunction(range);
        }

        return new DisjointRangeDomainHistogram(histogram, ImmutableSet.of(range.toRange()));
    }

    /**
     * @return the span if it exists, empty otherwise
     */
    private Optional<Range<Double>> getSpan()
    {
        try {
            return Optional.of(rangeSet.get().span());
        }
        catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", this.source)
                .add("rangeSet", this.rangeSet)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DisjointRangeDomainHistogram)) {
            return false;
        }
        DisjointRangeDomainHistogram other = (DisjointRangeDomainHistogram) o;
        return Objects.equals(source, other.source) &&
                // getRanges() flattens and creates the minimal range set which
                // determines whether two histograms are truly equal
                Objects.equals(getRanges(), other.getRanges());
    }

    @Override
    public int hashCode()
    {
        return hash(source, getRanges());
    }

    @Override
    public long getEstimatedSize()
    {
        // don't count the source histogram as it's just a reference to
        // another histogram. We don't want to count the retained memory.
        return INSTANCE_SIZE +
                (RANGE_SIZE * ranges.size());
    }
}
