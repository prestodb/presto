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

import com.google.common.collect.ImmutableSet;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.predicate.Range.greaterThanOrEqual;
import static com.facebook.presto.common.predicate.Range.lessThanOrEqual;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertEquals;

public class TestDisjointRangeDomainHistogram
        extends TestHistogram
{
    /**
     * A uniform base with 2 ranges that are fully within the range of the uniform histogram.
     */
    @Test
    public void testBasicDisjointRanges()
    {
        ConnectorHistogram source = new UniformDistributionHistogram(0, 100);
        ConnectorHistogram constrained = DisjointRangeDomainHistogram
                .addDisjunction(source, rangeOpen(0d, 25d));
        constrained = DisjointRangeDomainHistogram
                .addDisjunction(constrained, rangeOpen(75d, 100d));
        assertEquals(constrained.inverseCumulativeProbability(0.75).getValue(), 87.5);
        assertEquals(constrained.inverseCumulativeProbability(0.0).getValue(), 0.0);
        assertEquals(constrained.inverseCumulativeProbability(1.0).getValue(), 100);
        assertEquals(constrained.inverseCumulativeProbability(0.5).getValue(), 25);
    }

    /**
     * A uniform base with a range that (1) doesn't have any overlap with the base distribution (2)
     * has partial overlap (both ends of the base) and (3) complete overlap.
     */
    @Test
    public void testSingleDisjointRange()
    {
        ConnectorHistogram source = new UniformDistributionHistogram(0, 10);

        // no overlap, left bound
        ConnectorHistogram constrained = DisjointRangeDomainHistogram
                .addDisjunction(source, rangeOpen(-10d, -5d));
        for (int i = -11; i < 12; i++) {
            assertEquals(constrained.cumulativeProbability(i, true).getValue(), 0.0, 1E-8);
            assertEquals(constrained.cumulativeProbability(i, false).getValue(), 0.0, 1E-8);
        }
        assertEquals(constrained.inverseCumulativeProbability(0.0), Estimate.unknown());
        assertEquals(constrained.inverseCumulativeProbability(1.0), Estimate.unknown());

        // partial overlap left bound
        constrained = new DisjointRangeDomainHistogram(source, ImmutableSet.of(rangeOpen(-2d, 2d)));
        assertEquals(constrained.cumulativeProbability(-3, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(-1, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(0, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(1, false).getValue(), 0.5, 1E-8);
        assertEquals(constrained.cumulativeProbability(1.5, false).getValue(), 0.75, 1E-8);
        assertEquals(constrained.cumulativeProbability(2, false).getValue(), 1.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(4, false).getValue(), 1.0, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.0).getValue(), 0d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.5).getValue(), 1d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.75).getValue(), 1.5d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(1.0).getValue(), 2d, 1E-8);

        //full overlap
        constrained = new DisjointRangeDomainHistogram(source, ImmutableSet.of(rangeOpen(3d, 4d)));
        assertEquals(constrained.cumulativeProbability(-3, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(0, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(1, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(3, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(3.5, false).getValue(), 0.5, 1E-8);
        assertEquals(constrained.cumulativeProbability(4, false).getValue(), 1.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(4.5, false).getValue(), 1.0, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.0).getValue(), 3d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.5).getValue(), 3.5d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.75).getValue(), 3.75d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(1.0).getValue(), 4d, 1E-8);

        //right side overlap
        constrained = new DisjointRangeDomainHistogram(source, ImmutableSet.of(rangeOpen(8d, 12d)));
        assertEquals(constrained.cumulativeProbability(-3, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(0, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(5, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(8, false).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(9, false).getValue(), 0.5, 1E-8);
        assertEquals(constrained.cumulativeProbability(9.5, false).getValue(), 0.75, 1E-8);
        assertEquals(constrained.cumulativeProbability(10, false).getValue(), 1.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(11, false).getValue(), 1.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(12, false).getValue(), 1.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(13, false).getValue(), 1.0, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.0).getValue(), 8d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.5).getValue(), 9d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.75).getValue(), 9.5d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(1.0).getValue(), 10d, 1E-8);

        // no overlap, right bound
        constrained = DisjointRangeDomainHistogram
                .addDisjunction(source, rangeOpen(15d, 20d));
        for (int i = 15; i < 20; i++) {
            assertEquals(constrained.cumulativeProbability(i, true).getValue(), 0.0, 1E-8);
            assertEquals(constrained.cumulativeProbability(i, false).getValue(), 0.0, 1E-8);
        }
        assertEquals(constrained.inverseCumulativeProbability(0.0), Estimate.unknown());
        assertEquals(constrained.inverseCumulativeProbability(1.0), Estimate.unknown());
    }

    /**
     * Tests that calculations across N > 1 disjunctions applied to the source histogram are
     * calculated properly.
     */
    @Test
    public void testMultipleDisjunction()
    {
        StandardNormalHistogram source = new StandardNormalHistogram();
        RealDistribution dist = source.getDistribution();
        ConnectorHistogram constrained = disjunction(source, rangeClosed(-2d, -1d));
        constrained = disjunction(constrained, rangeClosed(1d, 2d));
        double rangeLeftProb = dist.cumulativeProbability(-1) - dist.cumulativeProbability(-2);
        double rangeRightProb = dist.cumulativeProbability(2) - dist.cumulativeProbability(1);
        double sumRangeProb = rangeLeftProb + rangeRightProb;
        assertEquals(constrained.cumulativeProbability(-2, true).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(-1.5, true).getValue(), (dist.cumulativeProbability(-1.5d) - dist.cumulativeProbability(-2)) / sumRangeProb, 1E-8);
        assertEquals(constrained.cumulativeProbability(-1, true).getValue(), 0.5, 1E-8);
        assertEquals(constrained.cumulativeProbability(1, true).getValue(), 0.5, 1E-8);
        assertEquals(constrained.cumulativeProbability(1.5, true).getValue(), (rangeLeftProb / sumRangeProb) + ((dist.cumulativeProbability(1.5) - dist.cumulativeProbability(1.0)) / sumRangeProb));
        assertEquals(constrained.cumulativeProbability(2, true).getValue(), 1.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(3, true).getValue(), 1.0, 1E-8);
    }

    /**
     * Ensures assumptions made in tests for uniform distributions apply correctly for
     * a non-uniform distribution.
     */
    @Test
    public void testNormalDistribution()
    {
        // standard normal
        StandardNormalHistogram source = new StandardNormalHistogram();
        RealDistribution dist = source.getDistribution();
        ConnectorHistogram constrained = new DisjointRangeDomainHistogram(source, ImmutableSet.of(rangeOpen(-1d, 1d)));
        assertEquals(constrained.cumulativeProbability(-1.0, true).getValue(), 0.0, 1E-8);
        assertEquals(constrained.cumulativeProbability(0.0, true).getValue(), 0.5, 1E-8);
        assertEquals(constrained.cumulativeProbability(1.0, true).getValue(), 1.0, 1E-8);
        double probability = (dist.cumulativeProbability(-0.5) - dist.cumulativeProbability(-1.0)) / (dist.cumulativeProbability(1.0) - dist.cumulativeProbability(-1));
        assertEquals(constrained.cumulativeProbability(-0.5, true).getValue(), probability, 1E-8);
        assertEquals(constrained.cumulativeProbability(0.5, true).getValue(), probability + (1.0 - (2 * probability)), 1E-8);

        assertEquals(constrained.inverseCumulativeProbability(0.0).getValue(), -1.0d, 1E-8);
        probability = dist.inverseCumulativeProbability(dist.cumulativeProbability(-1) + 0.25 * (dist.cumulativeProbability(1) - dist.cumulativeProbability(-1)));
        assertEquals(constrained.inverseCumulativeProbability(0.25).getValue(), -0.44177054668d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.5).getValue(), 0.0d, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(0.75).getValue(), -1 * probability, 1E-8);
        assertEquals(constrained.inverseCumulativeProbability(1.0).getValue(), 1.0d, 1E-8);
    }

    /**
     * Ensures disjunctions of ranges works properly
     */
    @Test
    public void testAddDisjunction()
    {
        ConnectorHistogram source = new UniformDistributionHistogram(0, 100);
        DisjointRangeDomainHistogram constrained = disjunction(source, rangeOpen(-1d, 2d));
        assertEquals(constrained.getRanges().getOrderedRanges().size(), 1);
        assertEquals(ranges(constrained).get(0), range(DOUBLE, 0d, true, 2d, false));
        constrained = disjunction(constrained, rangeOpen(1d, 10d));
        assertEquals(ranges(constrained).size(), 1);
        assertEquals(ranges(constrained).get(0), range(DOUBLE, 0d, true, 10d, false));
        constrained = disjunction(constrained, range(DOUBLE, 50d, true, 100d, false));
        assertEquals(ranges(constrained).size(), 2);
        assertEquals(ranges(constrained).get(0), range(DOUBLE, 0d, true, 10d, false));
        assertEquals(ranges(constrained).get(1), range(DOUBLE, 50d, true, 100d, false));
    }

    /**
     * Ensures conjunctions of ranges works properly
     */
    @Test
    public void testAddConjunction()
    {
        ConnectorHistogram source = new UniformDistributionHistogram(0, 100);
        DisjointRangeDomainHistogram constrained = disjunction(source, rangeOpen(10d, 90d));
        assertEquals(constrained.getRanges().getOrderedRanges().size(), 1);
        assertEquals(ranges(constrained).get(0), rangeOpen(10d, 90d));
        constrained = conjunction(constrained, lessThanOrEqual(DOUBLE, 50d));
        assertEquals(ranges(constrained).size(), 1);
        assertEquals(ranges(constrained).get(0), range(DOUBLE, 10d, false, 50d, true));
        constrained = conjunction(constrained, greaterThanOrEqual(DOUBLE, 25d));
        assertEquals(ranges(constrained).size(), 1);
        assertEquals(ranges(constrained).get(0), rangeClosed(25d, 50d));
    }

    private static DisjointRangeDomainHistogram disjunction(ConnectorHistogram source, com.facebook.presto.common.predicate.Range range)
    {
        return (DisjointRangeDomainHistogram) DisjointRangeDomainHistogram.addDisjunction(source, range);
    }

    private static DisjointRangeDomainHistogram conjunction(ConnectorHistogram source, com.facebook.presto.common.predicate.Range range)
    {
        return (DisjointRangeDomainHistogram) DisjointRangeDomainHistogram.addConjunction(source, range);
    }

    private static List<com.facebook.presto.common.predicate.Range> ranges(DisjointRangeDomainHistogram hist)
    {
        return hist.getRanges().getOrderedRanges();
    }

    private static com.facebook.presto.common.predicate.Range rangeOpen(double low, double high)
    {
        return range(DOUBLE, low, false, high, false);
    }

    private static com.facebook.presto.common.predicate.Range rangeClosed(double low, double high)
    {
        return range(DOUBLE, low, true, high, true);
    }

    private static class StandardNormalHistogram
            implements ConnectorHistogram
    {
        private final NormalDistribution distribution = new NormalDistribution();

        public NormalDistribution getDistribution()
        {
            return distribution;
        }

        @Override
        public Estimate cumulativeProbability(double value, boolean inclusive)
        {
            return Estimate.of(distribution.cumulativeProbability(value));
        }

        @Override
        public Estimate inverseCumulativeProbability(double percentile)
        {
            // assume lower/upper limit is 10, in order to not throw
            // exception, even though technically the bounds are technically
            // INF
            if (percentile <= 0.0) {
                return Estimate.of(-10);
            }
            if (percentile >= 1.0) {
                return Estimate.of(10);
            }
            return Estimate.of(distribution.inverseCumulativeProbability(percentile));
        }

        @Override
        public long getEstimatedSize()
        {
            return 0;
        }
    }

    @Override
    ConnectorHistogram createHistogram()
    {
        RealDistribution distribution = getDistribution();
        return new DisjointRangeDomainHistogram(
                new UniformDistributionHistogram(
                        distribution.getSupportLowerBound(), distribution.getSupportUpperBound()))
                .addDisjunction(rangeClosed(0.0, 100.0));
    }

    @Override
    double getDistinctValues()
    {
        return 100;
    }

    @Override
    RealDistribution getDistribution()
    {
        return new UniformRealDistribution(0.0, 100.0);
    }

    /**
     * Support depends on the underlying distribution.
     */
    @Override
    public void testInclusiveExclusive()
    {
    }
}
