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
package com.facebook.presto.facebook.qdigest;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestQuantileDigest
{
    @Test
    public void testSingleAdd()
    {
        QuantileDigest digest = new QuantileDigest(1);
        digest.add(0);

        digest.validate();

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);

        assertEquals(digest.getCount(), (double) 1);
        assertEquals(digest.getNodeCount(), 1);
    }

    @Test
    public void testNegativeValues()
    {
        QuantileDigest digest = new QuantileDigest(1);
        addAll(digest, asList(-1, -2, -3, -4, -5, 0, 1, 2, 3, 4, 5));

        assertEquals(digest.getCount(), (double) 11);
    }

    @Test
    public void testRepeatedValue()
    {
        QuantileDigest digest = new QuantileDigest(1);
        digest.add(0);
        digest.add(0);

        digest.validate();

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);

        assertEquals(digest.getCount(), (double) 2);
        assertEquals(digest.getNodeCount(), 1);
    }

    @Test
    public void testTwoDistinctValues()
    {
        QuantileDigest digest = new QuantileDigest(1);
        digest.add(0);
        digest.add(Long.MAX_VALUE);

        digest.validate();

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);
        assertEquals(digest.getCount(), (double) 2);
        assertEquals(digest.getNodeCount(), 3);
    }

    @Test
    public void testTreeBuilding()
    {
        QuantileDigest digest = new QuantileDigest(1);

        List<Integer> values = asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7);
        addAll(digest, values);

        assertEquals(digest.getCount(), (double) values.size());
    }

    @Test
    public void testTreeBuildingReverse()
    {
        QuantileDigest digest = new QuantileDigest(1);

        List<Integer> values = asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7);
        addAll(digest, Lists.reverse(values));

        assertEquals(digest.getCount(), (double) values.size());
    }

    @Test
    public void testBasicCompression()
    {
        // maxError = 0.8 so that we get compression factor = 5 with the data below
        QuantileDigest digest = new QuantileDigest(0.8, 0, new TestingTicker());

        List<Integer> values = asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7);
        addAll(digest, values);

        digest.compress();
        digest.validate();

        assertEquals(digest.getCount(), (double) values.size());
        assertEquals(digest.getNodeCount(), 7);
        assertEquals(digest.getConfidenceFactor(), 0.2);
    }

    @Test
    public void testCompression()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(1, 0, new TestingTicker());

        for (int loop = 0; loop < 2; ++loop) {
            addRange(digest, 0, 15);

            digest.compress();
            digest.validate();
        }
    }

    @Test
    public void testQuantile()
    {
        QuantileDigest digest = new QuantileDigest(1);

        addAll(digest, asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);

        assertEquals(digest.getQuantile(0.0), 0);
        assertEquals(digest.getQuantile(0.1), 1);
        assertEquals(digest.getQuantile(0.2), 2);
        assertEquals(digest.getQuantile(0.3), 3);
        assertEquals(digest.getQuantile(0.4), 4);
        assertEquals(digest.getQuantile(0.5), 5);
        assertEquals(digest.getQuantile(0.6), 6);
        assertEquals(digest.getQuantile(0.7), 7);
        assertEquals(digest.getQuantile(0.8), 8);
        assertEquals(digest.getQuantile(0.9), 9);
        assertEquals(digest.getQuantile(1), 9);
    }

    @Test
    public void testQuantileLowerBound()
    {
        QuantileDigest digest = new QuantileDigest(0.5);

        addRange(digest, 1, 100);

        assertEquals(digest.getQuantileLowerBound(0.0), 1);
        for (int i = 1; i <= 10; i++) {
            assertTrue(digest.getQuantileLowerBound(i / 10.0) <= i * 10);
            if (i > 5) {
                assertTrue(digest.getQuantileLowerBound(i / 10.0) >= (i - 5) * 10);
            }
        }

        assertEquals(
                digest.getQuantilesLowerBound(ImmutableList.of(0.0, 0.1, 0.2)),
                ImmutableList.of(digest.getQuantileLowerBound(0.0), digest.getQuantileLowerBound(0.1), digest.getQuantileLowerBound(0.2)));
    }

    @Test
    public void testQuantileUpperBound()
    {
        QuantileDigest digest = new QuantileDigest(0.5);

        addRange(digest, 1, 100);

        assertEquals(digest.getQuantileUpperBound(1.0), 99);
        for (int i = 0; i < 10; i++) {
            assertTrue(digest.getQuantileUpperBound(i / 10.0) >= i * 10);
            if (i < 5) {
                assertTrue(digest.getQuantileUpperBound(i / 10.0) <= (i + 5) * 10);
            }
        }

        assertEquals(
                digest.getQuantilesUpperBound(ImmutableList.of(0.8, 0.9, 1.0)),
                ImmutableList.of(digest.getQuantileUpperBound(0.8), digest.getQuantileUpperBound(0.9), digest.getQuantileUpperBound(1.0)));
    }

    @Test
    public void testWeightedValues()
    {
        QuantileDigest digest = new QuantileDigest(1);

        digest.add(0, 3);
        digest.add(2, 1);
        digest.add(4, 5);
        digest.add(5, 1);
        digest.validate();

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);

        assertEquals(digest.getQuantile(0.0), 0);
        assertEquals(digest.getQuantile(0.1), 0);
        assertEquals(digest.getQuantile(0.2), 0);
        assertEquals(digest.getQuantile(0.3), 2);
        assertEquals(digest.getQuantile(0.4), 4);
        assertEquals(digest.getQuantile(0.5), 4);
        assertEquals(digest.getQuantile(0.6), 4);
        assertEquals(digest.getQuantile(0.7), 4);
        assertEquals(digest.getQuantile(0.8), 4);
        assertEquals(digest.getQuantile(0.9), 5);
        assertEquals(digest.getQuantile(1), 5);
    }

    @Test
    public void testBatchQuantileQuery()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(1);

        addAll(digest, asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);

        assertEquals(digest.getQuantiles(asList(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)),
                asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 9L));
    }

    @Test
    public void testHistogramQuery()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(1);

        addAll(digest, asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);

        assertEquals(digest.getHistogram(asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)),
                asList(new QuantileDigest.Bucket(0, Double.NaN),
                        new QuantileDigest.Bucket(1, 0),
                        new QuantileDigest.Bucket(1, 1),
                        new QuantileDigest.Bucket(1, 2),
                        new QuantileDigest.Bucket(1, 3),
                        new QuantileDigest.Bucket(1, 4),
                        new QuantileDigest.Bucket(1, 5),
                        new QuantileDigest.Bucket(1, 6),
                        new QuantileDigest.Bucket(1, 7),
                        new QuantileDigest.Bucket(1, 8),
                        new QuantileDigest.Bucket(1, 9)));

        assertEquals(digest.getHistogram(asList(7L, 10L)),
                asList(new QuantileDigest.Bucket(7, 3),
                        new QuantileDigest.Bucket(3, 8)));

        // test some edge conditions
        assertEquals(digest.getHistogram(asList(0L)), asList(new QuantileDigest.Bucket(0, Double.NaN)));
        assertEquals(digest.getHistogram(asList(9L)), asList(new QuantileDigest.Bucket(9, 4)));
        assertEquals(digest.getHistogram(asList(10L)), asList(new QuantileDigest.Bucket(10, 4.5)));
        assertEquals(digest.getHistogram(asList(Long.MAX_VALUE)),
                asList(new QuantileDigest.Bucket(10, 4.5)));
    }

    @Test
    public void testHistogramOfDoublesQuery()
    {
        QuantileDigest digest = new QuantileDigest(1);

        LongStream.range(-10, 10)
                .map(TestQuantileDigest::doubleToSortableLong)
                .boxed()
                .forEach(digest::add);

        assertEquals(digest.getConfidenceFactor(), 0.0);

        List<Long> bucketUpperBounds = LongStream.range(-10, 10)
                .map(TestQuantileDigest::doubleToSortableLong)
                .boxed()
                .collect(toImmutableList());

        QuantileDigest.MiddleFunction middleFunction = (lowerBound, upperBound) -> {
            // qdigest will put the range at the top of the tree as the entire set of long values.  Sortable long values
            // which equal Long.MIN_VALUE or Long.MAX_VALUE are NaN values in IEEE 754 standard, therefore they can't
            // be accurately represented as floating point numbers.  Because NaN values cannot be used in the middle
            // calculation, treat them as Double.MIN_VALUE when the min is encountered, and Double.MAX_VALUE when the max
            // is encountered.
            double left = lowerBound > Long.MIN_VALUE ? sortableLongToDouble(lowerBound) : -1 * Double.MAX_VALUE;
            double right = upperBound < Long.MAX_VALUE ? sortableLongToDouble(upperBound) : Double.MAX_VALUE;
            return left + (right - left) / 2;
        };

        List<QuantileDigest.Bucket> expected = LongStream.range(-9, 10)
                .boxed()
                .map(i -> new QuantileDigest.Bucket(1, i - 1))
                .collect(Collectors.toList());
        expected.add(0, new QuantileDigest.Bucket(0, Double.NaN));
        assertEquals(digest.getHistogram(bucketUpperBounds, middleFunction),
                expected);

        assertEquals(digest.getHistogram(asList(doubleToSortableLong(7), doubleToSortableLong(10)), middleFunction),
                asList(new QuantileDigest.Bucket(17, -2.0),
                        new QuantileDigest.Bucket(3, 8)));

        // edge cases
        assertEquals(digest.getHistogram(asList(doubleToSortableLong(-1 * Double.MAX_VALUE)), middleFunction),
                asList(new QuantileDigest.Bucket(0, Double.NaN)));
        assertEquals(digest.getHistogram(asList(doubleToSortableLong(-1 * Double.MAX_VALUE), doubleToSortableLong(-1 * Double.MAX_VALUE + 1)), middleFunction),
                asList(new QuantileDigest.Bucket(0, Double.NaN), new QuantileDigest.Bucket(0, Double.NaN)));
        assertEquals(digest.getHistogram(asList(doubleToSortableLong(0)), middleFunction),
                asList(new QuantileDigest.Bucket(10.0, -5.5)));
        assertEquals(digest.getHistogram(asList(doubleToSortableLong(9)), middleFunction),
                asList(new QuantileDigest.Bucket(19, -1.0)));
        assertEquals(digest.getHistogram(asList(doubleToSortableLong(10)), middleFunction),
                asList(new QuantileDigest.Bucket(20, -0.5)));
        assertEquals(digest.getHistogram(asList(doubleToSortableLong(Double.MAX_VALUE)), middleFunction),
                asList(new QuantileDigest.Bucket(20, -0.5)));
    }

    @Test
    public void testHistogramQueryAfterCompression()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(0.1);

        int total = 10000;
        addRange(digest, 0, total);

        // compression should've run at this error rate and count
        assertTrue(digest.getConfidenceFactor() > 0.0);

        double actualMaxError = digest.getConfidenceFactor();

        for (long value = 0; value < total; ++value) {
            QuantileDigest.Bucket bucket = digest.getHistogram(asList(value)).get(0);

            // estimated count should have an absolute error smaller than 2 * maxError * N
            assertTrue(Math.abs(bucket.getCount() - value) < 2 * actualMaxError * total);
        }
    }

    @Test
    public void testQuantileQueryError()
    {
        double maxError = 0.1;

        QuantileDigest digest = new QuantileDigest(maxError);

        int count = 10000;
        addRange(digest, 0, count);

        // compression should've run at this error rate and count
        assertTrue(digest.getConfidenceFactor() > 0);
        assertTrue(digest.getConfidenceFactor() < maxError);

        for (int value = 0; value < count; ++value) {
            double quantile = value * 1.0 / count;
            long estimatedValue = digest.getQuantile(quantile);

            // true rank of estimatedValue is == estimatedValue because
            // we've inserted a list of ordered numbers starting at 0
            double error = Math.abs(estimatedValue - quantile * count) * 1.0 / count;

            assertTrue(error < maxError);
        }
    }

    @Test
    public void testDecayedQuantiles()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        QuantileDigest digest = new QuantileDigest(1, computeAlpha(0.5, 60), ticker);

        addAll(digest, asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);

        ticker.increment(60, TimeUnit.SECONDS);
        addAll(digest, asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));

        // Considering that the first 10 values now have a weight of 0.5 per the alpha factor, they only contributed a count
        // of 5 to rank computations. Therefore, the 50th percentile is equivalent to a weighted rank of (5 + 10) / 2 = 7.5,
        // which corresponds to value 12
        assertEquals(digest.getQuantile(0.5), 12);
    }

    @Test
    public void testDecayedCounts()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        QuantileDigest digest = new QuantileDigest(1, computeAlpha(0.5, 60), ticker);

        addAll(digest, asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // should have no compressions with so few values and the allowed error
        assertEquals(digest.getConfidenceFactor(), 0.0);

        ticker.increment(60, TimeUnit.SECONDS);
        addAll(digest, asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));

        assertEquals(digest.getCount(), 15.0);
    }

    @Test
    public void testDecayedCountsWithClockIncrementSmallerThanRescaleThreshold()
            throws Exception
    {
        int targetAgeInSeconds = (int) (QuantileDigest.RESCALE_THRESHOLD_SECONDS - 1);

        TestingTicker ticker = new TestingTicker();
        QuantileDigest digest = new QuantileDigest(1,
                computeAlpha(0.5, targetAgeInSeconds), ticker);

        addAll(digest, asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        ticker.increment(targetAgeInSeconds, TimeUnit.SECONDS);
        addAll(digest, asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));

        assertEquals(digest.getCount(), 15.0);
    }

    @Test
    public void testMinMax()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(0.01, 0, new TestingTicker());

        int from = 500;
        int to = 700;
        addRange(digest, from, to + 1);

        assertEquals(digest.getMin(), from);
        assertEquals(digest.getMax(), to);
    }

    @Test
    public void testMinMaxWithDecay()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();

        QuantileDigest digest = new QuantileDigest(0.01,
                computeAlpha(QuantileDigest.ZERO_WEIGHT_THRESHOLD, 60), ticker);

        addRange(digest, 1, 10);

        ticker.increment(1000, TimeUnit.SECONDS); // TODO: tighter bounds?

        int from = 4;
        int to = 7;
        addRange(digest, from, to + 1);

        digest.validate();

        assertEquals(digest.getMin(), from);
        assertEquals(digest.getMax(), to);
    }

    @Test
    public void testRescaleWithDecayKeepsCompactTree()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        int targetAgeInSeconds = (int) (QuantileDigest.RESCALE_THRESHOLD_SECONDS);

        QuantileDigest digest = new QuantileDigest(0.01,
                computeAlpha(QuantileDigest.ZERO_WEIGHT_THRESHOLD / 2, targetAgeInSeconds),
                ticker);

        for (int i = 0; i < 10; ++i) {
            digest.add(i);
            digest.validate();

            // bump the clock to make all previous values decay to ~0
            ticker.increment(targetAgeInSeconds, TimeUnit.SECONDS);
        }

        assertEquals(digest.getNodeCount(), 1);
    }

    @Test
    public void testEquivalenceEmpty()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);

        assertTrue(a.equivalent(b));
    }

    @Test
    public void testEquivalenceSingle()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);

        a.add(1);
        b.add(1);

        assertTrue(a.equivalent(b));
    }

    @Test
    public void testEquivalenceSingleDifferent()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);

        a.add(1);
        b.add(2);

        assertFalse(a.equivalent(b));
    }

    @Test
    public void testEquivalenceComplex()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);

        addAll(a, asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7));
        addAll(b, asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7));

        assertTrue(a.equivalent(b));
    }

    @Test
    public void testEquivalenceComplexDifferent()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);

        addAll(a, asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7));
        addAll(b, asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7, 8));

        assertFalse(a.equivalent(b));
    }

    @Test
    public void testMergeEmpty()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);
        QuantileDigest pristineB = new QuantileDigest(0.01);

        a.merge(b);

        a.validate();
        b.validate();

        assertTrue(b.equivalent(pristineB));

        assertEquals(a.getCount(), 0.0);
        assertEquals(a.getNodeCount(), 0);

        assertEquals(b.getCount(), 0.0);
        assertEquals(b.getNodeCount(), 0);
    }

    @Test
    public void testMergeIntoEmpty()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);
        QuantileDigest pristineB = new QuantileDigest(0.01);

        b.add(1);
        pristineB.add(1);

        a.merge(b);

        a.validate();
        b.validate();

        assertTrue(b.equivalent(pristineB));

        assertEquals(a.getCount(), 1.0);
        assertEquals(a.getNodeCount(), 1);

        assertEquals(b.getCount(), 1.0);
        assertEquals(b.getNodeCount(), 1);
    }

    @Test
    public void testMergeWithEmpty()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);
        QuantileDigest pristineB = new QuantileDigest(0.01);

        a.add(1);
        a.merge(b);

        a.validate();
        b.validate();

        assertTrue(b.equivalent(pristineB));

        assertEquals(a.getCount(), 1.0);
        assertEquals(a.getNodeCount(), 1);

        assertEquals(b.getCount(), 0.0);
        assertEquals(b.getNodeCount(), 0);
    }

    @Test
    public void testMergeSample()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);

        a.add(1);
        addAll(b, asList(2, 3));

        a.merge(b);

        a.validate();

        assertEquals(a.getCount(), 3.0);
        assertEquals(a.getNodeCount(), 5);
    }

    @Test
    public void testMergeSeparateBranches()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);
        QuantileDigest pristineB = new QuantileDigest(0.01);

        a.add(1);

        b.add(2);
        pristineB.add(2);

        a.merge(b);

        assertTrue(b.equivalent(pristineB));

        assertEquals(a.getCount(), 2.0);
        assertEquals(a.getNodeCount(), 3);

        assertEquals(b.getCount(), 1.0);
        assertEquals(b.getNodeCount(), 1);
    }

    @Test
    public void testMergeWithLowerLevel()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(1, 0, Ticker.systemTicker());
        QuantileDigest b = new QuantileDigest(1, 0, Ticker.systemTicker());
        QuantileDigest pristineB = new QuantileDigest(1, 0, Ticker.systemTicker());

        a.add(6);
        a.compress();

        List<Integer> values = asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5);

        addAll(b, values);
        b.compress();

        addAll(pristineB, values);
        pristineB.compress();

        a.merge(b);

        assertTrue(b.equivalent(pristineB));

        assertEquals(a.getCount(), 14.0);
        assertEquals(b.getCount(), 13.0);
    }

    @Test
    public void testMergeWithHigherLevel()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(1, 0, Ticker.systemTicker());
        QuantileDigest b = new QuantileDigest(1, 0, Ticker.systemTicker());
        QuantileDigest pristineB = new QuantileDigest(1, 0, Ticker.systemTicker());

        addAll(a, asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5));

        a.compress();

        addAll(b, asList(6, 7));
        addAll(pristineB, asList(6, 7));

        a.merge(b);

        assertTrue(b.equivalent(pristineB));

        assertEquals(a.getCount(), 15.0);
        assertEquals(a.getNodeCount(), 7);

        assertEquals(b.getCount(), 2.0);
        assertEquals(b.getNodeCount(), 3);
    }

    // test merging two digests that have a node at the highest level to make sure
    // we handle boundary conditions properly
    @Test
    public void testMergeMaxLevel()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(0.01);
        QuantileDigest b = new QuantileDigest(0.01);
        QuantileDigest pristineB = new QuantileDigest(0.01);

        addAll(a, asList(-1, 1));
        addAll(b, asList(-2, 2));
        addAll(pristineB, asList(-2, 2));
        a.merge(b);

        a.validate();
        b.validate();

        assertTrue(b.equivalent(pristineB));

        assertEquals(a.getCount(), 4.0);
        assertEquals(a.getNodeCount(), 7);
    }

    @Test
    public void testMergeSameLevel()
            throws Exception
    {
        QuantileDigest a = new QuantileDigest(1, 0, Ticker.systemTicker());
        QuantileDigest b = new QuantileDigest(1, 0, Ticker.systemTicker());
        QuantileDigest pristineB = new QuantileDigest(1, 0, Ticker.systemTicker());

        a.add(0);
        b.add(0);
        pristineB.add(0);

        a.merge(b);

        assertTrue(b.equivalent(pristineB));

        assertEquals(a.getCount(), 2.0);
        assertEquals(a.getNodeCount(), 1);

        assertEquals(b.getCount(), 1.0);
        assertEquals(b.getNodeCount(), 1);
    }

    @Test
    public void testSerializationEmpty()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(0.01);
        QuantileDigest deserialized = deserialize(digest.serialize());

        assertTrue(digest.equivalent(deserialized));
    }

    @Test
    public void testSerializationSingle()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(0.01);
        digest.add(1);

        assertTrue(digest.equivalent(deserialize(digest.serialize())));
    }

    @Test
    public void testSerializationComplex()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(1);
        addAll(digest, asList(0, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7));

        assertTrue(digest.equivalent(deserialize(digest.serialize())));

        digest.compress();

        assertTrue(digest.equivalent(deserialize(digest.serialize())));
    }

    @Test
    public void testSerializationWithExtremeEndsOfLong()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(1);
        digest.add(Long.MIN_VALUE);
        digest.add(Long.MAX_VALUE);

        assertTrue(digest.equivalent(deserialize(digest.serialize())));

        digest.compress();

        assertTrue(digest.equivalent(deserialize(digest.serialize())));
    }

    @Test(invocationCount = 1000)
    public void testSerializationRandom()
            throws Exception
    {
        QuantileDigest digest = new QuantileDigest(1);

        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        }

        addAll(digest, values);

        assertTrue(digest.equivalent(deserialize(digest.serialize())), format("Serialization roundtrip failed for input: %s", values));
    }

    private QuantileDigest deserialize(Slice serialized)
            throws IOException
    {
        QuantileDigest result = new QuantileDigest(serialized);
        result.validate();
        return result;
    }

    private void addAll(QuantileDigest digest, List<Integer> values)
    {
        for (int value : values) {
            digest.add(value);
        }
        digest.validate();
    }

    private void addRange(QuantileDigest digest, int from, int to)
    {
        for (int i = from; i < to; ++i) {
            digest.add(i);
        }
        digest.validate();
    }

    private static long doubleToSortableLong(double value)
    {
        long bits = Double.doubleToLongBits(value);
        return bits ^ ((bits >> 63) & Long.MAX_VALUE);
    }

    private static double sortableLongToDouble(long value)
    {
        value = value ^ ((value >> 63) & Long.MAX_VALUE);
        return Double.longBitsToDouble(value);
    }

    public static class TestingTicker
            extends Ticker
    {
        private long time;

        @Override
        public long read()
        {
            return time;
        }

        public void increment(long delta, TimeUnit unit)
        {
            checkArgument(delta >= 0, "delta is negative");
            time += unit.toNanos(delta);
        }
    }

    /**
     * Compute the alpha decay factor such that the weight of an entry with age 'targetAgeInSeconds' is targetWeight'
     */
    public static double computeAlpha(double targetWeight, long targetAgeInSeconds)
    {
        checkArgument(targetAgeInSeconds > 0, "targetAgeInSeconds must be > 0");
        checkArgument(targetWeight > 0 && targetWeight < 1, "targetWeight must be in range (0, 1)");

        return -Math.log(targetWeight) / targetAgeInSeconds;
    }
}
