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
package com.facebook.presto.operator.aggregation.fixedhistogram;

import com.google.common.collect.Streams;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestFixedDoubleBreakdownHistogram
{
    @Test
    public void testGetters()
    {
        final FixedDoubleBreakdownHistogram histogram =
                new FixedDoubleBreakdownHistogram(200, 3.0, 4.0);

        assertEquals(histogram.getBucketCount(), 200);
        assertEquals(histogram.getMin(), 3.0);
        assertEquals(histogram.getMax(), 4.0);
    }

    @Test
    public void testIllegalBucketWeightCount()
    {
        try {
            new FixedDoubleBreakdownHistogram(-200, 3.0, 4.0);
            fail("exception expected");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("bucketCount"));
        }
    }

    @Test
    public void testIllegalMinMax()
    {
        try {
            new FixedDoubleBreakdownHistogram(-200, 3.0, 3.0);
            fail("exception expected");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("bucketCount"));
        }
    }

    @Test
    public void testBasicOps()
    {
        final FixedDoubleBreakdownHistogram histogram =
                new FixedDoubleBreakdownHistogram(200, 3.0, 4.0);

        histogram.add(3.1, 100.0);
        histogram.add(3.8, 200.0);
        histogram.add(3.1, 100.0);
        assertEquals(
                Streams.stream(histogram.iterator()).mapToDouble(c -> c.getWeight()).sum(),
                300.0);
    }

    @Test
    public void testEqualValuesDifferentWeights()
    {
        final FixedDoubleBreakdownHistogram histogram =
                new FixedDoubleBreakdownHistogram(200, 3.0, 4.0);

        histogram.add(3.5, 0.2, 1);
        histogram.add(3.5, 0.4, 1);
        histogram.add(3.5, 0.3, 2);
        Streams.stream(histogram.iterator()).forEach(c -> {
            assertTrue(c.getLeft() <= 3.5);
            assertTrue(c.getRight() >= 3.5);
            assertTrue(c.getLeft() < c.getRight());
        });
        assertEquals(
                Streams.stream(histogram.iterator()).mapToLong(c -> 1).sum(),
                3);
        assertEquals(
                Streams.stream(histogram.iterator()).mapToDouble(c -> c.getWeight()).sum(),
                0.9);
        assertEquals(
                Streams.stream(histogram.iterator()).mapToLong(c -> c.getCount()).sum(),
                4);
    }

    @Test
    public void testMassive()
    {
        final FixedDoubleBreakdownHistogram histogram =
                new FixedDoubleBreakdownHistogram(100, 0.0, 1.0);

        ArrayList<Double> values = new ArrayList<>();
        ArrayList<Double> weights = new ArrayList<>();

        IntStream.range(0, 1000000).forEach(i -> {
            double value = Math.random();
            double weight = ((int) (10 * Math.random())) / 10.0;
            values.add(value);
            weights.add(weight);
            histogram.add(value, weight, 1);
        });

        for (FixedDoubleBreakdownHistogram.BucketWeight b : histogram) {
            long count = IntStream.range(0, values.size())
                    .filter(i -> b.getLeft() < values.get(i) && values.get(i) <= b.getRight() && b.getWeight() == weights.get(i))
                    .count();
            assertEquals(b.getCount(), count);
        }
    }

    @Test
    public void testMassiveMerge()
    {
        ArrayList<Double> values = new ArrayList<>();
        ArrayList<Double> weights = new ArrayList<>();

        final FixedDoubleBreakdownHistogram lhs =
                new FixedDoubleBreakdownHistogram(100, 0.0, 1.0);
        IntStream.range(0, 100).forEach(i -> {
            double value = Math.random();
            double weight = Math.random();
            values.add(value);
            weights.add(weight);
            lhs.add(value, weight, 1);
        });

        final FixedDoubleBreakdownHistogram rhs =
                new FixedDoubleBreakdownHistogram(100, 0.0, 1.0);
        ArrayList<Double> rhsValues = new ArrayList<>();
        ArrayList<Double> rhsWeights = new ArrayList<>();
        IntStream.range(0, 100).forEach(i -> {
            double value = Math.random();
            double weight = Math.random();
            values.add(value);
            weights.add(weight);
            rhsValues.add(value);
            rhsWeights.add(weight);
            rhs.add(value, weight, 1);
        });

        lhs.mergeWith(rhs.clone());

        for (FixedDoubleBreakdownHistogram.BucketWeight b : lhs) {
            long count = IntStream.range(0, values.size())
                    .filter(i -> b.getLeft() < values.get(i) && values.get(i) <= b.getRight() && b.getWeight() == weights.get(i))
                    .count();
            assertEquals(b.getCount(), count);
        }
        for (FixedDoubleBreakdownHistogram.BucketWeight b : rhs) {
            long count = IntStream.range(0, rhsValues.size())
                    .filter(i -> b.getLeft() < rhsValues.get(i) && rhsValues.get(i) <= b.getRight() &&
                            b.getWeight() == rhsWeights.get(i))
                    .count();
            assertEquals(b.getCount(), count);
        }
    }
}
