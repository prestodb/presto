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

public class TestFixedDoubleHistogram
{
    @Test
    public void testGetters()
    {
        FixedDoubleHistogram histogram = new FixedDoubleHistogram(200, 3.0, 4.0);

        assertEquals(histogram.getBucketCount(), 200);
        assertEquals(histogram.getMin(), 3.0);
        assertEquals(histogram.getMax(), 4.0);
    }

    @Test
    public void testIllegalBucketCount()
    {
        try {
            new FixedDoubleHistogram(-200, 3.0, 4.0);
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
            new FixedDoubleHistogram(-200, 3.0, 3.0);
            fail("exception expected");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("bucketCount"));
        }
    }

    @Test
    public void testBasicOps()
    {
        FixedDoubleHistogram histogram = new FixedDoubleHistogram(200, 3.0, 4.0);

        histogram.add(3.1, 100.0);
        histogram.add(3.8, 200.0);
        assertEquals(
                Streams.stream(histogram.iterator()).mapToDouble(c -> c.getWeight()).sum(),
                300.0);
    }

    @Test
    public void testMassive()
    {
        final FixedDoubleHistogram histogram =
                new FixedDoubleHistogram(100, 0.0, 1.0);

        ArrayList<Double> values = new ArrayList<>();
        ArrayList<Double> weights = new ArrayList<>();

        IntStream.range(0, 1000000).forEach(i -> {
            double value = Math.random();
            double weight = ((int) (10 * Math.random())) / 10.0;
            values.add(value);
            weights.add(weight);
            histogram.add(value, weight);
        });

        for (FixedDoubleHistogram.Bucket b : histogram) {
            double weight = IntStream.range(0, values.size())
                    .filter(i -> b.getLeft() < values.get(i) && values.get(i) <= b.getRight())
                    .mapToDouble(i -> weights.get(i))
                    .sum();
            assertEquals(b.getWeight(), weight, 0.0001);
        }
    }

    @Test
    public void testMassiveMerge()
    {
        ArrayList<Double> values = new ArrayList<>();
        ArrayList<Double> weights = new ArrayList<>();

        final FixedDoubleHistogram lhs =
                new FixedDoubleHistogram(100, 0.0, 1.0);
        IntStream.range(0, 100).forEach(i -> {
            double value = Math.random();
            double weight = Math.random();
            values.add(value);
            weights.add(weight);
            lhs.add(value, weight);
        });

        final FixedDoubleHistogram rhs =
                new FixedDoubleHistogram(100, 0.0, 1.0);
        ArrayList<Double> rhsValues = new ArrayList<>();
        ArrayList<Double> rhsWeights = new ArrayList<>();
        IntStream.range(0, 100).forEach(i -> {
            double value = Math.random();
            double weight = Math.random();
            values.add(value);
            weights.add(weight);
            rhsValues.add(value);
            rhsWeights.add(weight);
            rhs.add(value, weight);
        });

        lhs.mergeWith(rhs.clone());

        for (FixedDoubleHistogram.Bucket b : lhs) {
            double weight = IntStream.range(0, values.size())
                    .filter(i -> b.getLeft() < values.get(i) && values.get(i) <= b.getRight())
                    .mapToDouble(i -> weights.get(i))
                    .sum();
            assertEquals(b.getWeight(), weight, 0.0001);
        }
        for (FixedDoubleHistogram.Bucket b : rhs) {
            double weight = IntStream.range(0, rhsValues.size())
                    .filter(i -> b.getLeft() < rhsValues.get(i) && rhsValues.get(i) <= b.getRight())
                    .mapToDouble(i -> rhsWeights.get(i))
                    .sum();
            assertEquals(b.getWeight(), weight, 0.0001);
        }
    }
}
