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

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "bucketCount must be at least 2: -200")
    public void testIllegalBucketCount()
    {
        new FixedDoubleHistogram(-200, 3.0, 4.0);
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "min must be smaller than max: 3.0 3.0")
    public void testIllegalMinMax()
    {
        new FixedDoubleHistogram(200, 3.0, 3.0);
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
        FixedDoubleHistogram histogram =
                new FixedDoubleHistogram(100, 0.0, 1.0);

        ArrayList<Double> values = new ArrayList<>();
        ArrayList<Double> weights = new ArrayList<>();

        for (int i = 0; i < 1000000; ++i) {
            double value = Math.random();
            double weight = ((int) (10 * Math.random())) / 10.0;
            values.add(value);
            weights.add(weight);
            histogram.add(value, weight);
        }

        Streams.stream(histogram.iterator()).forEach(b -> {
            double weight = IntStream.range(0, values.size())
                    .filter(i -> b.getLeft() < values.get(i) && values.get(i) <= b.getRight())
                    .mapToDouble(i -> weights.get(i))
                    .sum();
            assertEquals(b.getWeight(), weight, 0.0001);
        });
    }

    @Test
    public void testMassiveMerge()
    {
        ArrayList<Double> values = new ArrayList<>();
        ArrayList<Double> weights = new ArrayList<>();

        FixedDoubleHistogram left =
                new FixedDoubleHistogram(100, 0.0, 1.0);
        for (int i = 0; i < 100; ++i) {
            double value = Math.random();
            double weight = Math.random();
            values.add(value);
            weights.add(weight);
            left.add(value, weight);
        }

        FixedDoubleHistogram right =
                new FixedDoubleHistogram(100, 0.0, 1.0);
        ArrayList<Double> rightValues = new ArrayList<>();
        ArrayList<Double> rightWeights = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double value = Math.random();
            double weight = Math.random();
            values.add(value);
            weights.add(weight);
            rightValues.add(value);
            rightWeights.add(weight);
            right.add(value, weight);
        }

        left.mergeWith(right.clone());

        Streams.stream(left.iterator()).forEach(b -> {
            double weight = IntStream.range(0, values.size())
                    .filter(i -> b.getLeft() < values.get(i) && values.get(i) <= b.getRight())
                    .mapToDouble(i -> weights.get(i))
                    .sum();
            assertEquals(b.getWeight(), weight, 0.0001);
        });
        Streams.stream(right.iterator()).forEach(b -> {
            double weight = IntStream.range(0, rightValues.size())
                    .filter(i -> b.getLeft() < rightValues.get(i) && rightValues.get(i) <= b.getRight())
                    .mapToDouble(i -> rightWeights.get(i))
                    .sum();
            assertEquals(b.getWeight(), weight, 0.0001);
        });
    }
}
