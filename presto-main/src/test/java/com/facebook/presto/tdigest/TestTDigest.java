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

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.tdigest;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.GeometricDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static java.lang.String.format;
import static java.util.Collections.sort;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTDigest
{
    private static final int NUMBER_OF_ENTRIES = 1_000_000;
    private static final int STANDARD_COMPRESSION_FACTOR = 100;
    private static final double STANDARD_ERROR = 0.01;
    private static final double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
            0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

    @Test
    public void testAddElementsInOrder()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            tDigest.add(i);
            list.add(i);
        }

        assertSumInts(list, tDigest);

        for (int i = 0; i < quantile.length; i++) {
            assertDiscreteWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testMergeTwoDistributionsWithoutOverlap()
    {
        TDigest tDigest1 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            tDigest1.add(i);
            tDigest2.add(i + NUMBER_OF_ENTRIES / 2);
            list.add(i);
            list.add(i + NUMBER_OF_ENTRIES / 2);
        }

        tDigest1.merge(tDigest2);
        assertSumInts(list, tDigest1);
        sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertDiscreteWithinBound(quantile[i], STANDARD_ERROR, list, tDigest1);
        }
    }

    @Test
    public void testMergeTwoDistributionsWithOverlap()
    {
        TDigest tDigest1 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            tDigest1.add(i);
            tDigest2.add(i);
            list.add(i);
            list.add(i);
        }

        tDigest2.merge(tDigest1);
        assertSumInts(list, tDigest2);
        sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertDiscreteWithinBound(quantile[i], STANDARD_ERROR, list, tDigest2);
        }
    }

    @Test
    public void testAddElementsRandomized()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = Math.random() * NUMBER_OF_ENTRIES;
            tDigest.add(value);
            list.add(value);
        }

        assertSum(list, tDigest);

        sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testNormalDistributionLowVariance()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();
        NormalDistribution normal = new NormalDistribution(1000, 1);

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            tDigest.add(value);
            list.add(value);
        }

        assertSum(list, tDigest);

        sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testLargeScalePreservesWeights()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        NormalDistribution normal = new NormalDistribution(1000, 100);

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            tDigest.add(normal.sample());
        }

        tDigest.scale(Integer.MAX_VALUE * 2.0);

        for (Centroid centroid : tDigest.centroids()) {
            assertTrue(centroid.getWeight() > Integer.MAX_VALUE);
        }
    }

    @Test
    public void testNormalDistributionHighVariance()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();
        NormalDistribution normal = new NormalDistribution(0, 1);

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            tDigest.add(value);
            list.add(value);
        }

        assertSum(list, tDigest);

        sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testMergeTwoNormalDistributions()
    {
        TDigest tDigest1 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(0, 50);

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            double value1 = normal.sample();
            double value2 = normal.sample();
            tDigest1.add(value1);
            tDigest2.add(value2);
            list.add(value1);
            list.add(value2);
        }

        tDigest1.merge(tDigest2);
        assertSum(list, tDigest1);
        sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest1);
        }
    }

    @Test
    public void testMergeManySmallNormalDistributions()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        int digests = 100_000;

        for (int k = 0; k < digests; k++) {
            TDigest current = createTDigest(STANDARD_COMPRESSION_FACTOR);
            for (int i = 0; i < 10; i++) {
                double value = normal.sample();
                current.add(value);
                list.add(value);
            }
            tDigest.merge(current);
        }

        assertSum(list, tDigest);

        sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test
    public void testMergeManyLargeNormalDistributions()
    {
        TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        int digests = 1000;

        for (int k = 0; k < digests; k++) {
            TDigest current = createTDigest(STANDARD_COMPRESSION_FACTOR);
            for (int i = 0; i < NUMBER_OF_ENTRIES / digests; i++) {
                double value = normal.sample();
                current.add(value);
                list.add(value);
            }
            tDigest.merge(current);
        }

        assertSum(list, tDigest);

        sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    // disabled because test takes almost 10s
    @Test(enabled = false)
    public void testBinomialDistribution()
    {
        int trials = 10;
        for (int k = 1; k < trials; k++) {
            TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
            BinomialDistribution binomial = new BinomialDistribution(trials, k * 0.1);
            List<Integer> list = new ArrayList<>();

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = binomial.sample();
                tDigest.add(sample);
                list.add(sample);
            }

            assertSumInts(list, tDigest);

            Collections.sort(list);

            for (int i = 0; i < quantile.length; i++) {
                assertDiscreteWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
            }
        }
    }

    @Test(enabled = false)
    public void testGeometricDistribution()
    {
        int trials = 10;
        for (int k = 1; k < trials; k++) {
            TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
            GeometricDistribution geometric = new GeometricDistribution(k * 0.1);
            List<Integer> list = new ArrayList<Integer>();

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = geometric.sample();
                tDigest.add(sample);
                list.add(sample);
            }

            assertSumInts(list, tDigest);

            Collections.sort(list);

            for (int i = 0; i < quantile.length; i++) {
                assertDiscreteWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
            }
        }
    }

    @Test(enabled = false)
    public void testPoissonDistribution()
    {
        int trials = 10;
        for (int k = 1; k < trials; k++) {
            TDigest tDigest = createTDigest(STANDARD_COMPRESSION_FACTOR);
            PoissonDistribution poisson = new PoissonDistribution(k * 0.1);
            List<Integer> list = new ArrayList<Integer>();

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = poisson.sample();
                tDigest.add(sample);
                list.add(sample);
            }

            assertSumInts(list, tDigest);

            Collections.sort(list);

            for (int i = 0; i < quantile.length; i++) {
                assertDiscreteWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
            }
        }
    }

    private void assertContinuousWithinBound(double quantile, double bound, List<Double> values, TDigest tDigest)
    {
        double lowerBound = quantile - bound;
        double upperBound = quantile + bound;

        if (lowerBound < 0) {
            lowerBound = tDigest.getMin();
        }
        else {
            lowerBound = values.get((int) (NUMBER_OF_ENTRIES * lowerBound));
        }

        if (upperBound >= 1) {
            upperBound = tDigest.getMax();
        }
        else {
            upperBound = values.get((int) (NUMBER_OF_ENTRIES * upperBound));
        }

        assertTrue(tDigest.getQuantile(quantile) >= lowerBound && tDigest.getQuantile(quantile) <= upperBound,
                format("Value %s is outside bound [%s, %s] for quantile %s",
                        tDigest.getQuantile(quantile), lowerBound, upperBound, quantile));
    }

    private void assertDiscreteWithinBound(double quantile, double bound, List<Integer> values, TDigest tDigest)
    {
        double lowerBound = quantile - bound;
        double upperBound = quantile + bound;

        if (lowerBound < 0) {
            lowerBound = tDigest.getMin();
        }
        else {
            lowerBound = values.get((int) (NUMBER_OF_ENTRIES * lowerBound));
        }

        if (upperBound >= 1) {
            upperBound = tDigest.getMax();
        }
        else {
            upperBound = values.get((int) (NUMBER_OF_ENTRIES * upperBound));
        }
        // for discrete distributions, t-digest usually gives back a double that is between 2 integers (2.88, 1.16, etc)
        // however, a discrete distribution should return an integer, not something in between
        // we use Math.rint to round to the nearest integer, since casting as (int) always rounds down and no casting results in error > 1%
        assertTrue(Math.rint(tDigest.getQuantile(quantile)) >= lowerBound && Math.rint(tDigest.getQuantile(quantile)) <= upperBound,
                format("Value %s is outside bound [%s, %s] for quantile %s", tDigest.getQuantile(quantile), lowerBound, upperBound, quantile));
    }

    private void assertSumInts(List<Integer> values, TDigest tDigest)
    {
        assertSum(values.stream().map(Double::new).collect(Collectors.toList()), tDigest);
    }

    private void assertSum(List<Double> values, TDigest tDigest)
    {
        double expectedSum = values.stream().reduce(0.0d, Double::sum);
        assertEquals(tDigest.getSum(), expectedSum, 0.0001);
    }
}
