package com.facebook.presto.operator.scalar;

import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.GeometricDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTDigest
{
    private static final int NUMBER_OF_ENTRIES = 5_000_000;
    private static final int STANDARD_COMPRESSION_FACTOR = 100;
    private static final int MINIMUM_DISCRETE_ERROR = 1;
    private static final double STANDARD_ERROR = 0.01;

    @Test
    public void testAddElementsInOrder()
    {
        TDigest tDigest = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            tDigest.add(i);
        }

        for (int i = 0; i < quantile.length; i++) {
            assertEquals(tDigest.quantile(quantile[i]), NUMBER_OF_ENTRIES * quantile[i], NUMBER_OF_ENTRIES * STANDARD_ERROR);
        }
    }

    @Test
    public void testMergeTwoDistributionsWithoutOverlap()
    {
        TDigest tDigest1 = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
        TDigest tDigest2 = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            tDigest1.add(i);
            tDigest2.add(i + NUMBER_OF_ENTRIES / 2);
        }

        tDigest1.add(tDigest2);

        for (int i = 0; i < quantile.length; i++) {
            assertEquals(tDigest1.quantile(quantile[i]), NUMBER_OF_ENTRIES * quantile[i], NUMBER_OF_ENTRIES * STANDARD_ERROR);
        }
    }

    @DataProvider(name = "compressionFactors")
    public Object[][] dataProvider()
    {
        return new Object[][] {{100}, {500}, {1000}};
    }

    @Test(dataProvider = "compressionFactors")
    public void testMergeTwoDistributionsWithOverlap(int compressionFactor)
    {
        TDigest tDigest1 = TDigest.createMergingDigest(compressionFactor);
        TDigest tDigest2 = TDigest.createMergingDigest(compressionFactor);
        List<Double> list = new ArrayList<Double>();
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};
        double[] quantileExtremes = {0.0001, 0.02000, 0.0300, 0.0400, 0.0500, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            tDigest1.add(i);
            tDigest2.add(i);
            list.add((double) i);
            list.add((double) i);
        }

        tDigest2.add(ImmutableList.of(tDigest1));
        Collections.sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertEquals(tDigest2.quantile(quantile[i]), list.get((int) (NUMBER_OF_ENTRIES * quantile[i])), NUMBER_OF_ENTRIES * STANDARD_ERROR);
        }
    }

    @Test (enabled = false)
    public void testAddElementsRandomized()
    {
        TDigest tDigest = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = Math.random() * NUMBER_OF_ENTRIES;
            tDigest.add(value);
            list.add(value);
        }

        Collections.sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertEquals(tDigest.quantile(quantile[i]), list.get((int) (NUMBER_OF_ENTRIES * quantile[i])), NUMBER_OF_ENTRIES * STANDARD_ERROR);
        }
    }

    @Test
    public void testNormalDistributionLowVariance()
    {
        TDigest tDigest = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
        List<Double> list = new ArrayList<Double>();
        NormalDistribution normal = new NormalDistribution(1000, 1);
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            tDigest.add(value);
            list.add(value);
        }
        Collections.sort(list);
        for (int i = 0; i < quantile.length; i++) {
            assertEquals(tDigest.quantile(quantile[i]), list.get((int) (NUMBER_OF_ENTRIES * quantile[i])), list.get((int) (NUMBER_OF_ENTRIES * quantile[i])) * STANDARD_ERROR);
        }
    }

    @Test(invocationCount = 10)
    public void testNormalDistributionHighVariance()
    {
        TDigest tDigest = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
        QuantileDigest qDigest = new QuantileDigest(STANDARD_ERROR);
        List<Double> list = new ArrayList<Double>();
        NormalDistribution normal = new NormalDistribution(0, 1);
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            tDigest.add(value);
            qDigest.add((long) value);
            list.add(value);
        }
        Collections.sort(list);
        for (int i = 0; i < quantile.length; i++) {
            System.out.println("quantile:" + quantile[i] + " value:" + list.get((int) (NUMBER_OF_ENTRIES * quantile[i])) + " t-digest:" + tDigest.quantile(quantile[i]));
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test(dataProvider = "compressionFactors")
    public void testMergeTwoNormalDistributions(int compressionFactor)
    {
        TDigest tDigest1 = TDigest.createMergingDigest(compressionFactor);
        TDigest tDigest2 = TDigest.createMergingDigest(compressionFactor);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};
        double[] quantileExtremes = {0.0001, 0.02000, 0.0300, 0.0400, 0.0500, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int i = 0; i < NUMBER_OF_ENTRIES / 2; i++) {
            double value1 = normal.sample();
            double value2 = normal.sample();
            tDigest1.add(value1);
            tDigest2.add(value2);
            list.add(value1);
            list.add(value2);
        }

        tDigest1.add(tDigest2);
        Collections.sort(list);

        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest1);
        }
    }
    @Test(dataProvider = "compressionFactors")
    public void testMergeManySmallNormalDistributions(int compressionFactor)
    {
        TDigest tDigest = TDigest.createMergingDigest(compressionFactor);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        int digests = 100_000;
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};
        double[] quantileExtremes = {0.0001, 0.02000, 0.0300, 0.0400, 0.0500, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int k = 0; k < digests; k++) {
            TDigest current = TDigest.createMergingDigest(compressionFactor);
            for (int i = 0; i < 10; i++) {
                double value = normal.sample();
                current.add(value);
                list.add(value);
            }
            tDigest.add(current);
        }
        Collections.sort(list);
        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test(dataProvider = "compressionFactors")
    public void testMergeManyLargeNormalDistributions(int compressionFactor)
    {
        TDigest tDigest = TDigest.createMergingDigest(compressionFactor);
        List<Double> list = new ArrayList<>();
        NormalDistribution normal = new NormalDistribution(500, 20);
        int digests = 1000;
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};
        double[] quantileExtremes = {0.0001, 0.02000, 0.0300, 0.0400, 0.0500, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

        for (int k = 0; k < digests; k++) {
            TDigest current = TDigest.createMergingDigest(compressionFactor);
            for (int i = 0; i < NUMBER_OF_ENTRIES / digests; i++) {
                double value = normal.sample();
                current.add(value);
                list.add(value);
            }
            tDigest.add(current);
        }
        Collections.sort(list);
        for (int i = 0; i < quantile.length; i++) {
            assertContinuousWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
        }
    }

    @Test(dataProvider = "compressionFactors")
    public void testCompareAccuracy(int compressionFactor)
    {
        TDigest tDigest = TDigest.createMergingDigest(compressionFactor);
        QuantileDigest qDigest = new QuantileDigest(STANDARD_ERROR);
        List<Double> values = new ArrayList<>();
        //NormalDistribution normal = new NormalDistribution(500, 20);
        UniformRealDistribution normal = new UniformRealDistribution(0, 1_000_000_000);
        UniformIntegerDistribution uniformReal = new UniformIntegerDistribution(0, 1_000_000_000);
        double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};
        double[] quantileExtremes = {0.0001, 0.0005, 0.0010, 0.0050, 0.0100, 0.9900, 0.9950, 0.9990, 0.9995, 0.9999};

        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            double value = normal.sample();
            qDigest.add((long) value);
            tDigest.add(value);
            values.add(value);
        }
        int success = 0;
        int total = 0;
        Collections.sort(values);
        for (int i = 0; i < quantile.length; i++) {
            double errorQuantileT = normal.cumulativeProbability(tDigest.quantile(quantile[i]));
            errorQuantileT = (double) Math.round(errorQuantileT * 10000d) / 10000d;

            double errorQuantileQ = normal.cumulativeProbability(qDigest.getQuantile(quantile[i]));
            errorQuantileQ = (double) Math.round(errorQuantileQ * 10000d) / 10000d;

            // System.out.println(quantile[i] + " " + errorQuantile);

            System.out.println("T" + " size: " + tDigest.byteSize() + " quantile: " + String.format("%.4f", quantile[i]) + " error: " + String.format("%.4f", 100 * Math.abs(errorQuantileT - quantile[i])));
            System.out.println("Q" + " size: " + qDigest.estimatedSerializedSizeInBytes() + " quantile: " + String.format("%.4f", quantile[i]) + " %error: "
                    + String.format("%.4f", 100 * Math.abs(errorQuantileQ - quantile[i])));

            /*double tError = Math.abs(values.get((int) (quantile[i] * NUMBER_OF_ENTRIES)) - tDigest.quantile(quantile[i])) / values.get((int) (quantile[i] * NUMBER_OF_ENTRIES)) * 100;
            double qError = Math.abs(values.get((int) (quantile[i] * NUMBER_OF_ENTRIES)) - qDigest.getQuantile(quantile[i])) / values.get((int) (quantile[i] * NUMBER_OF_ENTRIES)) * 100;
            double error = Math.abs(values.get((int) (quantile[i] * NUMBER_OF_ENTRIES)) - normal.inverseCumulativeProbability(quantile[i])) / values.get((int) (quantile[i] * NUMBER_OF_ENTRIES)) * 100;
            //System.out.println(error);
            System.out.println("T" + " size: " + tDigest.byteSize() + " quantile: " + String.format("%.4f", quantile[i]) + " %error: " + String.format("%.15f", tError));
            System.out.println("Q" + " size: " + qDigest.estimatedSerializedSizeInBytes() + " quantile: " + String.format("%.4f", quantile[i]) + " %error: "
                    + String.format("%.15f", qError));
            */

            if (Math.abs(errorQuantileT - quantile[i]) < Math.abs(errorQuantileQ - quantile[i]) || Math.abs(errorQuantileT - quantile[i]) < STANDARD_ERROR) {
                success++;
            }
            total++;
        }
        System.out.println("correct:" + success + " total:" + total + " %:" + (double) success / (double) total * 100);
    }

    @Test(enabled = false)
    public void testBinomialDistribution()
    {
        int trials = 10;
        for (int k = 1; k < trials; k++) {
            TDigest tDigest = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
            BinomialDistribution binomial = new BinomialDistribution(trials, k * 0.1);
            List<Integer> list = new ArrayList<>();
            double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                    0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = binomial.sample();
                tDigest.add(sample);
                list.add(sample);
            }

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
            TDigest tDigest = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
            GeometricDistribution geometric = new GeometricDistribution(k * 0.1);
            List<Integer> list = new ArrayList<Integer>();
            double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                    0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = geometric.sample();
                tDigest.add(sample);
                list.add(sample);
            }

            Collections.sort(list);

            for (int i = 0; i < quantile.length; i++) {
                assertDiscreteWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
                //assertEquals(Math.rint(tDigest.quantile(quantile[i])), geometric.inverseCumulativeProbability(quantile[i]), MINIMUM_DISCRETE_ERROR);
            }
        }
    }

    @Test(enabled = true)
    public void testPoissonDistribution()
    {
        int trials = 10;
        for (int k = 1; k < trials; k++) {
            TDigest tDigest = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
            PoissonDistribution poisson = new PoissonDistribution(k * 0.1);
            List<Integer> list = new ArrayList<Integer>();
            double[] quantile = {0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000, 0.3000, 0.4000, 0.5000, 0.6000, 0.7000, 0.8000,
                    0.9000, 0.9500, 0.9600, 0.9700, 0.9800, 0.9999};

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                int sample = poisson.sample();
                tDigest.add(sample);
                list.add(sample);
            }

            Collections.sort(list);

            for (int i = 0; i < quantile.length; i++) {
                assertDiscreteWithinBound(quantile[i], STANDARD_ERROR, list, tDigest);
                //assertEquals(Math.rint(tDigest.quantile(quantile[i])), poisson.inverseCumulativeProbability(quantile[i]), MINIMUM_DISCRETE_ERROR);
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

        assertTrue(tDigest.quantile(quantile) >= lowerBound, String.format("Value %s is outside than lower bound %s", tDigest.quantile(quantile), lowerBound));
        assertTrue(tDigest.quantile(quantile) <= upperBound, String.format("Value %s is outside than upper bound %s", tDigest.quantile(quantile), upperBound));
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

        assertTrue(Math.rint(tDigest.quantile(quantile)) >= lowerBound, String.format("Value %s is outside than lower bound %s", tDigest.quantile(quantile), lowerBound));
        assertTrue(Math.rint(tDigest.quantile(quantile)) <= upperBound, String.format("Value %s is outside than upper bound %s", tDigest.quantile(quantile), upperBound));
    }
}
