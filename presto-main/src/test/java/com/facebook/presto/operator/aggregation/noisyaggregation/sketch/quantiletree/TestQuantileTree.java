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
package com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree;

import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.RandomizationStrategy;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.testing.SliceAssertions.assertSlicesEqual;
import static java.util.Collections.sort;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestQuantileTree
{
    @Test
    public void testBinningUnitInterval()
    {
        double lower = 0;
        double upper = 1;
        int totalBins = 4096;
        double binWidth = 1.0 / 4096.0;
        for (int branchingFactor : new int[] {2, 4, 8}) {
            QuantileTree tree = new QuantileTree(lower, upper, totalBins, branchingFactor, 1, 10_000);

            assertEquals(tree.getBinWidth(), binWidth);

            for (double value : new double[] {0, 0.1, 0.25, 0.5, 0.99}) {
                long bin = tree.toBin(value);
                assertEquals(bin, Math.floor(value * totalBins));
            }

            // the maximum bin is totalBins - 1
            assertEquals(tree.toBin(1), 4095);
        }
    }

    @Test
    public void testBinningOutOfBounds()
    {
        double lower = 10;
        double upper = 20;
        int totalBins = 1024;
        QuantileTree tree = new QuantileTree(lower, upper, totalBins, 2, 3, 10_000);

        assertEquals(tree.toBin(5), 0); // 5 is below rangeLowerBound, so it should be assigned minimum bin
        assertEquals(tree.toBin(500), 1023); // 500 is above rangeUpperBound, so it should be assigned maximum bin
    }

    @Test
    public void testBinningRoundTrip()
    {
        double lower = -5;
        double upper = 5;
        int totalBins = 1024;
        double binWidth = 10.0 / 1024.0;
        QuantileTree tree = new QuantileTree(lower, upper, totalBins, 2, 3, 10_000);

        for (double value : new double[] {-5, -4, -3.14, -2.78, 0, 1, 1.23, 2, 3, 4.5}) {
            // Going to bin and back should return a value within binWidth of the original value.
            double roundTrip = tree.fromBin(tree.toBin(value));
            assertTrue(roundTrip > value - binWidth && roundTrip < value + binWidth, "round-trip deviated too far from original value");
        }
    }

    @Test
    public void testBinCountRoundUpBinary()
    {
        double lower = 0;
        double upper = 1;
        int totalBins = 800; // not a power of 2!
        double requestedBinWidth = 1.0 / 800.0; // not what we'll get

        QuantileTree tree = new QuantileTree(lower, upper, totalBins, 2, 3, 10_000);

        // Bin width should be smaller than requested because we will use 1024 bins (next power of 2)
        assertTrue(tree.getBinWidth() < requestedBinWidth, "bin width should be smaller than requested");

        // All 1024 bins should be used to cover [0, 1], not the original 800 requested.
        // Consequently, the bin for 0.9 should be around 0.9 * 1024, not 0.9 * 800.
        assertEquals(tree.toBin(0.9), Math.floor(0.9 * 1024), "binning should map range to all bins");
    }

    @Test
    public void testBinCountRoundUpArbitraryBranching()
    {
        double lower = 0;
        double upper = 1;
        int totalBins = 70; // not a power of anything (except 70) :(

        int[] branchingFactors = new int[] {2, 3, 4, 5, 8};
        int[] expectedBinCounts = new int[] {128, 81, 256, 125, 512};

        for (int i = 0; i < branchingFactors.length; i++) {
            QuantileTree tree = new QuantileTree(lower, upper, totalBins, branchingFactors[i], 3, 10_000);
            assertEquals(tree.getBinWidth(), 1.0 / expectedBinCounts[i], 0.0001);
        }
    }

    @Test
    public void testBinCountEqualToBranchingFactor()
    {
        // By setting branchingFactor equal to bin count, you essentially obtain a linear histogram (root node + histogram)
        double lower = 0;
        double upper = 1;
        int totalBins = 700;
        QuantileTree tree = new QuantileTree(lower, upper, totalBins, totalBins, 3, 10_000);

        assertEquals(tree.totalLevels(), 2);
        assertEquals(tree.getBinWidth(), 1.0 / 700);
    }

    @Test
    public void testBranchingFactor()
    {
        int binCount = 4096; // 2^12 = 4^6 = 8^4 = 16^3
        double lower = 0;
        double upper = 1;
        int sketchDepth = 5;
        int sketchWidth = 300;

        QuantileTree tree2 = new QuantileTree(lower, upper, binCount, 2, sketchDepth, sketchWidth);
        assertEquals(tree2.getBranchingFactor(), 2);
        assertEquals(tree2.totalLevels(), 12 + 1);
        assertEquals(tree2.getBinWidth(), 1.0 / binCount);

        QuantileTree tree4 = new QuantileTree(lower, upper, binCount, 4, sketchDepth, sketchWidth);
        assertEquals(tree4.getBranchingFactor(), 4);
        assertEquals(tree4.totalLevels(), 6 + 1);
        assertEquals(tree4.getBinWidth(), 1.0 / binCount);

        QuantileTree tree8 = new QuantileTree(lower, upper, binCount, 8, sketchDepth, sketchWidth);
        assertEquals(tree8.getBranchingFactor(), 8);
        assertEquals(tree8.totalLevels(), 4 + 1);
        assertEquals(tree8.getBinWidth(), 1.0 / binCount);

        QuantileTree tree16 = new QuantileTree(lower, upper, binCount, 16, sketchDepth, sketchWidth);
        assertEquals(tree16.getBranchingFactor(), 16);
        assertEquals(tree16.totalLevels(), 3 + 1);
        assertEquals(tree16.getBinWidth(), 1.0 / binCount);
    }

    @Test
    public void testCardinality()
    {
        for (int branchingFactor : new int[] {2, 4, 8}) {
            QuantileTree tree = new QuantileTree(0, 10, 4096, branchingFactor, 1, 10_000);
            assertEquals(tree.cardinality(), 0);

            int n = 123;
            for (int i = 0; i < n; i++) {
                tree.add(i % 10); // add n values to the tree
            }

            // check non-noisy cardinality
            assertEquals(tree.cardinality(), n);

            // now add noise and check again
            tree.enablePrivacy(0.5, new TestQuantileTreeSeededRandomizationStrategy(1));
            assertEquals(tree.cardinality(), n, 10.0);
        }
    }

    @Test
    public void testQuantileTreeQueries()
    {
        double lower = -12.34;
        double upper = 12.34;
        double binWidth = 0.001;
        int sketchDepth = 5;
        int sketchWidth = 1000;
        int totalLeafNodes = (int) Math.round((upper - lower) / binWidth);
        RandomizationStrategy randomizationStrategy = new TestQuantileTreeSeededRandomizationStrategy(1);

        int n = 10_000;
        List<Double> numbers = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            double x = randomizationStrategy.nextGaussian();
            numbers.add(x);
        }

        sort(numbers);

        for (int branchingFactor : new int[] {2, 3, 4, 5}) {
            QuantileTree tree = new QuantileTree(lower, upper, totalLeafNodes, branchingFactor, sketchDepth, sketchWidth);
            for (double num : numbers) {
                tree.add(num);
            }
            tree.enablePrivacy(0.1, randomizationStrategy);

            double tolerance = 0.01;
            for (double p : new double[] {0.01, 0.05, 0.1, 0.5, 0.9, 0.95, 0.99}) {
                double lowerBound = p - tolerance;
                double upperBound = p + tolerance;

                if (lowerBound < 0) {
                    lowerBound = numbers.get(0);
                }
                else {
                    lowerBound = numbers.get((int) (n * lowerBound));
                }

                if (upperBound >= 1) {
                    upperBound = numbers.get(n - 1);
                }
                else {
                    upperBound = numbers.get((int) (n * upperBound));
                }

                double res = tree.quantile(p);
                assertTrue(res >= lowerBound && res <= upperBound, "estimated quantile is outside of tolerated range");
            }
        }
    }

    @Test
    public void testNonPrivateQuantileTreeQuery()
    {
        QuantileTree tree = new QuantileTree(0, 1024, 1024, 2, 1, 10_000);
        List<Integer> numbers = new ArrayList<>();
        int n = 100;
        for (int i = 1; i <= n; i++) {
            tree.add(i);
            numbers.add(i);
        }
        double quantile = 0.9;
        int x = numbers.get((int) (n * quantile));
        double res = tree.quantile(quantile);

        // Note: bins are discretized to [0, 1), [1, 2), ... [1023, 1024].
        // Since we return the midpoint of the bin containing the quantile, we'll be off by 0.5 here.
        assertEquals(res, x + 0.5);
    }

    @Test
    public void testNonPrivateQuantileTreeMerge()
    {
        double[] quantiles = {0.1, 0.5, 0.9};
        QuantileTree tree1 = new QuantileTree(0, 1024, 1024, 2, 5, 300);
        QuantileTree tree2 = new QuantileTree(0, 1024, 1024, 2, 5, 300);
        QuantileTree tree = new QuantileTree(0, 1024, 1024, 2, 5, 300);

        for (int i = 1; i <= 50; i++) {
            tree1.add(i);
            tree.add(i);
        }
        for (int i = 51; i <= 100; i++) {
            tree2.add(i);
            tree.add(i);
        }
        tree1.merge(tree2);
        assertFalse(tree1.isPrivacyEnabled());
        for (int i = 0; i < quantiles.length; i++) {
            assertEquals(tree1.quantile(quantiles[i]), tree.quantile(quantiles[i]));
        }
    }

    @Test
    public void testQuantileTreeMerge()
    {
        double lower = -10;
        double upper = 10;
        double binWidth = 0.001;
        int totalLeafNodes = (int) Math.round((upper - lower) / binWidth);
        double rho1 = 0.4;
        double rho2 = 0.5;
        RandomizationStrategy randomizationStrategy1 = new TestQuantileTreeSeededRandomizationStrategy(1);
        RandomizationStrategy randomizationStrategy2 = new TestQuantileTreeSeededRandomizationStrategy(2);
        QuantileTree tree1 = new QuantileTree(lower, upper, totalLeafNodes, 2, 5, 300);
        QuantileTree tree2 = new QuantileTree(lower, upper, totalLeafNodes, 2, 5, 300);
        tree1.enablePrivacy(rho1, randomizationStrategy1);
        tree2.enablePrivacy(rho2, randomizationStrategy2);

        List<Double> numbers = new ArrayList<>();

        int n = 10_000;
        for (int i = 1; i <= 5000; i++) {
            double x = randomizationStrategy1.nextGaussian();
            tree1.add(x);
            numbers.add(x);
        }
        for (int i = 5001; i <= n; i++) {
            double x = randomizationStrategy2.nextGaussian();
            tree2.add(x);
            numbers.add(x);
        }
        tree1.merge(tree2);
        sort(numbers);

        assertTrue(tree1.isPrivacyEnabled(), "tree should be marked private");
        assertTrue(tree1.getRho() < Math.min(rho1, rho2), "rho should be less than before merging");

        double tolerance = 0.01;
        for (double p : new double[] {0.05, 0.1, 0.5, 0.9, 0.95}) {
            double lowerBound = p - tolerance;
            double upperBound = p + tolerance;

            if (lowerBound < 0) {
                lowerBound = numbers.get(0);
            }
            else {
                lowerBound = numbers.get((int) (n * lowerBound));
            }

            if (upperBound >= 1) {
                upperBound = numbers.get(n - 1);
            }
            else {
                upperBound = numbers.get((int) (n * upperBound));
            }

            double res = tree1.quantile(p);
            assertTrue(res >= lowerBound && res <= upperBound, "estimated quantile is outside of tolerated range");
        }
    }

    @Test
    public void testMergedRho()
    {
        // merging x with NON_PRIVATE_RHO should return x
        assertEquals(QuantileTree.mergedRho(QuantileTree.NON_PRIVATE_RHO, 0.123), 0.123);
        assertEquals(QuantileTree.mergedRho(0.123, QuantileTree.NON_PRIVATE_RHO), 0.123);

        // merging rho should be symmetric
        assertEquals(QuantileTree.mergedRho(0.1, 0.2), QuantileTree.mergedRho(0.2, 0.1));

        // merging two finite values should return 1 / (1 / rho1 + 1 / rho2)
        // 1 / 0.5 = 2, 1 / 0.25 = 4
        assertEquals(QuantileTree.mergedRho(0.5, 0.25), 1.0 / 6, 1e-6);
        // 1 / 0.1 = 10
        assertEquals(QuantileTree.mergedRho(0.1, 0.1), 1.0 / 20, 1e-6);
    }

    @Test
    public void testFindRho2()
    {
        // findRho2 solves the mergedRho equation for a different variable, i.e.,
        // given rho1 and targetRho, findRho2 returns the rho2 value that would satisfy
        // mergedRho(rho1, rho2) = targetRho
        double[] rho1 = new double[]{0.5, 0.6, 0.7};
        double[] targetRho = new double[]{0.1, 0.2, 0.3};

        for (int i = 0; i < rho1.length; i++) {
            double rho2 = QuantileTree.findRho2(rho1[i], targetRho[i]);
            assertEquals(QuantileTree.mergedRho(rho1[i], rho2), targetRho[i], 1e-6);
        }
    }

    @Test
    public void testEnablePrivacy()
    {
        QuantileTree tree = new QuantileTree(0, 1024, 1024, 2, 5, 300);
        int n = 100;
        for (int i = 1; i <= n; i++) {
            tree.add(i);
        }

        assertFalse(tree.isPrivacyEnabled());

        double p = 0.5;
        double nonPrivateQuantile = tree.quantile(p);

        tree.enablePrivacy(0.3, new TestQuantileTreeSeededRandomizationStrategy(1));
        assertTrue(tree.isPrivacyEnabled());

        double privateQuantile = tree.quantile(p);

        assertNotEquals(nonPrivateQuantile, privateQuantile, 0.0, "quantile should be noisy after enabling privacy");
        assertEquals(privateQuantile, n * p, 5.0, "quantile should be around true value");
    }

    @Test
    public void testEnableAdditionalPrivacy()
    {
        RandomizationStrategy randomizationStrategy = new TestQuantileTreeSeededRandomizationStrategy(1);
        QuantileTree tree = new QuantileTree(0, 1024, 1024, 2, 5, 300);
        int n = 1000;
        for (int i = 1; i <= n; i++) {
            tree.add(i);
        }

        tree.enablePrivacy(0.8, randomizationStrategy);
        assertTrue(tree.isPrivacyEnabled());
        assertEquals(tree.getRho(), 0.8);
        double originalMedian = tree.quantile(0.5);
        double originalCardinality = tree.cardinality();

        // this should be a no-op
        tree.enablePrivacy(0.8, randomizationStrategy);
        assertTrue(tree.isPrivacyEnabled());
        assertEquals(tree.getRho(), 0.8);
        assertEquals(tree.quantile(0.5), originalMedian);
        assertEquals(tree.cardinality(), originalCardinality);

        // now we add a little more noise
        tree.enablePrivacy(0.6, randomizationStrategy);
        assertTrue(tree.isPrivacyEnabled());
        assertEquals(tree.getRho(), 0.6);
        assertNotEquals(tree.cardinality(), originalCardinality, "cardinality should no longer match exactly");
        assertEquals(tree.cardinality(), originalCardinality, 5.0, "cardinality should be close to original cardinality");
        assertEquals(tree.quantile(0.5), originalMedian, 5.0, "median should be close to original median");
    }

    @Test
    public void testRhoForEpsilonDelta()
    {
        // See math in N2273303
        assertEquals(QuantileTree.getRhoForEpsilonDelta(4, 0.001), 0.4548534, 0.00001);
        assertEquals(QuantileTree.getRhoForEpsilonDelta(4, 0.0001), 0.3596987, 0.00001);
        assertEquals(QuantileTree.getRhoForEpsilonDelta(4, 0.00001), 0.2976519, 0.00001);
        assertEquals(QuantileTree.getRhoForEpsilonDelta(1, 0.001), 0.0337869, 0.00001);
        assertEquals(QuantileTree.getRhoForEpsilonDelta(1, 0.0001), 0.0257628, 0.00001);
        assertEquals(QuantileTree.getRhoForEpsilonDelta(1, 0.00001), 0.0208199, 0.00001);
    }

    @Test
    public void testQuantileTreeSerializeRoundTrip()
    {
        double lower = -9.9;
        double upper = 9.9;
        double binWidth = 0.001;
        int totalLeafNodes = (int) Math.round((upper - lower) / binWidth);
        RandomizationStrategy randomizationStrategy = new TestQuantileTreeSeededRandomizationStrategy(1);

        // Build a tree
        QuantileTree tree = new QuantileTree(lower, upper, totalLeafNodes, 3, 5, 1000);
        tree.enablePrivacy(0.5, randomizationStrategy);
        int n = 10_000;
        for (int i = 0; i < n; i++) {
            double x = randomizationStrategy.nextGaussian();
            tree.add(x);
        }

        Slice serialized = tree.serialize();
        QuantileTree rebuildTree = QuantileTree.deserialize(serialized);

        assertSlicesEqual(serialized, rebuildTree.serialize());
        assertEquals(tree.quantile(0.5), rebuildTree.quantile(0.5), "quantile does not match in round-trip tree");
    }

    @Test
    public void testMemoryUsage()
    {
        // This tree will have a mix of histogram, sketched, and offset levels.
        QuantileTree tree = new QuantileTree(0, 10, 65536, 2, 3, 100);

        long expectedLevelsMemoryUsage = 0;
        for (int i = 0; i < tree.totalLevels(); i++) {
            expectedLevelsMemoryUsage += tree.getLevel(i).estimatedSizeInBytes();
        }
        long expectedMemoryUsage = ClassLayout.parseClass(QuantileTree.class).instanceSize() + SizeOf.sizeOfObjectArray(tree.totalLevels()) + expectedLevelsMemoryUsage;

        assertEquals(tree.estimatedSizeInBytes(), expectedMemoryUsage);
    }

    @Test
    public void testSerializedSize()
    {
        // This tree will have a mix of histogram, sketched, and offset levels.
        QuantileTree tree = new QuantileTree(0, 10, 65536, 2, 3, 100);

        // insert some values (doesn't really make an effect on size)
        for (int i = 0; i < 500; i++) {
            tree.add(i);
        }

        assertEquals(tree.estimatedSerializedSizeInBytes(), tree.serialize().length());
    }
}
