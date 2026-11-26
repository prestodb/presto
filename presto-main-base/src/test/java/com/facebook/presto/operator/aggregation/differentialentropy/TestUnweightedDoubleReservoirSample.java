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
package com.facebook.presto.operator.aggregation.differentialentropy;

import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestUnweightedDoubleReservoirSample
{
    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Maximum number of samples must be positive: 0")
    public void testIllegalMaxSamples()
    {
        new UnweightedDoubleReservoirSample(0);
    }

    @Test
    public void testGetMaxSamples()
    {
        UnweightedDoubleReservoirSample reservoir = new UnweightedDoubleReservoirSample(200);

        assertEquals(reservoir.getMaxSamples(), 200);
        assertEquals(reservoir.getTotalPopulationCount(), 0);
    }

    @Test
    public void testFew()
    {
        UnweightedDoubleReservoirSample reservoir = new UnweightedDoubleReservoirSample(200);

        reservoir.add(1.0);
        reservoir.add(2.0);
        reservoir.add(3.0);

        assertEquals(Arrays.stream(reservoir.getSamples()).sorted().toArray(), new double[] {1.0, 2.0, 3.0});
        assertEquals(reservoir.getTotalPopulationCount(), 3);
    }

    @Test
    public void testMany()
    {
        UnweightedDoubleReservoirSample reservoir = new UnweightedDoubleReservoirSample(200);

        long streamLength = 1_000_000;
        for (int i = 0; i < streamLength; ++i) {
            assertEquals(reservoir.getTotalPopulationCount(), i);
            reservoir.add(i);
        }

        double[] quantized = new double[4];
        for (double sample : reservoir.getSamples()) {
            int index = (int) (4.0 * sample / (streamLength + 1));
            ++quantized[index];
        }
        int expectedMin = 25;
        for (int i = 0; i < 4; ++i) {
            assertTrue(quantized[i] > expectedMin, format("Expected quantized[i] > got: i=%s, quantized=%s, got=%s", i, quantized[i], expectedMin));
        }
    }

    @Test
    public void testMergeEqualPopulations()
    {
        // All 192 equally likely paths must give each of the six pairs 32 times.
        assertUniformMergedPairs(4, 32);
    }

    @Test
    public void testMergeUnequalPopulations()
    {
        // Include all three possible reservoirs from the right-hand population of three.
        // All 1200 equally likely paths must give each of the ten pairs 120 times.
        assertUniformMergedPairs(5, 120);
    }

    @Test(dataProvider = "partialReservoirPopulations")
    public void testMergePartialReservoirs(int leftCount, int rightCount)
    {
        UnweightedDoubleReservoirSample left = new UnweightedDoubleReservoirSample(2);
        UnweightedDoubleReservoirSample right = new UnweightedDoubleReservoirSample(2);
        for (int i = 0; i < leftCount; i++) {
            left.add(i);
        }
        for (int i = 0; i < rightCount; i++) {
            right.add(leftCount + i);
        }

        left.mergeWith(right);

        int population = leftCount + rightCount;
        double[] samples = left.getSamples();
        assertEquals(left.getTotalPopulationCount(), population);
        assertEquals(left.getMaxSamples(), 2);
        assertEquals(right.getTotalPopulationCount(), rightCount);
        assertEquals(samples.length, Math.min(2, population));
        assertEquals(Arrays.stream(samples).distinct().count(), samples.length);
        for (double sample : samples) {
            assertTrue(sample >= 0 && sample < population);
        }
    }

    @DataProvider
    public Object[][] partialReservoirPopulations()
    {
        return new Object[][] {
                {0, 0}, {0, 1}, {1, 0}, {1, 1},
                {0, 2}, {2, 0}, {1, 2}, {2, 1},
        };
    }

    private static void assertUniformMergedPairs(int population, int expectedCount)
    {
        int[][] counts = new int[population][population];
        // A common multiple of the old fixed bound and the correct second-draw bound
        // enumerates both algorithms with equal-probability paths, without randomness.
        int secondDrawRange = population * (population - 1);
        for (int firstRightSample = 2; firstRightSample < population; firstRightSample++) {
            for (int secondRightSample = firstRightSample + 1; secondRightSample < population; secondRightSample++) {
                for (int leftShuffle = 0; leftShuffle < 2; leftShuffle++) {
                    for (int rightShuffle = 0; rightShuffle < 2; rightShuffle++) {
                        for (int firstDraw = 0; firstDraw < population; firstDraw++) {
                            for (int secondDraw = 0; secondDraw < secondDrawRange; secondDraw++) {
                                UnweightedDoubleReservoirSample left = new UnweightedDoubleReservoirSample(2);
                                left.add(0);
                                left.add(1);
                                UnweightedDoubleReservoirSample right = reservoirWithPopulation(
                                        population - 2, firstRightSample, secondRightSample);
                                int[] shuffleChoices = {leftShuffle, rightShuffle};
                                long[] drawChoices = {firstDraw, secondDraw};
                                int[] shuffleIndex = {0};
                                int[] drawIndex = {0};
                                left.mergeWith(right,
                                        bound -> {
                                            assertEquals(bound, 2);
                                            return shuffleChoices[shuffleIndex[0]++];
                                        },
                                        bound -> {
                                            int index = drawIndex[0]++;
                                            if (index == 0) {
                                                assertEquals(bound, population);
                                            }
                                            else {
                                                assertEquals(secondDrawRange % bound, 0L);
                                            }
                                            return drawChoices[index] % bound;
                                        });

                                assertEquals(shuffleIndex[0], 2);
                                assertEquals(drawIndex[0], 2);
                                assertEquals(left.getTotalPopulationCount(), population);
                                assertEquals(right.getTotalPopulationCount(), population - 2);
                                double[] samples = left.getSamples();
                                assertEquals(samples.length, 2);
                                Arrays.sort(samples);
                                assertTrue(samples[0] < samples[1]);
                                counts[(int) samples[0]][(int) samples[1]]++;
                            }
                        }
                    }
                }
            }
        }
        for (int first = 0; first < population; first++) {
            for (int second = first + 1; second < population; second++) {
                assertEquals(counts[first][second], expectedCount, format("Pair {%s, %s}", first, second));
            }
        }
    }

    private static UnweightedDoubleReservoirSample reservoirWithPopulation(int population, double firstSample, double secondSample)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(2 * Integer.BYTES + 2 * Double.BYTES);
        output.appendInt(population).appendInt(2).appendDouble(firstSample).appendDouble(secondSample);
        return UnweightedDoubleReservoirSample.deserialize(output.slice().getInput());
    }
}
