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
package com.facebook.presto.operator.aggregation.reservoirsample;

import org.testng.annotations.Test;

import java.util.Arrays;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestWeightedDoubleReservoirSample
{
    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Maximum number of samples must be positive: 0")
    public void testIllegalMaxSamples()
    {
        new WeightedDoubleReservoirSample(0);
    }

    @Test
    public void testGetters()
    {
        WeightedDoubleReservoirSample reservoir = new WeightedDoubleReservoirSample(200);

        assertEquals(reservoir.getMaxSamples(), 200);
        assertEquals(reservoir.getTotalPopulationWeight(), 0.0);
    }

    @Test
    public void testFew()
    {
        WeightedDoubleReservoirSample reservoir = new WeightedDoubleReservoirSample(200);

        reservoir.add(1.0, 1.0);
        reservoir.add(2.0, 1.0);
        reservoir.add(3.0, 0.5);

        assertEquals(Arrays.stream(reservoir.getSamples()).sorted().toArray(), new double[] {1.0, 2.0, 3.0});
        assertEquals(reservoir.getTotalPopulationWeight(), 2.5);
    }

    @Test
    public void testMany()
    {
        WeightedDoubleReservoirSample reservoir = new WeightedDoubleReservoirSample(200);

        long streamLength = 1_000_000;
        for (int i = 0; i < streamLength; ++i) {
            assertEquals(reservoir.getTotalPopulationWeight(), i, 0.0001);
            reservoir.add(i, 1.0);
        }

        double[] quantized = new double[4];
        int count = 0;
        for (double sample : reservoir.getSamples()) {
            ++count;
            int index = (int) (4.0 * sample / (streamLength + 1));
            ++quantized[index];
        }
        assertEquals(count, 200, format("Number of samples should be full: got=%s, expected=%s", count, 200));
        int expectedMin = 25;
        for (int i = 0; i < 4; ++i) {
            assertTrue(quantized[i] > expectedMin, format("Expected quantized[i] > got: i=%s, quantized=%s, got=%s", i, quantized[i], 35));
        }
    }

    @Test
    public void testManyWeighted()
    {
        WeightedDoubleReservoirSample reservoir = new WeightedDoubleReservoirSample(200);

        long streamLength = 1_000_000;
        double epsilon = 0.00000001;
        for (int i = 0; i < streamLength; ++i) {
            assertEquals(reservoir.getTotalPopulationWeight(), epsilon * i, epsilon / 100);
            reservoir.add(3, epsilon);
        }
        for (int i = 0; i < streamLength; ++i) {
            reservoir.add(i, 9999999999.0);
        }

        double[] quantized = new double[4];
        int count = 0;
        for (double sample : reservoir.getSamples()) {
            ++count;
            int index = (int) (4.0 * sample / (streamLength + 1));
            ++quantized[index];
        }
        assertEquals(count, 200, format("Number of samples should be full: got=%s, expected=%s", count, 200));
        int expectedMin = 25;
        for (int i = 0; i < 4; ++i) {
            assertTrue(quantized[i] > expectedMin, format("Expected quantized[i] > got: i=%s, quantized=%s, got=%s", i, quantized[i], expectedMin));
        }
    }
}
