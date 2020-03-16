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
}
