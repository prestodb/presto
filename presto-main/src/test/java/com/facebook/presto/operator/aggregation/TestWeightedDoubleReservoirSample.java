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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.reservoirsample.WeightedDoubleReservoirSample;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestWeightedDoubleReservoirSample
{
    @Test
    public void testGetters()
    {
        final WeightedDoubleReservoirSample sample =
                new WeightedDoubleReservoirSample(200);

        assertEquals(sample.getMaxSamples(), 200);
    }

    @Test
    public void testIllegalBucketCount()
    {
        try {
            new WeightedDoubleReservoirSample(0);
            fail("exception expected");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("positive"));
        }
    }

    @Test
    public void testFew()
    {
        final WeightedDoubleReservoirSample reservoir =
                new WeightedDoubleReservoirSample(200);

        reservoir.add(1.0, 1.0);
        reservoir.add(2.0, 1.0);
        reservoir.add(3.0, 0.5);

        Set<Double> samples = new HashSet<>();
        reservoir.stream().forEach(o -> samples.add(Double.valueOf(o)));
        assertEquals(samples.size(), 3);
        assertTrue(samples.contains(Double.valueOf(1)));
        assertTrue(samples.contains(Double.valueOf(2)));
        assertTrue(samples.contains(Double.valueOf(3)));
    }

    @Test
    public void testMany()
    {
        final WeightedDoubleReservoirSample reservoir =
                new WeightedDoubleReservoirSample(200);

        long streamLength = 1000000;
        for (int i = 0; i < streamLength; ++i) {
            double value = i * 10 + i % 2;
            reservoir.add(value, 1.0);
        }

        double quantized[] = new double[4];
        reservoir.stream().forEach(s -> {
            int index = (int) (4.0 * s / (10.0 * streamLength + 1));
            ++quantized[index];
        });
        for (int i = 0; i < 4; ++i) {
            assertTrue(quantized[i] > 35);
        }
    }

    @Test
    public void testManyWeighted()
    {
        final WeightedDoubleReservoirSample reservoir =
                new WeightedDoubleReservoirSample(200);

        long streamLength = 1000000;
        for (int i = 0; i < streamLength; ++i) {
            reservoir.add(3, 0.00000001);
        }
        for (int i = 0; i < streamLength; ++i) {
            double value = i * 10 + i % 2;
            reservoir.add(value, 9999999999.0);
        }

        double quantized[] = new double[4];
        reservoir.stream().forEach(s -> {
            int index = (int) (4.0 * s / (10.0 * streamLength + 1));
            ++quantized[index];
        });
        for (int i = 0; i < 4; ++i) {
            assertTrue(quantized[i] > 35);
        }
    }
}
