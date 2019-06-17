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
import com.google.common.collect.Streams;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestWeightedLongReservoirSample
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
        final WeightedDoubleReservoirSample sample =
                new WeightedDoubleReservoirSample(200);

        sample.add((long) 1, 1.0);
        sample.add((long) 2, 1.0);
        sample.add((long) 3, 0.5);

        Set<Long> samples = new HashSet<Long>();
        Streams.stream(sample.iterator()).forEach(o -> samples.add((Long) o));
        assertEquals(samples.size(), 3);
        assertTrue(samples.contains(Long.valueOf(1)));
        assertTrue(samples.contains(Long.valueOf(2)));
        assertTrue(samples.contains(Long.valueOf(3)));
    }

    @Test
    public void testMany()
    {
        final WeightedDoubleReservoirSample sample =
                new WeightedDoubleReservoirSample(200);

        long streamLength = 1000000;
        for (long i = 0; i < streamLength; ++i) {
            long value = i * 10 + i % 2;
            sample.add(value, 1.0);
        }

        Set<Long> sampled = new HashSet<Long>();
        Streams.stream(sample.iterator()).forEach(o -> sampled.add((Long) o));
        System.out.println("res " + Arrays.toString(sampled.toArray()));
    }
}
