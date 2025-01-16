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
package com.facebook.presto.type.khyperloglog;

import io.airlift.slice.Slice;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static io.airlift.slice.testing.SliceAssertions.assertSlicesEqual;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKHyperLogLog
{
    @Test
    public void testCardinality()
            throws Exception
    {
        int trials = 1000;
        for (int indexBits = 4; indexBits <= 12; indexBits++) {
            Map<Integer, StandardDeviation> errors = new HashMap<>();
            int numberOfBuckets = 1 << indexBits;
            int maxCardinality = numberOfBuckets * 2;

            for (int trial = 0; trial < trials; trial++) {
                KHyperLogLog khll = new KHyperLogLog();
                for (int cardinality = 1; cardinality <= maxCardinality; cardinality++) {
                    khll.add(ThreadLocalRandom.current().nextLong(), 0L);

                    if (cardinality % (numberOfBuckets / 10) == 0) {
                        // only do this a few times, since computing the cardinality is currently not
                        // as cheap as it should be
                        double error = (khll.cardinality() - cardinality) * 1.0 / cardinality;

                        StandardDeviation stdev = errors.computeIfAbsent(cardinality, k -> new StandardDeviation());
                        stdev.increment(error);
                    }
                }
            }

            double expectedStandardError = 1.04 / Math.sqrt(1 << indexBits);

            for (Map.Entry<Integer, StandardDeviation> entry : errors.entrySet()) {
                // Give an extra error margin. This is mostly a sanity check to catch egregious errors
                double realStandardError = entry.getValue().getResult();
                assertTrue(realStandardError <= expectedStandardError * 1.1,
                        String.format("Failed at p = %s, cardinality = %s. Expected std error = %s, actual = %s",
                                indexBits,
                                entry.getKey(),
                                expectedStandardError,
                                realStandardError));
            }
        }
    }

    @Test
    public void testMerge()
            throws Exception
    {
        // small vs small
        verifyMerge(LongStream.rangeClosed(0, 100), LongStream.rangeClosed(50, 150));

        // small vs big
        verifyMerge(LongStream.rangeClosed(0, 100), LongStream.rangeClosed(50, 5000));

        // big vs small
        verifyMerge(LongStream.rangeClosed(50, 5000), LongStream.rangeClosed(0, 100));

        // big vs big
        verifyMerge(LongStream.rangeClosed(0, 5000), LongStream.rangeClosed(3000, 8000));
    }

    private void verifyMerge(LongStream one, LongStream two)
    {
        KHyperLogLog khll1 = new KHyperLogLog();
        KHyperLogLog khll2 = new KHyperLogLog();

        KHyperLogLog expected = new KHyperLogLog();

        long uii;
        for (long value : one.toArray()) {
            uii = randomLong(100);
            khll1.add(value, uii);
            expected.add(value, uii);
        }

        for (long value : two.toArray()) {
            uii = randomLong(100);
            khll2.add(value, uii);
            expected.add(value, uii);
        }

        KHyperLogLog merged = khll1.mergeWith(khll2);

        assertEquals(merged.cardinality(), expected.cardinality());
        assertEquals(merged.reidentificationPotential(10), expected.reidentificationPotential(10));
        assertSlicesEqual(khll1.serialize(), expected.serialize());
    }

    @Test
    public void testSerialization()
            throws Exception
    {
        // small
        verifySerialization(LongStream.rangeClosed(0, 1000));

        // large
        verifySerialization(LongStream.rangeClosed(0, 200000));
    }

    private void verifySerialization(LongStream sequence)
    {
        KHyperLogLog khll = new KHyperLogLog();

        for (Long value : sequence.toArray()) {
            khll.add(value, (long) (Math.random() * 100));
        }

        Slice serialized = khll.serialize();
        KHyperLogLog deserialized = KHyperLogLog.newInstance(serialized);

        assertEquals(khll.cardinality(), deserialized.cardinality());
        assertEquals(khll.reidentificationPotential(10), deserialized.reidentificationPotential(10));

        Slice reserialized = deserialized.serialize();
        assertSlicesEqual(serialized, reserialized);
    }

    @Test
    public void testHistogram()
            throws Exception
    {
        // small
        buildHistogramAndVerify(256, 1000);

        // large
        buildHistogramAndVerify(256, 200000);
    }

    public void buildHistogramAndVerify(int histogramSize, int count)
    {
        KHyperLogLog khll = new KHyperLogLog();
        Map<Long, HashSet<Long>> map = new HashMap<>();

        long uii;
        long value;
        for (int i = 0; i < count; i++) {
            uii = randomLong(histogramSize);
            value = randomLong(count);
            khll.add(value, uii);
            map.computeIfAbsent(value, k -> new HashSet<>()).add(uii);
        }

        int size = map.size();
        Map<Long, Double> realHistogram = new HashMap<>();
        for (HashSet<Long> uiis : map.values()) {
            long bucket = Math.min(uiis.size(), histogramSize);
            realHistogram.merge(bucket, (double) 1 / size, Double::sum);
        }
        Map<Long, Double> khllHistogram = khll.uniquenessDistribution(histogramSize);

        verifyUniquenessDistribution(realHistogram, khllHistogram);
        verifyReidentificationPotential(map, khll);
    }

    public void verifyUniquenessDistribution(Map<Long, Double> realHistogram, Map<Long, Double> khllHistogram)
    {
        double estimated = 0.0;
        double real = 0.0;
        int histogramSize = realHistogram.size();
        for (long i = 1; i < histogramSize; i++) {
            estimated += khllHistogram.get(i);
            real += realHistogram.getOrDefault(i, 0.0);
            assertTrue(Math.abs(estimated - real) <= 0.1 * real,
                    format("Expected histogram value %f +/- 10%%, got %f, for bucket %d", real, estimated, i));
        }
    }

    public void verifyReidentificationPotential(Map<Long, HashSet<Long>> map, KHyperLogLog khll)
    {
        double estimated;
        double real;
        int size = map.size();
        for (int threshold = 1; threshold < 10; threshold++) {
            estimated = khll.reidentificationPotential(threshold);
            real = 0.0;
            for (HashSet<Long> uiis : map.values()) {
                if (uiis.size() <= threshold) {
                    real++;
                }
            }
            real /= size;
            assertTrue(Math.abs(estimated - real) <= 0.1 * real,
                    format("Expected reidentification potential %f +/- 10%%, got %f, for set of size %d", real, estimated, size));
        }
    }

    private long randomLong(int range)
    {
        return (long) (Math.pow(Math.random(), 2.0) * range);
    }
}
