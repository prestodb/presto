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

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestQuantileTreeCountSketch
{
    @Test
    public void testEstimatedSerializedSize()
    {
        CountSketch sketch = new CountSketch(5, 272);
        int res = sketch.estimatedSerializedSizeInBytes();
        assertEquals(sketch.getWidth(), 272); // check the table's width
        assertEquals(sketch.getDepth(), 5);  // check the table's depth
        assertEquals(res, 5448);
    }

    @Test
    public void testMemoryUsage()
    {
        float[][] expectedTable = new float[5][272];
        CountSketch sketch = new CountSketch(5, 272);

        long expectedMemoryUsage = ClassLayout.parseClass(CountSketch.class).instanceSize() +
                SizeOf.sizeOf(expectedTable) + 5 * SizeOf.sizeOfFloatArray(272);

        assertEquals(sketch.getWidth(), 272); // check the table's width
        assertEquals(sketch.getDepth(), 5); // check the table's depth
        assertEquals(sketch.estimatedSizeInBytes(), expectedMemoryUsage);
    }

    @Test
    public void testSerializeAndDeserialize()
    {
        CountSketch sketch = new CountSketch(5, 272);

        sketch.addNoise(0.5, 5, new TestQuantileTreeSeededRandomizationStrategy(1));
        for (int i = 0; i < 100; i++) {
            sketch.add(1);
        }

        // take a few random noisy estimates from our sketch
        double[] counts = new double[]{sketch.estimateCount(1), sketch.estimateCount(2), sketch.estimateCount(3)};

        Slice sketchSlice = sketch.serialize();
        SliceInput sliceInput = new BasicSliceInput(sketchSlice);
        CountSketch deserializedSketch = new CountSketch(sliceInput);
        assertEquals(deserializedSketch.getDepth(), sketch.getDepth());
        assertEquals(deserializedSketch.getWidth(), sketch.getWidth());

        // take the same estimates from the deserialized sketch
        double[] deserializedCounts = new double[]{deserializedSketch.estimateCount(1), deserializedSketch.estimateCount(2), deserializedSketch.estimateCount(3)};
        assertEquals(deserializedCounts, counts);
    }

    @Test
    public void testCountsSparse()
    {
        CountSketch sketch = new CountSketch(5, 272);

        // if we only add a few items to the sketch, we should be able to return those without error
        sketch.add(123, 50);
        sketch.add(1234, 200);
        sketch.add(999, 1000);

        // these should match what we inserted (approximately)
        assertEquals(sketch.estimateCount(123), 50);
        assertEquals(sketch.estimateCount(1234), 200);
        assertEquals(sketch.estimateCount(999), 1000);

        // this was never inserted
        assertEquals(sketch.estimateCount(7), 0);
    }

    @Test
    public void testCountsSaturated()
    {
        CountSketch sketch = new CountSketch(5, 272);

        // first, we add many small counts over the range 1xxxx
        addCountsToSketch(sketch, 10000, 19999, 10);

        // then we add some special counts that we really care about
        sketch.add(123, 500);
        sketch.add(1234, 2000);
        sketch.add(999, 10000);

        // We've now added roughly 10000 * 5.5 + 500 + 2000 + 10000 = 67,500 items to the sketch (10,003 unique items)
        // Our estimates will be approximate this time.
        double tolerance = 50;
        assertEquals(sketch.estimateCount(123), 500, tolerance);
        assertEquals(sketch.estimateCount(1234), 2000, tolerance);
        assertEquals(sketch.estimateCount(999), 10000, tolerance);
    }

    @Test
    public void testCountsPrivate()
    {
        CountSketch sketch = new CountSketch(5, 272);

        // we only add a few items to the sketch
        sketch.add(123, 50);
        sketch.add(1234, 200);
        sketch.add(999, 1000);

        // but then we add noise to the sketch
        sketch.addNoise(0.5, 1, new TestQuantileTreeSeededRandomizationStrategy(1));

        // these should match what we inserted (approximately)
        double tolerance = 10;
        assertEquals(sketch.estimateCount(123), 50, tolerance);
        assertEquals(sketch.estimateCount(1234), 200, tolerance);
        assertEquals(sketch.estimateCount(999), 1000, tolerance);

        // this was never inserted
        assertEquals(sketch.estimateCount(7), 0, tolerance);
    }

    @Test
    public void testHashingUniformity()
    {
        // This is an impractically small sketch, but the point is to test the uniformity of getBucket() and getSign() on the iterated hashes
        int width = 12;
        CountSketch sketch = new CountSketch(1, width);

        int n = 1000;
        long[] testItems = new long[]{-1, 0, 1, 2, 3, 4, 1234, 987654321, Long.MAX_VALUE};
        for (long item : testItems) {
            long baseHash = CountSketch.getBaseHash(item);

            // try n hashes, compute distribution of buckets and signs
            int[] bucketCounts = new int[width];
            int positiveSigns = 0;
            for (int i = 0; i < n; i++) {
                long iteratedHash = sketch.getIteratedHash(baseHash, i);
                bucketCounts[CountSketch.getBucket(iteratedHash)]++;
                if (CountSketch.getSign(iteratedHash) > 0) {
                    positiveSigns++;
                }
            }

            // sign should be positive about half the time
            assertEquals((float) positiveSigns / n, 0.5, 0.02);

            // each bucket should get about equal allocation (1 / width)
            for (int i = 0; i < width; i++) {
                assertEquals((float) bucketCounts[i] / n, 1.0 / width, 0.02);
            }
        }
    }

    /**
     * Deterministically fill sketch with counts covering a range of keys from lowerKey to upperKey, up to a given maximum count.
     * The counts are a simple cyclic sequence from 1 to maximum.
     */
    private static void addCountsToSketch(CountSketch sketch, int lowerKey, int upperKey, int maximum)
    {
        for (int i = lowerKey; i <= upperKey; i++) {
            sketch.add(i, 1 + i % maximum);
        }
    }
}
