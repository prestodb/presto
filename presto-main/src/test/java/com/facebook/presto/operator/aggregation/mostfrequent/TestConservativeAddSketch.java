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

package com.facebook.presto.operator.aggregation.mostfrequent;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestConservativeAddSketch
{
    @Test
    public void testAccuracy()
    {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 10000000;
        int maxScale = 15000;
        double epsOfTotalCount = 0.00075;
        double errorRange = epsOfTotalCount;
        double confidence = 0.99;

        int[] actualFreq = new int[maxScale];
        ConservativeAddSketch sketch = new ConservativeAddSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch baseSketch = new CountMinSketch(epsOfTotalCount, confidence, seed);

        for (int i = 0; i < numItems; i++) {
            int x = r.nextInt(maxScale);
            Slice xAsSlice = Slices.wrappedIntArray(x);
            sketch.add(xAsSlice, 1);
            baseSketch.add(xAsSlice, 1);
            actualFreq[x]++;
        }

        int numErrors = 0;
        int usedNumbers = 0;
        int betterNumbers = 0;
        long totalDelta = 0;
        int okayError = (int) (numItems * errorRange) + 1;
        long totalError = 0;
        for (int i = 0; i < actualFreq.length; i++) {
            if (actualFreq[i] > 0) {
                usedNumbers++;
            }
            else {
                continue;
            }
            Slice iAsSlice = Slices.wrappedIntArray(i);
            long error = sketch.estimateCount(iAsSlice) - actualFreq[i];
            totalError += error;
            if (error > okayError) {
                numErrors++;
            }
            long delta = baseSketch.estimateCount(iAsSlice) - sketch.estimateCount(iAsSlice);
            if (delta > 0) {
                totalDelta += delta;
                betterNumbers++;
            }
        }
        double pCorrect = 1 - 1.0 * numErrors / usedNumbers;
        assertTrue(pCorrect > confidence, "Confidence not reached: required " + confidence + ", reached " + pCorrect);
    }

    @Test
    public void testAccuracyStrings()
    {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        String[] xs = new String[numItems];
        int maxScale = 20;
        for (int i = 0; i < xs.length; i++) {
            int scale = r.nextInt(maxScale);
            xs[i] = UUID.randomUUID().toString().substring(10, 10 + scale);
        }

        double epsOfTotalCount = 0.00001;
        double confidence = 0.99;

        ConservativeAddSketch sketch = new ConservativeAddSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch baseSketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        for (String x : xs) {
            Slice xAsSlice = Slices.wrappedBuffer(x.getBytes());
            sketch.add(xAsSlice, 1);
            baseSketch.add(xAsSlice, 1);
        }

        Map<String, Long> actualFreq = new HashMap<String, Long>(numItems / 10);
        for (String x : xs) {
            Long val = actualFreq.get(x);
            if (val == null) {
                actualFreq.put(x, 1L);
            }
            else {
                actualFreq.put(x, val + 1L);
            }
        }

        int numErrors = 0;
        int betterNumbers = 0;
        long totalDelta = 0;
        int okayError = (int) (numItems * epsOfTotalCount) + 1;
        long totalError = 0;
        for (Map.Entry<String, Long> entry : actualFreq.entrySet()) {
            String key = entry.getKey();
            Long value = entry.getValue();
            Slice keyAsSlice = Slices.wrappedBuffer(key.getBytes());
            long error = sketch.estimateCount(keyAsSlice) - value;
            totalError += error;
            if (error > okayError) {
                numErrors++;
            }
            long delta = baseSketch.estimateCount(keyAsSlice) - sketch.estimateCount(keyAsSlice);
            if (delta > 0) {
                totalDelta += delta;
                betterNumbers++;
            }
        }
        long usedValues = actualFreq.size();
        double pCorrect = 1 - 1.0 * numErrors / usedValues;
        assertTrue(pCorrect > confidence, "Confidence not reached: required " + confidence + ", reached " + pCorrect);
    }

    /**
     * The merging guarantees are a little different for us. Sometimes it is
     * better to split and merge and sometimes not. As long as we are more
     * accurate than the regular version though, I am happy.
     */
    @Test
    public void merge()
            throws RuntimeException
    {
        int numToMerge = 5;
        int cardinality = 1000000;

        double epsOfTotalCount = 0.00001;
        double confidence = 0.99;
        int seed = 7364181;

        int maxScale = 20;
        Random r = new Random();
        TreeSet<Slice> vals = new TreeSet<>();

        CountMinSketch baseline = new ConservativeAddSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch stdMinSketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch[] sketchs = new CountMinSketch[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            sketchs[i] = new ConservativeAddSketch(epsOfTotalCount, confidence, seed);
            for (int j = 0; j < cardinality; j++) {
                int scale = r.nextInt(maxScale);
                int val = r.nextInt(1 << scale);
                Slice valAsSlice = Slices.wrappedIntArray(val);
                if (vals.add(valAsSlice)) {
                    sketchs[i].add(valAsSlice, 1);
                    baseline.add(valAsSlice, 1);
                    stdMinSketch.add(valAsSlice, 1);
                }
            }
        }

        CountMinSketch merged = sketchs[0].merge(Arrays.copyOfRange(sketchs, 1, sketchs.length));

        assertEquals(baseline.size(), merged.size());
        assertEquals(baseline.getConfidence(), merged.getConfidence(), baseline.getConfidence() / 100);
        assertEquals(baseline.getRelativeError(), merged.getRelativeError(), baseline.getRelativeError() / 100);
        for (Slice val : vals) {
            long base = baseline.estimateCount(val);
            long merge = merged.estimateCount(val);
            long std = stdMinSketch.estimateCount(val);
            assertTrue(1 <= base);
            assertTrue(base <= std);
            assertTrue(1 <= merge);
            assertTrue(merge <= std);
        }
    }
}
