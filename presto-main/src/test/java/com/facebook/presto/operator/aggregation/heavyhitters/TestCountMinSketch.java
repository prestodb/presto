/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.operator.aggregation.heavyhitters;

import java.util.*;

import org.testng.annotations.Test;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.*;
import static io.airlift.testing.Assertions.assertInstanceOf;

import org.apache.commons.lang3.RandomStringUtils;

import com.facebook.presto.operator.aggregation.heavyhitters.CountMinSketch.CMSMergeException;

public class TestCountMinSketch {

//    @Test
//    public void negativeSize() {
//        try {
//            new CountMinSketch(20, 4, -1, new long[]{1}, new long[][]{{10, 20}, {100, 200}});
//        }catch(Exception e){
//            assertInstanceOf(e, IllegalArgumentException.class);
//        }
//    }

    @Test
    public void sizeOverflow() {
        try {
            CountMinSketch sketch = new CountMinSketch(0.0001, 0.99999, 1);
            sketch.add(3, Long.MAX_VALUE);
            sketch.add(4, 1);
        }catch(Exception e){
            assertInstanceOf(e, IllegalStateException.class);
        }
    }

    @Test
    public void testSize() throws CMSMergeException {
        CountMinSketch sketch = new CountMinSketch(0.00001, 0.99999, 1);
        assertEquals(0, sketch.size(), 0);

        sketch.add(1, 11);
        sketch.add(2, 22);
        sketch.add(3, 33);

        long expectedSize = 11 + 22 + 33;
        assertEquals(expectedSize, sketch.size());
    }

    @Test
    public void testSizeCanStoreLong() throws CMSMergeException {
        double confidence = 0.999;
        double epsilon = 0.0001;
        int seed = 1;

        CountMinSketch sketch = new CountMinSketch(epsilon, confidence, seed);

        long freq1 = Integer.MAX_VALUE;
        long freq2 = 156;

        sketch.add(1, freq1);
        sketch.add(2, freq2);

        CountMinSketch newSketch = sketch.merge(sketch);

        long expectedSize = 2 * (freq1 + freq2);
        assertEquals(expectedSize, newSketch.size());
    }

    @Test
    public void testAccuracy() {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        int[] xs = new int[numItems];
        int maxScale = 20;
        for (int i = 0; i < numItems; i++) {
            int scale = r.nextInt(maxScale);
            xs[i] = r.nextInt(1 << scale);
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        for (int x : xs) {
            sketch.add(x, 1);
        }

        int[] actualFreq = new int[1 << maxScale];
        for (int x : xs) {
            actualFreq[x]++;
        }

        sketch = new CountMinSketch(sketch.serialize());

        int numErrors = 0;
        for (int i = 0; i < actualFreq.length; ++i) {
            double ratio = ((double) (sketch.estimateCount(i) - actualFreq[i])) / numItems;
            if (ratio > epsOfTotalCount) {
                numErrors++;
            }
        }
        double pCorrect = 1.0 - ((double) numErrors) / actualFreq.length;
        assertTrue(pCorrect > confidence,"Confidence not reached: required " + confidence + ", reached " + pCorrect);
    }

    @Test
    public void testAccuracyStrings() {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        int absentItems = numItems * 10;
        String[] xs = new String[numItems];
        int maxScale = 20;
        for (int i = 0; i < numItems; i++) {
            int scale = r.nextInt(maxScale);
            xs[i] = RandomStringUtils.random(scale);
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        for (String x : xs) {
            sketch.add(x, 1);
        }

        Map<String, Long> actualFreq = new HashMap<String, Long>();
        for (String x : xs) {
            Long val = actualFreq.get(x);
            if (val == null) {
                actualFreq.put(x, 1L);
            } else {
                actualFreq.put(x, val + 1L);
            }
        }

        sketch = new CountMinSketch(sketch.serialize());

        int numErrors = 0;
        for (Map.Entry<String,Long> entry : actualFreq.entrySet()) {
            String key = entry.getKey();
            long count = entry.getValue();
            double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
            if (ratio > epsOfTotalCount) {
                numErrors++;
            }
        }
        for (int i = 0; i < absentItems; i++) {
            int scale = r.nextInt(maxScale);
            String key = RandomStringUtils.random(scale);
            Long value = actualFreq.get(key);
            long count = (value == null) ? 0L : value;
            double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
            if (ratio > epsOfTotalCount) {
                numErrors++;
            }
        }

        double pCorrect = 1.0 - ((double) numErrors) / (numItems + absentItems);
        System.out.println(pCorrect);
        assertTrue(pCorrect > confidence,"Confidence not reached: required " + confidence + ", reached " + pCorrect);

        assertTrue(pCorrect > confidence, "Confidence not reached: required " + confidence + ", reached " + pCorrect);
    }


    @Test
    public void merge() throws CMSMergeException {
        int numToMerge = 5;
        int cardinality = 1000000;

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;
        int seed = 7364181;

        int maxScale = 20;
        Random r = new Random();
        TreeSet<Integer> vals = new TreeSet<Integer>();

        CountMinSketch baseline = new CountMinSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch[] sketchs = new CountMinSketch[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            sketchs[i] = new CountMinSketch(epsOfTotalCount, confidence, seed);
            for (int j = 0; j < cardinality; j++) {
                int scale = r.nextInt(maxScale);
                int val = r.nextInt(1 << scale);
                vals.add(val);
                sketchs[i].add(val, 1);
                baseline.add(val, 1);
            }
        }

        CountMinSketch merged = sketchs[0].merge(Arrays.copyOfRange(sketchs, 1, sketchs.length));

        assertEquals(baseline.size(), merged.size());
        assertEquals(baseline.getConfidence(), merged.getConfidence(), baseline.getConfidence() / 100);
        assertEquals(baseline.getRelativeError(), merged.getRelativeError(), baseline.getRelativeError() / 100);
        for (int val : vals) {
            assertEquals(baseline.estimateCount(val), merged.estimateCount(val));
        }
    }

//    @Test
//    public void testMergeEmpty() throws CMSMergeException {
//        assertNull(CountMinSketch.merge());
//    }

    @Test
    public void testUncompatibleMerge() throws CMSMergeException {
        CountMinSketch cms1 = new CountMinSketch(0.01, 0.99, 0);
        CountMinSketch cms2 = new CountMinSketch(0.1, 0.9, 0);
        try{
            cms1.merge(cms2);
        }catch(Exception e){
            assertInstanceOf(e, CMSMergeException.class);
        }
    }

    //TODO add test for serialize/deserialize
//    private static void checkCountMinSketchSerialization(CountMinSketch cms) throws IOException, ClassNotFoundException {
//        byte[] bytes = TestUtils.serialize(cms);
//        CountMinSketch serializedCms = (CountMinSketch)TestUtils.deserialize(bytes);
//
//        assertEquals(cms, serializedCms);
//    }

//    @Test
//    public void testSerializationForDepthCms() throws IOException, ClassNotFoundException {
//        checkCountMinSketchSerialization(new CountMinSketch(12, 2045, 1));
//    }
//
//    @Test
//    public void testSerializationForConfidenceCms() throws IOException, ClassNotFoundException {
//        checkCountMinSketchSerialization(new CountMinSketch(0.0001, 0.99999999999, 1));
//    }

    @Test
    public void testEquals() {
        double eps1 = 0.0001;
        double eps2 = 0.000001;
        double confidence = 0.99;
        int seed = 1;

        final CountMinSketch sketch1 = new CountMinSketch(eps1, confidence, seed);
        assertEquals(sketch1, sketch1);

        final CountMinSketch sketch2 = new CountMinSketch(eps1, confidence, seed);
        assertEquals(sketch1, sketch2);

        final CountMinSketch sketch3 = new ConservativeAddSketch(eps1, confidence, seed);
        assertNotEquals(sketch1, sketch3);

        assertNotEquals(sketch1, null);

        sketch1.add(1, 123);
        sketch2.add(1, 123);
        assertEquals(sketch1, sketch2);

        sketch1.add(1, 4);
        assertNotEquals(sketch1, sketch2);

        final CountMinSketch sketch4 = new CountMinSketch(eps1, confidence, seed);
        final CountMinSketch sketch5 = new CountMinSketch(eps2, confidence, seed);
        assertNotEquals(sketch4, sketch5);

        sketch3.add(1, 7);
        sketch4.add(1, 7);
        assertNotEquals(sketch4, sketch5);
    }

    @Test
    public void testToString() {
        double eps = 0.0001;
        double confidence = 0.99;
        int seed = 1;

        final CountMinSketch sketch = new CountMinSketch(eps, confidence, seed);
        assertEquals("CountMinSketch{" +
                "eps=" + eps +
                ", confidence=" + confidence +
                ", depth=" + 5 +
                ", width=" + 27183 +
                ", size=" + 0 +
                '}', sketch.toString());

        sketch.add(12, 145);
        assertEquals("CountMinSketch{" +
                "eps=" + eps +
                ", confidence=" + confidence +
                ", depth=" + 5 +
                ", width=" + 27183 +
                ", size=" + 145 +
                '}', sketch.toString());
    }
}
