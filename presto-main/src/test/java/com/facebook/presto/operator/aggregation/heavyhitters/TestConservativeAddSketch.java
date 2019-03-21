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

import org.apache.commons.lang3.RandomStringUtils;


import org.testng.annotations.Test;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestConservativeAddSketch {

    @Test
    public void testAccuracy() {
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
            sketch.add(x, 1);
            baseSketch.add(x, 1);
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
            } else {
                continue;
            }
            long error = sketch.estimateCount(i) - actualFreq[i];
            totalError += error;
            if (error > okayError) {
                numErrors++;
            }
            long delta = baseSketch.estimateCount(i) - sketch.estimateCount(i);
            if (delta > 0) {
                totalDelta += delta;
                betterNumbers++;
            }
        }
        double pCorrect = 1 - 1.0 * numErrors / usedNumbers;
        System.out.println("Confidence : " + pCorrect + "   Errors : " + numErrors + "  Error margin : " + okayError);
        System.out.println("Total error : " + totalError + "  Average error : " + totalError / usedNumbers);
        System.out.println("Beat base for : " + 100 * betterNumbers / usedNumbers + " percent of values" +
                           " with a total delta of " + totalDelta);
        assertTrue(pCorrect > confidence,"Confidence not reached: required " + confidence + ", reached " + pCorrect );
    }

    @Test
    public void testAccuracyStrings() {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        String[] xs = new String[numItems];
        int maxScale = 20;
        for (int i = 0; i < xs.length; i++) {
            int scale = r.nextInt(maxScale);
            xs[i] = RandomStringUtils.random(scale);
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        ConservativeAddSketch sketch = new ConservativeAddSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch baseSketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        for (String x : xs) {
            sketch.add(x, 1);
            baseSketch.add(x, 1);
        }

        Map<String, Long> actualFreq = new HashMap<String, Long>(numItems / 10);
        for (String x : xs) {
            Long val = actualFreq.get(x);
            if (val == null) {
                actualFreq.put(x, 1L);
            } else {
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
            long error = sketch.estimateCount(key) - value;
            totalError += error;
            if (error > okayError) {
                numErrors++;
            }
            long delta = baseSketch.estimateCount(key) - sketch.estimateCount(key);
            if (delta > 0) {
                totalDelta += delta;
                betterNumbers++;
            }
        }
        long usedValues = actualFreq.size();
        double pCorrect = 1 - 1.0 * numErrors / usedValues;
        System.out.println("Confidence : " + pCorrect + "   Errors : " + numErrors + "  Error margin : " + okayError);
        System.out.println("Total error : " + totalError + "  Average error : " + totalError / usedValues);
        System.out.println("Beat base for : " + 100 * betterNumbers / usedValues + " percent of values" +
                           " with a total delta of " + totalDelta);
        assertTrue(pCorrect > confidence,"Confidence not reached: required " + confidence + ", reached " + pCorrect );
    }

    /**
     * The merging guarantees are a little different for us. Sometimes it is
     * better to split and merge and sometimes not. As long as we are more
     * accurate than the regular version though, I am happy.
     */
    @Test
    public void merge() throws CountMinSketch.CMSMergeException {
        int numToMerge = 5;
        int cardinality = 1000000;

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;
        int seed = 7364181;

        int maxScale = 20;
        Random r = new Random();
        TreeSet<Integer> vals = new TreeSet<Integer>();

        CountMinSketch baseline = new ConservativeAddSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch stdMinSketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch[] sketchs = new CountMinSketch[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            sketchs[i] = new ConservativeAddSketch(epsOfTotalCount, confidence, seed);
            for (int j = 0; j < cardinality; j++) {
                int scale = r.nextInt(maxScale);
                int val = r.nextInt(1 << scale);
                if (vals.add(val)) {
                    sketchs[i].add(val, 1);
                    baseline.add(val, 1);
                    stdMinSketch.add(val, 1);
                }
            }
        }

        CountMinSketch merged = sketchs[0].merge(Arrays.copyOfRange(sketchs, 1, sketchs.length));

        assertEquals(baseline.size(), merged.size());
        assertEquals(baseline.getConfidence(), merged.getConfidence(), baseline.getConfidence() / 100);
        assertEquals(baseline.getRelativeError(), merged.getRelativeError(), baseline.getRelativeError() / 100);
        for (int val : vals) {
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
