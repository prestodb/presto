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

package com.facebook.presto.operator.aggregation.mostfrequent.state;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static com.facebook.airlift.testing.Assertions.assertContains;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.testing.Assertions.assertLessThan;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestTopElementsHistogram
{
    private static final Slice A = Slices.utf8Slice("a");
    private static final Slice B = Slices.utf8Slice("b\u00a2");
    private static final Slice C = Slices.utf8Slice("\u0472c");

    public static void populateHistogram(TopElementsHistogram histogram)
    {
        long i = 5;
        for (char c = 'a'; c <= 'v'; c++) {
            histogram.add(Slices.utf8Slice(String.valueOf(c)), i);
            i += 10;
        }
    }

    @Test
    public void testMinPctShare5NonCcms()
    {
        TopElementsHistogram histogram = new TopElementsHistogram(5, 0.01, 0.99, 1);
        populateHistogram(histogram);
        Set<Slice> expected = new HashSet<>();
        for (char c = 'm'; c <= 'v'; c++) {
            expected.add(Slices.utf8Slice(String.valueOf(c)));
        }
        assertEquals(histogram.getTopElements().keySet(), expected);
    }

    @Test
    public void testMinPctShare7NonCcms()
    {
        TopElementsHistogram histogram = new TopElementsHistogram(7, 0.01, 0.99, 1);
        populateHistogram(histogram);
        Map<Slice, Long> expected = new HashMap<>();
        long i = 175L;
        for (char c = 'r'; c <= 'v'; c++) {
            expected.put(Slices.utf8Slice(String.valueOf(c)), i);
            i += 10;
        }
        assertEquals(histogram.getTopElements(), expected);
    }

    @Test
    public void testMinPctShare5WithCcms()
    {
        TopElementsHistogram histogram = new TopElementsHistogram(5, 0.01, 0.99, 1);
        histogram.switchToSketch();
        populateHistogram(histogram);
        Set<Slice> expected = new HashSet<>();
        for (char c = 'm'; c <= 'v'; c++) {
            expected.add(Slices.utf8Slice(String.valueOf(c)));
        }
        assertEquals(histogram.getTopElements().keySet(), expected);
    }

    @Test
    public void testMinPctShare7WithCcms()
    {
        TopElementsHistogram histogram = new TopElementsHistogram(7, 0.01, 0.99, 1);
        histogram.switchToSketch();
        populateHistogram(histogram);
        Map<Slice, Long> expected = new HashMap<>();
        long i = 175L;
        for (char c = 'r'; c <= 'v'; c++) {
            expected.put(Slices.utf8Slice(String.valueOf(c)), i);
            i += 10;
        }
        assertEquals(histogram.getTopElements(), expected);
    }

    @Test
    public void testNull()
    {
        TopElementsHistogram histogram = new TopElementsHistogram(40, 0.01, 0.99, 1);
        histogram.add(Arrays.asList(null, Slices.utf8Slice("b"), null, Slices.utf8Slice("a"), null, Slices.utf8Slice("b")));
        assertEquals(histogram.getTopElements(), ImmutableMap.of(Slices.utf8Slice("b"), 2L));
    }

    @Test
    public void testSimple()
    {
        TopElementsHistogram histogram = new TopElementsHistogram(30, 0.0001, 0.999, 1);
        histogram.add(Arrays.asList(Slices.utf8Slice("a"), Slices.utf8Slice("b\u00a2"), Slices.utf8Slice("\u0472c"), Slices.utf8Slice("a"), Slices.utf8Slice("a"), Slices.utf8Slice("b\u00a2")));
        assertEquals(histogram.getTopElements(), ImmutableMap.of(Slices.utf8Slice("a"), 3L, Slices.utf8Slice("b\u00a2"), 2L));
    }

    public static List<String> getDeterministicDataSet()
    {
        List<String> lst = new ArrayList<>();
        lst.add("top0:40");
        lst.add("top1:41");
        lst.add("top2:42");
        lst.add("top3:43");

        Random random = new Random();
        int count = 160;
        while (count < 150000) {
            String item = UUID.randomUUID().toString();
            long itemCount = random.nextInt(29);
            count += itemCount;
            lst.add(item + ":" + itemCount);
        }
        return lst;
    }

    @Test(invocationCount = 10)
    public void testDeterministicEstimates()
    {
        List<String> lst = getDeterministicDataSet();
        TopElementsHistogram h1;
        Collections.shuffle(lst);
        h1 = new TopElementsHistogram(0.02, 0.0001, 0.9999, 5);
        for (String entry : lst) {
            String[] e = entry.split(":");
            h1.add(Slices.utf8Slice(e[0]), Long.valueOf(e[1]));
        }
        Set<Slice> s1 = h1.getTopElements().keySet();
        assertTrue(s1.contains(Slices.utf8Slice("top0")));
        assertTrue(s1.contains(Slices.utf8Slice("top1")));
        assertTrue(s1.contains(Slices.utf8Slice("top2")));
        assertTrue(s1.contains(Slices.utf8Slice("top3")));
    }

    @Test
    public void testNonCcmsMerge()
    {
        TopElementsHistogram h1 = new TopElementsHistogram(30, 0.1, 0.9999, 5);
        TopElementsHistogram h2 = new TopElementsHistogram(30, 0.1, 0.9999, 5);
        TopElementsHistogram h3 = new TopElementsHistogram(30, 0.1, 0.9999, 5);
        TopElementsHistogram h4 = new TopElementsHistogram(30, 0.1, 0.9999, 5);
        TopElementsHistogram hf1 = new TopElementsHistogram(30, 0.1, 0.9999, 5);
        h1.add(Arrays.asList(A, B, C, A, A, B));
        h2.add(Arrays.asList(A, B, C, A, A, B));
        h3.add(Arrays.asList(A, B, C, A, A, B));
        h4.add(Arrays.asList(A, B, C, A, A, B));
        hf1.merge(h1, h2, h3, h4);
        h2.merge(h3, h1, h4);
        assertTrue(hf1.isPrecise());
        assertTrue(h2.isPrecise());
        assertEquals(hf1.getTopElements(), h2.getTopElements());
    }

    @Test
    public void testSwitchToSketchMerge()
    {
        TopElementsHistogram h1 = new TopElementsHistogram(30, 0.0001, 0.9999, 5);
        TopElementsHistogram h2 = new TopElementsHistogram(30, 0.0001, 0.9999, 5);
        TopElementsHistogram h3 = new TopElementsHistogram(30, 0.0001, 0.9999, 5);
        TopElementsHistogram h4 = new TopElementsHistogram(30, 0.0001, 0.9999, 5);
        TopElementsHistogram hf1 = new TopElementsHistogram(30, 0.0001, 0.9999, 5);

        h1.add(Arrays.asList(A, B, Slices.utf8Slice("\u0472bc"), A, A, B));
        h2.add(Arrays.asList(A, B, C, Slices.utf8Slice("d"), A, A, B));
        h3.add(Arrays.asList(A, B, C, Slices.utf8Slice("e"), A, A, B));
        h4.add(Arrays.asList(A, B, C, Slices.utf8Slice("f"), A, A, B));
        assertTrue(h1.isPrecise());
        h1.switchToSketch();
        hf1.merge(h1, h2, h3, h4);
        h2.merge(h3, h1, h4);
        assertEquals(hf1.getTopElements(), h2.getTopElements());
        assertFalse(hf1.isPrecise());
        assertFalse(h2.isPrecise());
    }

    @Test(invocationCount = 1)
    public void testMerge()
    {
        List<String> lst = getDeterministicDataSet();
        Collections.shuffle(lst);
        TopElementsHistogram h1 = new TopElementsHistogram(0.05, 0.0001, 0.9999, 5);
        TopElementsHistogram h2 = new TopElementsHistogram(0.05, 0.0001, 0.9999, 5);
        TopElementsHistogram h3 = new TopElementsHistogram(0.05, 0.0001, 0.9999, 5);
        TopElementsHistogram h4 = new TopElementsHistogram(0.05, 0.0001, 0.9999, 5);
        TopElementsHistogram hf1 = new TopElementsHistogram(0.05, 0.0001, 0.9999, 5);
        String[] e;
        for (int i = 4; i < lst.size(); i += 4) {
            e = lst.get(i - 4).split(":");
            Slice eS = Slices.utf8Slice(e[0]);
            h1.add(eS, Long.valueOf(e[1]));
            e = lst.get(i - 3).split(":");
            h2.add(eS, Long.valueOf(e[1]));
            e = lst.get(i - 2).split(":");
            h3.add(eS, Long.valueOf(e[1]));
            e = lst.get(i - 1).split(":");
            h4.add(eS, Long.valueOf(e[1]));
        }
        hf1.merge(h1, h2, h3, h4);
        h2.merge(h3, h1, h4);
        assertEquals(hf1.getTopElements(), h2.getTopElements());
    }

    @Test
    public void testBigSet()
    {
        TopElementsHistogram histogram = new TopElementsHistogram(0.02, 0.00002, 0.9999, 5);
        List<String> arrEntries = getDeterministicDataSet();
        for (String entry : arrEntries) {
            String[] e = entry.split(":");
            histogram.add(Slices.utf8Slice(e[0]), Long.valueOf(e[1]));
        }
        assertEquals(histogram.getTopElements(), ImmutableMap.of(Slices.utf8Slice("top0"), 40L, Slices.utf8Slice("top1"), 41L, Slices.utf8Slice("top2"), 42L, Slices.utf8Slice("top3"), 43L));
    }

    @Test(invocationCount = 1)
    public void testHighCollisions()
    {
        int cnt = 100;
        TopElementsHistogram[] histograms = new TopElementsHistogram[cnt];
        for (int i = 0; i < cnt; i++) {
            // low confidence to increase chances of collision in hash
            histograms[i] = new TopElementsHistogram(0.1, 0.0001, 0.1, 5);
        }
        for (int j = 0; j < 1500; j++) {
            for (int i = 0; i < cnt; i++) {
                histograms[i].add(Slices.utf8Slice(UUID.randomUUID().toString()));
                if (j % 500 == 0) {
                    assertLessThan(histograms[i].getEntriesCount(), histograms[i].getMaxEntries() + 1);
                }
            }
        }
    }

    @Test
    public void testFalsePositive()
    {
        TopElementsHistogram histogram = new TopElementsHistogram(1.5, 0.01, 0.99, 1);
        histogram.add(Slices.utf8Slice("a"), 2);
        histogram.add(Slices.utf8Slice("b"), 3);
        histogram.add(Slices.utf8Slice("c"), 5);
        Random random = new Random();
        for (int i = 0; i < 190; i++) {
            String s = String.valueOf(65 + random.nextInt(24));
            histogram.add(Slices.utf8Slice(s));
        }

        assertEquals(histogram.getRowsProcessed(), 200);
        assertNull(histogram.getTopElements().get(Slices.utf8Slice("a")));  //a should not make it to top elements
        assertEquals((long) histogram.getTopElements().get(Slices.utf8Slice("b")), 3);
        assertEquals((long) histogram.getTopElements().get(Slices.utf8Slice("c")), 5);
    }

    @Test
    public void testSwitchToSketchMemory()
    {
        int minPercentShare = 20;
        double error = 0.001;
        TopElementsHistogram histogram;
        histogram = new TopElementsHistogram(minPercentShare, error, 0.99, 1);
        assertEquals(histogram.estimatedInMemorySize(), 96);
        for (int i = 0; i < histogram.getMaxEntries(); i++) {
            histogram.add(Slices.utf8Slice(UUID.randomUUID().toString()));
        }
        assertTrue(histogram.isPrecise());
        assertLessThan(histogram.estimatedInMemorySize(), 100000L);
        // adding one more element will trigger switch to ccms
        histogram.add(Slices.utf8Slice(UUID.randomUUID().toString()));
        assertFalse(histogram.isPrecise());
        assertLessThan(histogram.estimatedInMemorySize(), 300000L);
    }

    @Test
    public void testEmptyHistogramMemory()
    {
        TopElementsHistogram histogram;
        histogram = new TopElementsHistogram(10, 0.01, 0.99, 1);
        histogram.switchToSketch();
        assertTrue(histogram.estimatedInMemorySize() > 20000 && histogram.estimatedInMemorySize() < 21000);
        histogram = new TopElementsHistogram(1.5, 0.01, 0.99, 8);
        histogram.switchToSketch();
        assertTrue(histogram.estimatedInMemorySize() > 20000 && histogram.estimatedInMemorySize() < 30000);
        histogram = new TopElementsHistogram(10, 0.0001, 0.99, 1);
        histogram.switchToSketch();
        assertTrue(histogram.estimatedInMemorySize() > 2000000 && histogram.estimatedInMemorySize() < 2100000);
        histogram = new TopElementsHistogram(10, 0.01, 0.999, 1);
        histogram.switchToSketch();
        assertTrue(histogram.estimatedInMemorySize() > 27000 && histogram.estimatedInMemorySize() < 28000);
        histogram = new TopElementsHistogram(10, 0.0001, 0.999, 1);
        histogram.switchToSketch();
        assertTrue(histogram.estimatedInMemorySize() > 2700000 && histogram.estimatedInMemorySize() < 2800000);
        histogram = new TopElementsHistogram(10, 0.00001, 0.99, 1);
        histogram.switchToSketch();
        assertTrue(histogram.estimatedInMemorySize() > 20000000 && histogram.estimatedInMemorySize() < 21000000);
        histogram = new TopElementsHistogram(10, 0.0000001, 0.999, 1);
        try {
            histogram.estimatedInMemorySize();
        }
        catch (RuntimeException e) {
            assertContains(e.getMessage(), "breached max allowed memory");
        }
    }

    @Test
    public void testMemoryUsageStability()
    {
        String[] testStrings = new String[10];
        for (int i = 0; i < testStrings.length; i++) {
            testStrings[i] = UUID.randomUUID().toString();
        }
        TopElementsHistogram histogram = new TopElementsHistogram(20, 0.01, 0.99, 1);
        long emtpyHistogramMemoryUsage = histogram.estimatedInMemorySize();

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            histogram.add(Slices.utf8Slice(testStrings[random.nextInt(10)]));
        }
        long stabilizedMemorySize = histogram.estimatedInMemorySize();
        assertGreaterThan(stabilizedMemorySize, emtpyHistogramMemoryUsage);

        for (int i = 0; i < 1000; i++) {
            histogram.add(Slices.utf8Slice(testStrings[random.nextInt(10)]));
        }
        assertEquals(stabilizedMemorySize, histogram.estimatedInMemorySize());
    }

    @Test(invocationCount = 1)
    public void testSerialize()
    {
        TopElementsHistogram in = new TopElementsHistogram(5, 0.0001, 0.9999, 1);
        populateHistogram(in);
        TopElementsHistogram out = new TopElementsHistogram(in.serialize());
        assertTrue(in.equals(out));
    }
}
