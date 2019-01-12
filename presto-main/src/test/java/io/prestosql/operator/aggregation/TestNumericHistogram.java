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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestNumericHistogram
{
    @Test
    public void testBasic()
    {
        double[] data = {
                2.9, 3.1, 3.4, 3.5, 3.1, 2.9, 3, 3.8, 3.6, 3.1, 3.6, 2.5, 2.8, 2.3, 3, 3,
                3.2, 3.8, 2.6, 2.9, 3.9, 3.5, 2.2, 2.9, 3, 3.2, 3.4, 3.2, 3, 4.2, 3, 2.9,
                4.4, 2.4, 3.9, 2.8, 3.1, 3.2, 3, 3, 3.7, 2.9, 3, 3.1, 2.5, 3.3, 2.7, 3,
                3.1, 3, 2.8, 3, 3.5, 2.6, 3.2, 2.6, 2.9, 4, 2.8, 3.2, 2.8, 3.2, 3.4, 2.8,
                3.7, 3.8, 2.7, 3.3, 2.5, 2.8, 2.4, 2.9, 3, 2.7, 3.8, 2.8, 3, 3, 3.5, 3.4,
                3.4, 3.4, 3, 3.8, 2.9, 3, 3.6, 3.1, 3.4, 2.7, 2.5, 2.5, 3.2, 2.7, 2.3,
                3.3, 2.2, 3.7, 3.5, 2.7, 2.8, 3.4, 2.9, 3.4, 3, 2.8, 2.7, 3.1, 3.5, 3.3,
                3.2, 3.1, 3.2, 3.6, 3, 3.2, 3, 2.5, 3.1, 3, 3, 2.7, 2.7, 2.6, 3, 2.3,
                3.3, 2.8, 3.2, 3.4, 2.8, 3, 3.1, 2, 3, 2.5, 2.4, 3.3, 2.3, 3, 2.8, 2.8,
                2.6, 3.8, 3.2, 3.4, 2.5, 4.1, 2.2, 3.4};

        NumericHistogram histogram = new NumericHistogram(4, data.length);

        Set<Double> uniqueValues = new HashSet<>();
        for (double value : data) {
            histogram.add(value);

            uniqueValues.add(value);
            if (uniqueValues.size() == 8) {
                histogram.compact();
                uniqueValues.clear();
            }
        }

        Map<Double, Double> expected = ImmutableMap.<Double, Double>builder()
                .put(2.4571428571428564, 28.0)
                .put(3.568571428571429, 35.0)
                .put(3.0023809523809515, 84.0)
                .put(4.233333333333333, 3.0)
                .build();

        assertEquals(histogram.getBuckets(), expected);
    }

    @Test
    public void testSameValues()
    {
        NumericHistogram histogram = new NumericHistogram(4);

        for (int i = 0; i < 100; ++i) {
            histogram.add(1.0, 3);
            histogram.add(2.0, 5);
        }

        Map<Double, Double> expected = ImmutableMap.<Double, Double>builder()
                .put(1.0, 300.0)
                .put(2.0, 500.0)
                .build();

        assertEquals(histogram.getBuckets(), expected);
    }

    @Test
    public void testRoundtrip()
    {
        NumericHistogram histogram = new NumericHistogram(100, 20);
        for (int i = 0; i < 1000; i++) {
            histogram.add(i);
        }

        Slice serialized = histogram.serialize();
        NumericHistogram deserialized = new NumericHistogram(serialized, 20);

        assertEquals(deserialized.getBuckets(), histogram.getBuckets());
    }

    @Test
    public void testMergeSame()
    {
        NumericHistogram histogram = new NumericHistogram(10, 3);
        for (int i = 0; i < 1000; i++) {
            histogram.add(i);
        }

        Map<Double, Double> expected = Maps.transformValues(histogram.getBuckets(), value -> value * 2);

        histogram.mergeWith(histogram);

        assertEquals(histogram.getBuckets(), expected);
    }

    @Test
    public void testMergeDifferent()
    {
        NumericHistogram histogram1 = new NumericHistogram(10, 3);
        NumericHistogram histogram2 = new NumericHistogram(10, 3);
        for (int i = 0; i < 1000; i++) {
            histogram1.add(i);
            histogram2.add(i + 1000);
        }

        NumericHistogram expected = new NumericHistogram(10, 1000);
        for (Map.Entry<Double, Double> entry : histogram1.getBuckets().entrySet()) {
            expected.add(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Double, Double> entry : histogram2.getBuckets().entrySet()) {
            expected.add(entry.getKey(), entry.getValue());
        }
        expected.compact();

        histogram1.mergeWith(histogram2);
        assertEquals(histogram1.getBuckets(), expected.getBuckets());
    }
}
