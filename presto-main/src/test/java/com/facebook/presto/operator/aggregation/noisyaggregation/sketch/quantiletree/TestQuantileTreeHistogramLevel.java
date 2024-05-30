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

import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.RandomizationStrategy;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestQuantileTreeHistogramLevel
{
    @Test
    public void testEstimatedSerializedSize()
    {
        HistogramLevel sketch = new HistogramLevel(10);
        assertEquals(sketch.estimatedSerializedSizeInBytes(), sketch.serialize().length());
    }

    @Test
    public void testMemoryUsage()
    {
        HistogramLevel sketch = new HistogramLevel(10);

        long expectedMemoryUsage = ClassLayout.parseClass(HistogramLevel.class).instanceSize() +
                SizeOf.sizeOfFloatArray(10);

        assertEquals(sketch.estimatedSizeInBytes(), expectedMemoryUsage);
    }

    @Test
    public void testSerializeAndDeserialize()
    {
        int bins = 10;
        RandomizationStrategy randomizationStrategy = new TestQuantileTreeSeededRandomizationStrategy(1);
        HistogramLevel sketch = new HistogramLevel(bins);

        // populate bins with 100 random items
        for (int i = 0; i < 100; i++) {
            sketch.add(randomizationStrategy.nextInt(bins));
        }

        // enable privacy
        sketch.addNoise(0.5, randomizationStrategy);

        Slice sketchSlice = sketch.serialize();
        SliceInput sliceInput = new BasicSliceInput(sketchSlice);
        HistogramLevel deserializedSketch = new HistogramLevel(sliceInput);

        for (int i = 0; i < bins; i++) {
            assertEquals(deserializedSketch.query(i), sketch.query(i));
        }
    }

    @Test
    public void testAddOperation()
    {
        HistogramLevel sketch = new HistogramLevel(10);
        assertEquals(sketch.query(3), 0); // no items added yet
        for (int i = 1; i < 20; i++) {
            sketch.add(3);
            assertEquals(sketch.query(3), i); // i items added
        }
    }

    @Test
    public void testAddNoise()
    {
        int bins = 10;
        HistogramLevel sketch = new HistogramLevel(bins);
        sketch.addNoise(0.5, new TestQuantileTreeSeededRandomizationStrategy(1));
        for (int i = 1; i < bins; i++) {
            assertNotEquals(sketch.query(i), 0.0); // should have noise added
        }
    }
}
