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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.TestingSeededRandomizationStrategy;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestSfmSketchStateFactory
{
    private final SfmSketchStateFactory factory = new SfmSketchStateFactory();

    @Test
    public void testCreateSingleStateEmpty()
    {
        SfmSketchState state = factory.createSingleState();
        assertNull(state.getSketch());
        assertEquals(state.getEstimatedSize(), ClassLayout.parseClass(SfmSketchStateFactory.SingleSfmSketchState.class).instanceSize());
    }

    @Test
    public void testCreateSingleStatePresent()
    {
        SfmSketchState state = factory.createSingleState();
        long emptySize = state.getEstimatedSize();

        SfmSketch sketch = SfmSketch.create(16, 16);
        state.setSketch(sketch);
        assertEquals(sketch, state.getSketch());
        assertEquals(state.getEstimatedSize() - emptySize, sketch.getRetainedSizeInBytes());
    }

    @Test
    public void testCreateGroupedStateEmpty()
    {
        SfmSketchState state = factory.createGroupedState();
        assertNull(state.getSketch());
        int instanceSize = 40;
        long bigObjectSize = (new ObjectBigArray<>()).sizeOf();
        assertTrue(state.getEstimatedSize() >= instanceSize + bigObjectSize, format("Estimated memory size was %d", state.getEstimatedSize()));
    }

    @Test
    public void testCreateGroupedStatePresent()
    {
        SfmSketchState state = factory.createGroupedState();
        assertTrue(state instanceof SfmSketchStateFactory.GroupedSfmSketchState);
        SfmSketchStateFactory.GroupedSfmSketchState groupedState = (SfmSketchStateFactory.GroupedSfmSketchState) state;

        groupedState.setGroupId(1);
        assertNull(state.getSketch());
        SfmSketch sketch1 = SfmSketch.create(16, 16);
        groupedState.setSketch(sketch1);
        assertEquals(state.getSketch(), sketch1);

        groupedState.setGroupId(2);
        assertNull(state.getSketch());
        SfmSketch sketch2 = SfmSketch.create(32, 32);
        groupedState.setSketch(sketch2);
        assertEquals(state.getSketch(), sketch2);

        groupedState.setGroupId(1);
        assertNotNull(state.getSketch());
    }

    @Test
    public void testGroupedMemoryAccounting()
    {
        SfmSketchState state = factory.createGroupedState();
        long emptySize = state.getEstimatedSize();

        // Create three sketches:
        // - sketch1 has one 1-bit.
        // - sketch2 has one other 1-bit.
        // - sketch3 has many random bits.
        // On initial creation, they will all be of equal size.
        // However, due to the internals of BitSet.valueOf(), serializing and deserializing will drop trailing zeros in the sketch.
        // So we can create sketches of equal logical size but different physical size by round-tripping them.
        // The physical sizes (SfmSketch.getRetainedSizeInBytes()) observed by the author after this round-trip are:
        // - sketch1: 232
        // - sketch2: 120
        // - sketch3: 2144
        SfmSketch sketch1 = SfmSketch.create(1024, 16);
        sketch1.add(1);
        sketch1 = SfmSketch.deserialize(sketch1.serialize());

        SfmSketch sketch2 = SfmSketch.create(1024, 16);
        sketch2.add(0);
        sketch2 = SfmSketch.deserialize(sketch2.serialize());

        SfmSketch sketch3 = SfmSketch.create(1024, 16);
        sketch3.enablePrivacy(0.1, new TestingSeededRandomizationStrategy(1));
        sketch3 = SfmSketch.deserialize(sketch3.serialize());

        // Set initial state to use sketch1, check memory estimate.
        // Memory usage should increase by the size of the new sketch.
        state.setSketch(sketch1);
        long memoryIncrease = state.getEstimatedSize() - emptySize;
        assertEquals(memoryIncrease, state.getSketch().getRetainedSizeInBytes());

        // Merge in sketch2, and ensure memory estimate reflects the size of the merged sketch.
        // Memory usage should stay the same, as the merged sketch should be the same size as the initial sketch.
        state.mergeSketch(sketch2);
        memoryIncrease = state.getEstimatedSize() - emptySize - memoryIncrease;
        assertEquals(memoryIncrease, 0);
        assertEquals(state.getEstimatedSize() - emptySize, state.getSketch().getRetainedSizeInBytes());

        // Merge in sketch3, and ensure memory estimate reflects the size of the merged sketch.
        // Memory usage should increase now to be at least as large as sketch3.
        // (The actual size may be larger than sketch3 due to the way BitSet expands.)
        state.mergeSketch(sketch3);
        memoryIncrease = state.getEstimatedSize() - emptySize - memoryIncrease;
        assertTrue(memoryIncrease >= 0);
        assertEquals(state.getEstimatedSize() - emptySize, state.getSketch().getRetainedSizeInBytes());
    }
}
