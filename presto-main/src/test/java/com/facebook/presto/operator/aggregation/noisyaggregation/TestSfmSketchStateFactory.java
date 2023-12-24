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
        SfmSketch sketch = SfmSketch.create(16, 16);
        state.setSketch(sketch);
        assertEquals(sketch, state.getSketch());
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
    public void testMemoryAccounting()
    {
        SfmSketchState state = factory.createGroupedState();
        long oldSize = state.getEstimatedSize();
        SfmSketch sketch1 = SfmSketch.create(16, 16);

        state.setSketch(sketch1);
        assertEquals(
                state.getEstimatedSize(),
                oldSize + sketch1.getRetainedSizeInBytes(),
                format(
                        "Expected old size %s plus new sketch sketch size to be equal than new estimate %s",
                        oldSize,
                        state.getEstimatedSize()));
    }
}
