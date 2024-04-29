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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.type.SfmSketchType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestSfmSketchStateSerializer
{
    @Test
    public void testSerializeDeserialize()
    {
        AccumulatorStateFactory<SfmSketchState> factory = StateCompiler.generateStateFactory(SfmSketchState.class);
        AccumulatorStateSerializer<SfmSketchState> serializer = StateCompiler.generateStateSerializer(SfmSketchState.class);
        SfmSketchState state = factory.createSingleState();
        SfmSketch sketch = SfmSketch.create(16, 16);
        state.setSketch(sketch);
        state.setEpsilon(0.1);

        BlockBuilder builder = SfmSketchType.SFM_SKETCH.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        state.setSketch(null);
        serializer.deserialize(block, 0, state);

        assertNotNull(state.getSketch());
        assertEquals(state.getEpsilon(), 0.1);
    }

    @Test
    public void testSerializeDeserializeGrouped()
    {
        AccumulatorStateFactory<SfmSketchState> factory = StateCompiler.generateStateFactory(SfmSketchState.class);
        AccumulatorStateSerializer<SfmSketchState> serializer = StateCompiler.generateStateSerializer(SfmSketchState.class);
        SfmSketchStateFactory.GroupedSfmSketchState state = (SfmSketchStateFactory.GroupedSfmSketchState) factory.createGroupedState();
        double epsilon1 = 0.1;
        double epsilon2 = 0.2;
        SfmSketch sketch1 = SfmSketch.create(16, 16);
        SfmSketch sketch2 = SfmSketch.create(32, 16);
        // Add state to group 1
        state.setGroupId(1);
        state.setSketch(sketch1);
        state.setEpsilon(epsilon1);
        // Add another state to group 2, to show that this doesn't affect the group under test (group 1)
        state.setGroupId(2);
        state.setSketch(sketch2);
        state.setEpsilon(epsilon2);
        // Return to group 1
        state.setGroupId(1);

        BlockBuilder builder = SfmSketchType.SFM_SKETCH.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        // Assert the state of group 1
        state.setEpsilon(0.99);
        serializer.deserialize(block, 0, state);
        state.getSketch().cardinality();
        assertNotNull(state.getSketch());
        assertEquals(state.getEpsilon(), epsilon1);
        // Verify nothing changed in group 2
        state.setGroupId(2);
        assertNotNull(state.getSketch());
        assertEquals(state.getEpsilon(), epsilon2);
        // Groups we did not touch are null
        state.setGroupId(3);
        assertNull(state.getSketch());
    }
}
