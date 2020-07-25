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
package com.facebook.presto.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.plugin.geospatial.GeometryType;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.geospatial.aggregation.GeometryStateFactory.GroupedGeometryState;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestGeometryStateSerializer
{
    @Test
    public void testSerializeDeserialize()
    {
        AccumulatorStateFactory<GeometryState> factory = StateCompiler.generateStateFactory(GeometryState.class);
        AccumulatorStateSerializer<GeometryState> serializer = StateCompiler.generateStateSerializer(GeometryState.class);
        GeometryState state = factory.createSingleState();

        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"), 0);

        BlockBuilder builder = GeometryType.GEOMETRY.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        assertEquals(GeometryType.GEOMETRY.getObjectValue(null, block, 0), "POINT (1 2)");

        long previousMemorySize = state.getGeometry().estimateMemorySize();
        state.setGeometry(null, previousMemorySize);
        serializer.deserialize(block, 0, state);

        assertEquals(state.getGeometry().asText(), "POINT (1 2)");
    }

    @Test
    public void testSerializeDeserializeGrouped()
    {
        AccumulatorStateFactory<GeometryState> factory = StateCompiler.generateStateFactory(GeometryState.class);
        AccumulatorStateSerializer<GeometryState> serializer = StateCompiler.generateStateSerializer(GeometryState.class);
        GroupedGeometryState state = (GroupedGeometryState) factory.createGroupedState();

        // Add state to group 1
        state.setGroupId(1);
        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"), 0);
        // Add another state to group 2, to show that this doesn't affect the group under test (group 1)
        state.setGroupId(2);
        state.setGeometry(OGCGeometry.fromText("POINT (2 3)"), 0);
        // Return to group 1
        state.setGroupId(1);

        BlockBuilder builder = GeometryType.GEOMETRY.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        assertEquals(GeometryType.GEOMETRY.getObjectValue(null, block, 0), "POINT (1 2)");

        long previousMemorySize = state.getGeometry().estimateMemorySize();
        state.setGeometry(null, previousMemorySize);
        serializer.deserialize(block, 0, state);

        // Assert the state of group 1
        assertEquals(state.getGeometry().asText(), "POINT (1 2)");
        // Verify nothing changed in group 2
        state.setGroupId(2);
        assertEquals(state.getGeometry().asText(), "POINT (2 3)");
        // Groups we did not touch are null
        state.setGroupId(3);
        assertNull(state.getGeometry());
    }
}
