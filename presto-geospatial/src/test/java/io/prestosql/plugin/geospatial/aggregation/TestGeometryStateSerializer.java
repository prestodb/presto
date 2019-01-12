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
package io.prestosql.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.plugin.geospatial.GeometryType;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import org.testng.annotations.Test;

import static io.prestosql.plugin.geospatial.aggregation.GeometryStateFactory.GroupedGeometryState;
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

        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"));

        BlockBuilder builder = GeometryType.GEOMETRY.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        assertEquals(GeometryType.GEOMETRY.getObjectValue(null, block, 0), "POINT (1 2)");

        state.setGeometry(null);
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
        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"));
        // Add another state to group 2, to show that this doesn't affect the group under test (group 1)
        state.setGroupId(2);
        state.setGeometry(OGCGeometry.fromText("POINT (2 3)"));
        // Return to group 1
        state.setGroupId(1);

        BlockBuilder builder = GeometryType.GEOMETRY.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        assertEquals(GeometryType.GEOMETRY.getObjectValue(null, block, 0), "POINT (1 2)");

        state.setGeometry(null);
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
