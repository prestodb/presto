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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationStateFactory;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationStateFactory.GroupedState;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationStateSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.function.IntFunction;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.appendToBlockBuilder;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestReduceAggregationStateSerializer
{
    private static final int GROUPS = 1000;
    private static final SqlFunctionProperties PROPERTIES = TEST_SESSION.getSqlFunctionProperties();

    @Test
    public void testBigintState()
    {
        assertDeserializedStates(BIGINT, group -> (long) group);
    }

    @Test
    public void testArrayState()
    {
        assertDeserializedStates(new ArrayType(BIGINT), group -> ImmutableList.of((long) group, group + 1L, group + 2L, group + 3L));
    }

    @Test
    public void testVarcharState()
    {
        assertDeserializedStates(VARCHAR, group -> "value-" + group);
    }

    @Test
    public void testMapState()
    {
        assertDeserializedStates(mapType(BIGINT, VARCHAR), group -> ImmutableMap.of((long) group, "value-" + group));
    }

    @Test
    public void testRowState()
    {
        assertDeserializedStates(RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR)), group -> ImmutableList.of((long) group, "value-" + group));
    }

    private static void assertDeserializedStates(Type type, IntFunction<Object> valueOfGroup)
    {
        BlockBuilder builder = type.createBlockBuilder(null, GROUPS + 1);
        for (int group = 0; group < GROUPS; group++) {
            appendToBlockBuilder(type, valueOfGroup.apply(group), builder);
        }
        builder.appendNull();
        Block page = builder.build();

        ReduceAggregationStateSerializer serializer = new ReduceAggregationStateSerializer(type);
        GroupedState state = (GroupedState) new ReduceAggregationStateFactory().createGroupedState();
        state.ensureCapacity(GROUPS + 1);
        long emptySize = state.getEstimatedSize();
        for (int group = 0; group <= GROUPS; group++) {
            state.setGroupId(group);
            serializer.deserialize(page, group, state);
        }

        // A state that is a view into the page is charged for the whole page, once per group.
        long perGroup = (state.getEstimatedSize() - emptySize) / GROUPS;
        assertTrue(perGroup < page.getRetainedSizeInBytes() / 10, "each group charged " + perGroup + " bytes for a page of " + page.getRetainedSizeInBytes());

        for (int group = 0; group < GROUPS; group++) {
            state.setGroupId(group);
            BlockBuilder roundTrip = type.createBlockBuilder(null, 1);
            serializer.serialize(state, roundTrip);
            assertEquals(type.getObjectValue(PROPERTIES, roundTrip.build(), 0), type.getObjectValue(PROPERTIES, page, group));
        }
        state.setGroupId(GROUPS);
        assertNull(state.getValue());
    }
}
