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
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.type.QuantileTreeType;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestQuantileTreeStateSerializer
{
    private static QuantileTree generateQuantileTree()
    {
        QuantileTree tree = new QuantileTree(0, 123.45, 1024, 2, 5, 272);
        for (int i = 0; i < 50; i++) {
            tree.add(i);
        }
        return tree;
    }

    @Test
    public void testQuantileTreeStateSerializeDeserialize()
    {
        AccumulatorStateFactory<QuantileTreeState> factory = StateCompiler.generateStateFactory(QuantileTreeState.class);
        AccumulatorStateSerializer<QuantileTreeState> serializer = StateCompiler.generateStateSerializer(QuantileTreeState.class);
        QuantileTreeState state = factory.createSingleState();
        state.setQuantileTree(generateQuantileTree());

        state.setDelta(0.1);
        state.setEpsilon(2.3);
        state.setProbabilities(Collections.singletonList(0.4));

        BlockBuilder builder = QuantileTreeType.QUANTILE_TREE_TYPE.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        state.setQuantileTree(null);
        state.setProbabilities(Collections.singletonList(0.0));
        state.setEpsilon(0);
        state.setDelta(0);
        serializer.deserialize(block, 0, state);

        assertEquals(state.getDelta(), 0.1);
        assertEquals(state.getEpsilon(), 2.3);
        assertEquals(state.getProbabilities().get(0), 0.4);

        QuantileTree refTree = generateQuantileTree();

        assertEquals(state.getQuantileTree().quantile(0.5), refTree.quantile(0.5));
        assertEquals(state.getQuantileTree().quantile(0.95), refTree.quantile(0.95));
    }

    @Test
    public void testQuantileTreeStateSerializeDeserializeGrouped()
    {
        AccumulatorStateFactory<QuantileTreeState> factory = StateCompiler.generateStateFactory(QuantileTreeState.class);
        AccumulatorStateSerializer<QuantileTreeState> serializer = StateCompiler.generateStateSerializer(QuantileTreeState.class);
        QuantileTreeStateFactory.GroupedQuantileTreeState groupedState = (QuantileTreeStateFactory.GroupedQuantileTreeState) factory.createGroupedState();
        QuantileTree tree1 = generateQuantileTree();
        QuantileTree tree2 = generateQuantileTree();
        for (int i = 0; i < 100; i++) {
            tree2.add(0); // add a spike to the histogram in tree2 (this makes quantiles in the two trees unequal)
        }

        // Add state to group 1
        groupedState.setGroupId(1);
        groupedState.setQuantileTree(tree1);
        groupedState.setEpsilon(1);
        groupedState.setDelta(0.001);
        groupedState.setProbabilities(Collections.singletonList(0.1));
        // Add another state to group 2, the following test will show it does not affect the group 1
        groupedState.setGroupId(2);
        groupedState.setQuantileTree(tree2);
        groupedState.setEpsilon(1);
        groupedState.setDelta(0.001);
        groupedState.setProbabilities(Collections.singletonList(0.1));
        // Return to group 1
        groupedState.setGroupId(1);

        BlockBuilder builder = QuantileTreeType.QUANTILE_TREE_TYPE.createBlockBuilder(null, 1);
        serializer.serialize(groupedState, builder);
        Block block = builder.build();

        // check on group 1
        serializer.deserialize(block, 0, groupedState);
        double valueFromStateGroup1 = groupedState.getQuantileTree().quantile(0.9);
        double valueFromOriginalTree1 = tree1.quantile(0.9);
        assertNotNull(groupedState.getQuantileTree());
        assertEquals(valueFromStateGroup1, valueFromOriginalTree1);

        // check on group 2
        groupedState.setGroupId(2);
        assertNotNull(groupedState.getQuantileTree());
        assertEquals(groupedState.getQuantileTree().quantile(0.7), tree2.quantile(0.7));

        // Groups we did not touch are null
        groupedState.setGroupId(3);
        assertNull(groupedState.getQuantileTree());
    }
}
