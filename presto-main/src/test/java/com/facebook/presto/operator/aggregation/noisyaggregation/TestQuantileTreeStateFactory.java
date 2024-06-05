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

import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.util.Collections;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestQuantileTreeStateFactory
{
    private final QuantileTreeStateFactory factory = new QuantileTreeStateFactory();

    @Test
    public void testCreateSingleStateEmpty()
    {
        QuantileTreeState state = factory.createSingleState();
        assertNull(state.getQuantileTree());
        assertNull(state.getProbabilities());
        assertEquals(state.getEstimatedSize(), ClassLayout.parseClass(QuantileTreeStateFactory.SingleQuantileTreeState.class).instanceSize());
    }

    @Test
    public void testCreateSingleStatePresent()
    {
        QuantileTreeState state = factory.createSingleState();
        QuantileTree tree = new QuantileTree(0, 123.45, 1024, 2, 5, 272);
        state.setQuantileTree(tree);
        state.setDelta(0.1);
        state.setEpsilon(2);
        state.setProbabilities(Collections.singletonList(0.3));
        assertEquals(state.getQuantileTree(), tree);
        assertEquals(state.getDelta(), 0.1);
        assertEquals(state.getEpsilon(), 2);
        assertEquals(state.getProbabilities().get(0), 0.3);
    }

    @Test
    public void testCreateGroupedStateEmpty()
    {
        QuantileTreeState state = factory.createGroupedState();
        assertNull(state.getQuantileTree());
        assertNull(state.getProbabilities());
    }

    @Test
    public void testCreateGroupedStatePresent()
    {
        QuantileTreeState state = factory.createGroupedState();
        assertTrue(state instanceof QuantileTreeStateFactory.GroupedQuantileTreeState);
        QuantileTreeStateFactory.GroupedQuantileTreeState groupedState = (QuantileTreeStateFactory.GroupedQuantileTreeState) state;

        groupedState.setGroupId(1);
        assertNull(state.getQuantileTree());
        QuantileTree tree1 = new QuantileTree(0, 16, 16, 2, 5, 272);
        tree1.add(0);
        groupedState.setQuantileTree(tree1);
        groupedState.setDelta(0.1);
        groupedState.setEpsilon(2);
        groupedState.setProbabilities(Collections.singletonList(0.3));
        assertEquals(state.getQuantileTree(), tree1);
        assertEquals(state.getDelta(), 0.1);
        assertEquals(state.getEpsilon(), 2);
        assertEquals(state.getProbabilities().get(0), 0.3);

        groupedState.setGroupId(2);
        assertNull(state.getQuantileTree());
        QuantileTree tree2 = new QuantileTree(0, 123.45, 1024, 2, 5, 272);
        tree2.add(1);
        tree2.add(2);
        groupedState.setQuantileTree(tree2);
        groupedState.setDelta(0.1);
        groupedState.setEpsilon(2);
        groupedState.setProbabilities(Collections.singletonList(0.3));
        assertEquals(state.getQuantileTree(), tree2);
        assertEquals(state.getDelta(), 0.1);
        assertEquals(state.getEpsilon(), 2);
        assertEquals(state.getProbabilities().get(0), 0.3);

        groupedState.setGroupId(1);
        assertNotNull(state.getQuantileTree());
        assertEquals(state.getQuantileTree().cardinality(), 1); // we inserted one item into this group

        groupedState.setGroupId(2);
        assertNotNull(state.getQuantileTree());
        assertEquals(state.getQuantileTree().cardinality(), 2); // we inserted two items into this group

        groupedState.setGroupId(3);
        assertNull(state.getQuantileTree()); // this group doesn't exist
    }

    @Test
    public void testMemoryAccounting()
    {
        QuantileTreeState state = factory.createGroupedState();
        long oldSize = state.getEstimatedSize();
        QuantileTree tree = new QuantileTree(0, 123.45, 1024, 2, 5, 272);
        state.setQuantileTree(tree);
        state.addMemoryUsage(tree.estimatedSizeInBytes());
        state.setEpsilon(5);
        state.setDelta(0.001);
        state.setProbabilities(Collections.singletonList(0.5));
        assertEquals(
                state.getEstimatedSize(),
                oldSize + tree.estimatedSizeInBytes(),
                format(
                        "Expected size %s before adding tree and %s after",
                        oldSize,
                        state.getEstimatedSize()));
    }
}
