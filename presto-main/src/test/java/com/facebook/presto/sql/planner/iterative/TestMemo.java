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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestMemo
{
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testInitialization()
            throws Exception
    {
        PlanNode plan = node(node());
        Memo memo = new Memo(idAllocator, plan);

        assertEquals(memo.getGroupCount(), 2);
        assertMatchesStructure(plan, memo.extract());
    }

    /*
      From: X -> Y  -> Z
      To:   X -> Y' -> Z'
     */
    @Test
    public void testReplaceSubtree()
            throws Exception
    {
        PlanNode plan = node(node(node()));

        Memo memo = new Memo(idAllocator, plan);
        assertEquals(memo.getGroupCount(), 3);

        // replace child of root node with subtree
        PlanNode transformed = node(node());
        memo.replace(getChildGroup(memo, memo.getRootGroup()), transformed, "rule");
        assertEquals(memo.getGroupCount(), 3);
        assertMatchesStructure(memo.extract(), node(plan.getId(), transformed));
    }

    /*
      From: X -> Y  -> Z  -> W
      To:   X -> Y' -> Z' -> W
     */
    @Test
    public void testReplaceNonLeafSubtree()
            throws Exception
    {
        PlanNode w = node();
        PlanNode z = node(w);
        PlanNode y = node(z);
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 4);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        int zGroup = getChildGroup(memo, yGroup);

        PlanNode rewrittenW = memo.getNode(zGroup).getSources().get(0);

        PlanNode newZ = node(rewrittenW);
        PlanNode newY = node(newZ);

        memo.replace(yGroup, newY, "rule");

        assertEquals(memo.getGroupCount(), 4);

        assertMatchesStructure(
                memo.extract(),
                node(x.getId(),
                        node(newY.getId(),
                                node(newZ.getId(),
                                        node(w.getId())))));
    }

    /*
      From: X -> Y -> Z
      To:   X -> Z
     */
    @Test
    public void testRemoveNode()
            throws Exception
    {
        PlanNode z = node();
        PlanNode y = node(z);
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        memo.replace(yGroup, memo.getNode(yGroup).getSources().get(0), "rule");

        assertEquals(memo.getGroupCount(), 2);

        assertMatchesStructure(
                memo.extract(),
                node(x.getId(),
                        node(z.getId())));
    }

    /*
       From: X -> Z
       To:   X -> Y -> Z
     */
    @Test
    public void testInsertNode()
            throws Exception
    {
        PlanNode z = node();
        PlanNode x = node(z);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 2);

        int zGroup = getChildGroup(memo, memo.getRootGroup());
        PlanNode y = node(memo.getNode(zGroup));
        memo.replace(zGroup, y, "rule");

        assertEquals(memo.getGroupCount(), 3);

        assertMatchesStructure(
                memo.extract(),
                node(x.getId(),
                        node(y.getId(),
                                node(z.getId()))));
    }

    /*
      From: X -> Y -> Z
      To:   X --> Y1' --> Z
              \-> Y2' -/
     */
    @Test
    public void testMultipleReferences()
            throws Exception
    {
        PlanNode z = node();
        PlanNode y = node(z);
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);
        assertEquals(memo.getGroupCount(), 3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());

        PlanNode rewrittenZ = memo.getNode(yGroup).getSources().get(0);
        PlanNode y1 = node(rewrittenZ);
        PlanNode y2 = node(rewrittenZ);

        PlanNode newX = node(y1, y2);
        memo.replace(memo.getRootGroup(), newX, "rule");
        assertEquals(memo.getGroupCount(), 4);

        assertMatchesStructure(
                memo.extract(),
                node(newX.getId(),
                        node(y1.getId(), node(z.getId())),
                        node(y2.getId(), node(z.getId()))));
    }

    private static void assertMatchesStructure(PlanNode actual, PlanNode expected)
    {
        assertEquals(actual.getClass(), expected.getClass());
        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getSources().size(), expected.getSources().size());

        for (int i = 0; i < actual.getSources().size(); i++) {
            assertMatchesStructure(actual.getSources().get(i), expected.getSources().get(i));
        }
    }

    private int getChildGroup(Memo memo, int group)
    {
        PlanNode node = memo.getNode(group);
        GroupReference child = (GroupReference) node.getSources().get(0);

        return child.getGroupId();
    }

    private GenericNode node(PlanNodeId id, PlanNode... children)
    {
        return new GenericNode(id, ImmutableList.copyOf(children));
    }

    private GenericNode node(PlanNode... children)
    {
        return node(idAllocator.getNextId(), children);
    }

    private static class GenericNode
            extends PlanNode
    {
        private final List<PlanNode> sources;

        public GenericNode(PlanNodeId id, List<PlanNode> sources)
        {
            super(id);
            this.sources = ImmutableList.copyOf(sources);
        }

        @Override
        public List<PlanNode> getSources()
        {
            return sources;
        }

        @Override
        public List<Symbol> getOutputSymbols()
        {
            return ImmutableList.of();
        }

        @Override
        public PlanNode replaceChildren(List<PlanNode> newChildren)
        {
            return new GenericNode(getId(), newChildren);
        }
    }
}
