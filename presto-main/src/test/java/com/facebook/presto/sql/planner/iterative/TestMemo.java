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
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestMemo
{
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testInitialization()
            throws Exception
    {
        ProjectNode plan = project(values());
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
        PlanNode plan = project(filter(values()));

        Memo memo = new Memo(idAllocator, plan);
        assertEquals(memo.getGroupCount(), 3);

        // replace child of root node with subtree
        PlanNode transformed = project(values());
        memo.replace(getChildGroup(memo, memo.getRootGroup()), transformed, "rule");
        assertEquals(memo.getGroupCount(), 3);
        assertMatchesStructure(memo.extract(), project(plan.getId(), transformed));
    }

    /*
      From: X -> Y  -> Z  -> W
      To:   X -> Y' -> Z' -> W
     */
    @Test
    public void testReplaceNonLeafSubtree()
            throws Exception
    {
        PlanNode w = values();
        PlanNode z = filter(w);
        PlanNode y = filter(z);
        PlanNode x = project(y);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 4);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        int zGroup = getChildGroup(memo, yGroup);

        PlanNode rewrittenW = memo.getNode(zGroup).getSources().get(0);

        ProjectNode newZ = project(rewrittenW);
        ProjectNode newY = project(newZ);

        memo.replace(yGroup, newY, "rule");

        assertEquals(memo.getGroupCount(), 4);

        assertMatchesStructure(
                memo.extract(),
                project(x.getId(),
                        project(newY.getId(),
                                project(newZ.getId(),
                                        values(w.getId())))));
    }

    /*
      From: X -> Y -> Z
      To:   X -> Z
     */
    @Test
    public void testRemoveNode()
            throws Exception
    {
        PlanNode z = values();
        PlanNode y = filter(z);
        PlanNode x = project(y);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        memo.replace(yGroup, memo.getNode(yGroup).getSources().get(0), "rule");

        assertEquals(memo.getGroupCount(), 2);

        assertMatchesStructure(
                memo.extract(),
                project(x.getId(),
                        values(z.getId())));
    }

    /*
       From: X -> Z
       To:   X -> Y -> Z
     */
    @Test
    public void testInsertNode()
            throws Exception
    {
        PlanNode z = values();
        PlanNode x = project(z);

        Memo memo = new Memo(idAllocator, x);

        assertEquals(memo.getGroupCount(), 2);

        int zGroup = getChildGroup(memo, memo.getRootGroup());
        PlanNode y = filter(memo.getNode(zGroup));
        memo.replace(zGroup, y, "rule");

        assertEquals(memo.getGroupCount(), 3);

        assertMatchesStructure(
                memo.extract(),
                project(x.getId(),
                        filter(y.getId(),
                                values(z.getId()))));
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
        PlanNode z = values();
        PlanNode y = filter(z);
        PlanNode x = project(y);

        Memo memo = new Memo(idAllocator, x);
        assertEquals(memo.getGroupCount(), 3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());

        PlanNode rewrittenZ = memo.getNode(yGroup).getSources().get(0);
        PlanNode y1 = filter(rewrittenZ);
        PlanNode y2 = project(rewrittenZ);

        PlanNode newX = join(y1, y2);
        memo.replace(memo.getRootGroup(), newX, "rule");
        assertEquals(memo.getGroupCount(), 4);

        assertMatchesStructure(
                memo.extract(),
                join(newX.getId(),
                        filter(y1.getId(), values(z.getId())),
                        project(y2.getId(), values(z.getId()))));
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

    private JoinNode join(PlanNodeId id, PlanNode left, PlanNode right)
    {
        return new JoinNode(id, JoinNode.Type.INNER, left, right, ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private JoinNode join(PlanNode left, PlanNode right)
    {
        return join(idAllocator.getNextId(), left, right);
    }

    private ProjectNode project(PlanNodeId id, PlanNode source)
    {
        return new ProjectNode(id, source, Assignments.of());
    }

    private ProjectNode project(PlanNode source)
    {
        return project(idAllocator.getNextId(), source);
    }

    private FilterNode filter(PlanNodeId id, PlanNode source)
    {
        return new FilterNode(id, source, BooleanLiteral.TRUE_LITERAL);
    }

    private FilterNode filter(PlanNode source)
    {
        return filter(idAllocator.getNextId(), source);
    }

    private ValuesNode values(PlanNodeId id)
    {
        return new ValuesNode(id, ImmutableList.of(), ImmutableList.of());
    }

    private ValuesNode values()
    {
        return values(idAllocator.getNextId());
    }
}
