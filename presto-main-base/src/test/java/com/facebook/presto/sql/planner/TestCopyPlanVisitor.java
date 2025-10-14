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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestCopyPlanVisitor
{
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private CopyPlanVisitor copyVisitor = new CopyPlanVisitor();

    @Test
    public void testCopySimplePlanWithGroupReference()
    {
        // Create a plan: Project -> Filter -> TableScan
        PlanNode tableScan = createTableScanNode();
        PlanNode filter = createFilterNode(tableScan);
        PlanNode project = createProjectNode(filter);

        Memo memo = new Memo(idAllocator, project);
        int initialGroupCount = memo.getGroupCount();

        // Get the root group and copy it
        PlanNode originalRoot = memo.getNode(memo.getRootGroup());
        PlanNode copiedRoot = originalRoot.accept(copyVisitor, memo);

        // Verify the copied plan has different node IDs
        assertNotEquals(copiedRoot.getId(), originalRoot.getId());
        assertEquals(copiedRoot.getClass(), originalRoot.getClass());

        // Verify the structure is preserved but with new IDs
        ProjectNode originalProject = (ProjectNode) originalRoot;
        ProjectNode copiedProject = (ProjectNode) copiedRoot;

        assertNotEquals(copiedProject.getId(), originalProject.getId());
        assertEquals(copiedProject.getAssignments(), originalProject.getAssignments());

        // Check that child nodes also have new IDs
        PlanNode originalChild = originalProject.getSource();
        PlanNode copiedChild = copiedProject.getSource();
        assertNotEquals(copiedChild.getId(), originalChild.getId());

        // Verify that new groups were created for the copied nodes
        int finalGroupCount = memo.getGroupCount();
        assert (finalGroupCount > initialGroupCount);
    }

    @Test
    public void testCopyPlanWithMultipleGroupReferences()
    {
        // Create a plan with shared subtree: Union -> [Project1 -> TableScan, Project2 -> TableScan]
        PlanNode tableScan = createTableScanNode();
        PlanNode project1 = createProjectNode(tableScan);
        PlanNode project2 = createProjectNode(tableScan);
        PlanNode union = createUnionNode(project1, project2);

        Memo memo = new Memo(idAllocator, union);
        int initialGroupCount = memo.getGroupCount();
        System.out.println("Initial group count: " + initialGroupCount);

        // Store some stats on the shared TableScan group
        int tableScanGroup = getDeepestChildGroup(memo, memo.getRootGroup());
        PlanNodeStatsEstimate stats = PlanNodeStatsEstimate.builder().setOutputRowCount(1000).build();
        PlanCostEstimate cost = new PlanCostEstimate(100, 0, 0, 0);
        memo.storeStats(tableScanGroup, stats);
        memo.storeCost(tableScanGroup, cost);

        // Copy the root plan
        PlanNode originalRoot = memo.getNode(memo.getRootGroup());
        PlanNode copiedRoot = originalRoot.accept(copyVisitor, memo);

        // Verify the copied plan structure
        UnionNode originalUnion = (UnionNode) originalRoot;
        UnionNode copiedUnion = (UnionNode) copiedRoot;

        assertNotEquals(copiedUnion.getId(), originalUnion.getId());
        assertEquals(copiedUnion.getSources().size(), originalUnion.getSources().size());

        // Verify that both branches of the union have been copied with new IDs
        for (int i = 0; i < copiedUnion.getSources().size(); i++) {
            PlanNode originalSource = originalUnion.getSources().get(i);
            PlanNode copiedSource = copiedUnion.getSources().get(i);
            assertNotEquals(copiedSource.getId(), originalSource.getId());
        }

        // Verify that the memo now has more groups due to replication
        int finalGroupCount = memo.getGroupCount();
        System.out.println("Final group count: " + finalGroupCount);

        // The exact count depends on how many groups were created initially and how many were replicated
        // We should have at least more groups than we started with
        assert (finalGroupCount > initialGroupCount);
    }

    @Test
    public void testCopyJoinWithGroupReferences()
    {
        // Create a join plan: Join -> [TableScan1, TableScan2]
        PlanNode leftTableScan = createTableScanNode();
        PlanNode rightTableScan = createTableScanNode();
        PlanNode join = createJoinNode(leftTableScan, rightTableScan);

        Memo memo = new Memo(idAllocator, join);
        int initialGroupCount = memo.getGroupCount();

        // Copy the join
        PlanNode originalJoin = memo.getNode(memo.getRootGroup());
        PlanNode copiedJoin = originalJoin.accept(copyVisitor, memo);

        // Verify the copied join has new IDs
        JoinNode originalJoinNode = (JoinNode) originalJoin;
        JoinNode copiedJoinNode = (JoinNode) copiedJoin;

        assertNotEquals(copiedJoinNode.getId(), originalJoinNode.getId());
        assertEquals(copiedJoinNode.getType(), originalJoinNode.getType());

        // Verify both children have new IDs
        assertNotEquals(copiedJoinNode.getLeft().getId(), originalJoinNode.getLeft().getId());
        assertNotEquals(copiedJoinNode.getRight().getId(), originalJoinNode.getRight().getId());

        // Verify that new groups were created for the copied nodes
        int finalGroupCount = memo.getGroupCount();
        assert (finalGroupCount > initialGroupCount);
    }

    @Test
    public void testCopyWithNestedGroupReferences()
    {
        // Create a nested plan: Project -> Filter -> Project -> TableScan
        PlanNode tableScan = createTableScanNode();
        PlanNode innerProject = createProjectNode(tableScan);
        PlanNode filter = createFilterNode(innerProject);
        PlanNode outerProject = createProjectNode(filter);

        Memo memo = new Memo(idAllocator, outerProject);
        int initialGroupCount = memo.getGroupCount();

        // Store stats on intermediate nodes
        int filterGroup = getChildGroup(memo, memo.getRootGroup());
        PlanNodeStatsEstimate filterStats = PlanNodeStatsEstimate.builder().setOutputRowCount(500).build();
        memo.storeStats(filterGroup, filterStats);

        // Copy the entire plan
        PlanNode originalRoot = memo.getNode(memo.getRootGroup());
        PlanNode copiedRoot = originalRoot.accept(copyVisitor, memo);

        // Verify the structure is preserved with new IDs
        ProjectNode originalOuter = (ProjectNode) originalRoot;
        ProjectNode copiedOuter = (ProjectNode) copiedRoot;

        assertNotEquals(copiedOuter.getId(), originalOuter.getId());

        // Verify nested structure - handle the fact that children might be GroupReferences
        PlanNode originalChild = originalOuter.getSource();
        PlanNode copiedChild = copiedOuter.getSource();
        assertNotEquals(copiedChild.getId(), originalChild.getId());

        // If the child is a GroupReference, get the actual node from the memo
        if (originalChild instanceof GroupReference) {
            originalChild = memo.getNode(((GroupReference) originalChild).getGroupId());
        }
        if (copiedChild instanceof GroupReference) {
            copiedChild = memo.getNode(((GroupReference) copiedChild).getGroupId());
        }

        // Now we can safely cast to FilterNode
        FilterNode originalFilter = (FilterNode) originalChild;
        FilterNode copiedFilter = (FilterNode) copiedChild;
        assertNotEquals(copiedFilter.getId(), originalFilter.getId());

        // Continue with the inner project
        PlanNode originalInnerChild = originalFilter.getSource();
        PlanNode copiedInnerChild = copiedFilter.getSource();

        if (originalInnerChild instanceof GroupReference) {
            originalInnerChild = memo.getNode(((GroupReference) originalInnerChild).getGroupId());
        }
        if (copiedInnerChild instanceof GroupReference) {
            copiedInnerChild = memo.getNode(((GroupReference) copiedInnerChild).getGroupId());
        }

        ProjectNode originalInner = (ProjectNode) originalInnerChild;
        ProjectNode copiedInner = (ProjectNode) copiedInnerChild;
        assertNotEquals(copiedInner.getId(), originalInner.getId());

        // Verify that all levels have been replicated
        int finalGroupCount = memo.getGroupCount();
        assert (finalGroupCount > initialGroupCount);
    }

    @Test
    public void testGroupReferenceReplication()
    {
        // Create a simple plan and then test GroupReference directly
        PlanNode tableScan = createTableScanNode();
        PlanNode project = createProjectNode(tableScan);

        Memo memo = new Memo(idAllocator, project);
        int initialGroupCount = memo.getGroupCount();
        int projectGroup = memo.getRootGroup();
        int tableScanGroup = getChildGroup(memo, projectGroup);

        // Create a GroupReference to the project group
        GroupReference groupRef = new GroupReference(
                Optional.empty(),
                idAllocator.getNextId(),
                projectGroup,
                ImmutableList.of(),
                Optional.empty());

        // Copy the GroupReference
        PlanNode copiedGroupRef = groupRef.accept(copyVisitor, memo);

        // Verify it's a new GroupReference with different group ID
        GroupReference copiedRef = (GroupReference) copiedGroupRef;
        assertNotEquals(copiedRef.getId(), groupRef.getId());
        assertNotEquals(copiedRef.getGroupId(), groupRef.getGroupId());

        // Verify that the referenced group was replicated
        PlanNode originalReferencedNode = memo.getNode(groupRef.getGroupId());
        PlanNode copiedReferencedNode = memo.getNode(copiedRef.getGroupId());
        assertNotEquals(copiedReferencedNode.getId(), originalReferencedNode.getId());
        assertEquals(copiedReferencedNode.getClass(), originalReferencedNode.getClass());

        // Verify memo has additional groups
        int finalGroupCount = memo.getGroupCount();
        assert (finalGroupCount > initialGroupCount);
    }

    @Test
    public void testStatsAndCostPreservation()
    {
        // Create a simple plan
        PlanNode tableScan = createTableScanNode();
        PlanNode project = createProjectNode(tableScan);

        Memo memo = new Memo(idAllocator, project);
        int projectGroup = memo.getRootGroup();

        // Store stats and cost
        PlanNodeStatsEstimate stats = PlanNodeStatsEstimate.builder().setOutputRowCount(100).build();
        PlanCostEstimate cost = new PlanCostEstimate(50, 0, 0, 0);
        memo.storeStats(projectGroup, stats);
        memo.storeCost(projectGroup, cost);

        // Create and copy a GroupReference
        GroupReference groupRef = new GroupReference(
                Optional.empty(),
                idAllocator.getNextId(),
                projectGroup,
                ImmutableList.of(),
                Optional.empty());

        PlanNode copiedGroupRef = groupRef.accept(copyVisitor, memo);
        GroupReference copiedRef = (GroupReference) copiedGroupRef;

        // Verify that stats and cost are preserved in the replicated group
        assertEquals(memo.getStats(copiedRef.getGroupId()), Optional.of(stats));
        assertEquals(memo.getCost(copiedRef.getGroupId()), Optional.of(cost));

        // Verify original group still has its stats and cost
        assertEquals(memo.getStats(projectGroup), Optional.of(stats));
        assertEquals(memo.getCost(projectGroup), Optional.of(cost));
    }

    // Helper methods for creating test nodes
    private TableScanNode createTableScanNode()
    {
        TableHandle tableHandle = new TableHandle(
                new ConnectorId("test"),
                new TestingMetadata.TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());

        return new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                ImmutableList.of(), // output variables
                ImmutableMap.of(), // assignments
                TupleDomain.all(), // current constraint
                TupleDomain.all(), // enforced constraint
                Optional.empty()); // cte materialization info
    }

    private ProjectNode createProjectNode(PlanNode source)
    {
        return new ProjectNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                Assignments.of(), // assignments
                ProjectNode.Locality.LOCAL);
    }

    private FilterNode createFilterNode(PlanNode source)
    {
        return new FilterNode(
                Optional.empty(),
                idAllocator.getNextId(),
                Optional.empty(), // stats equivalent plan node
                source,
                null); // predicate
    }

    private UnionNode createUnionNode(PlanNode... sources)
    {
        return new UnionNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.copyOf(sources),
                ImmutableList.of(), // output variables
                ImmutableMap.of()); // variable mapping
    }

    private JoinNode createJoinNode(PlanNode left, PlanNode right)
    {
        return new JoinNode(
                Optional.empty(),
                idAllocator.getNextId(),
                Optional.empty(), // stats equivalent plan node
                JoinType.INNER,
                left,
                right,
                ImmutableList.of(), // criteria
                ImmutableList.of(), // output variables
                Optional.empty(), // filter
                Optional.empty(), // left hash variable
                Optional.empty(), // right hash variable
                Optional.empty(), // distribution type
                ImmutableMap.of()); // dynamic filters
    }

    // Helper methods for navigating memo structure
    private int getChildGroup(Memo memo, int group)
    {
        PlanNode node = memo.getNode(group);
        GroupReference child = (GroupReference) node.getSources().get(0);
        return child.getGroupId();
    }

    private int getDeepestChildGroup(Memo memo, int group)
    {
        PlanNode node = memo.getNode(group);
        if (node.getSources().isEmpty()) {
            return group;
        }
        GroupReference child = (GroupReference) node.getSources().get(0);
        return getDeepestChildGroup(memo, child.getGroupId());
    }
}
