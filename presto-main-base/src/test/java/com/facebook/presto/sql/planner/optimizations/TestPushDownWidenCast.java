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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PUSH_DOWN_WIDEN_CAST_ENABLED;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link PushDownWidenCast}.
 */
public class TestPushDownWidenCast
        extends BasePlanTest
{
    private Metadata metadata;
    private PlanBuilder builder;
    private TableHandle ordersTableHandle;
    private TableHandle lineitemTableHandle;
    private TpchColumnHandle shippriorityColumnHandle;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        builder = new PlanBuilder(getQueryRunner().getDefaultSession(), new PlanNodeIdAllocator(), metadata);
        ConnectorId connectorId = getCurrentConnectorId();
        ordersTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle("orders", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());
        lineitemTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle("lineitem", 1.0),
                TestingTransactionHandle.create(),
                Optional.empty());
        shippriorityColumnHandle = new TpchColumnHandle("shippriority", INTEGER);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private CallExpression castExpr(VariableReferenceExpression input, com.facebook.presto.common.type.Type toType)
    {
        FunctionHandle handle = metadata.getFunctionAndTypeManager().lookupCast(CastType.CAST, input.getType(), toType);
        return new CallExpression("CAST", handle, toType, ImmutableList.of(input));
    }

    private PlanNode runOptimizer(PlanNode plan, Session session)
    {
        // Collect variables directly from the plan we're optimizing — keeps us independent of
        // the test class's shared builder (which can race with other parallel tests).
        Set<VariableReferenceExpression> planVars = new java.util.HashSet<>();
        collectVariables(plan, planVars);
        ImmutableList<VariableReferenceExpression> snapshot = ImmutableList.copyOf(planVars);
        return getQueryRunner().inTransaction(session, s -> {
            s.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(s, catalog));
            VariableAllocator allocator = new VariableAllocator(snapshot);
            return new PushDownWidenCast(metadata)
                    .optimize(plan, s, TypeProvider.empty(), allocator, new PlanNodeIdAllocator(), WarningCollector.NOOP)
                    .getPlanNode();
        });
    }

    private static void collectVariables(PlanNode node, Set<VariableReferenceExpression> out)
    {
        out.addAll(node.getOutputVariables());
        for (PlanNode child : node.getSources()) {
            collectVariables(child, out);
        }
    }

    private Session sessionWithOptimizerEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSH_DOWN_WIDEN_CAST_ENABLED, "true")
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .build();
    }

    /** Build a scan of orders with shippriority (INTEGER) and an optional extra BIGINT column. */
    private TableScanNode ordersScan(VariableReferenceExpression... vars)
    {
        ImmutableMap.Builder<VariableReferenceExpression, com.facebook.presto.spi.ColumnHandle> assignments = ImmutableMap.builder();
        for (VariableReferenceExpression v : vars) {
            assignments.put(v, new TpchColumnHandle(v.getName(), v.getType()));
        }
        return builder.tableScan(ordersTableHandle, ImmutableList.copyOf(vars), assignments.build());
    }

    // -----------------------------------------------------------------------
    // Project -> TableScan  (direct pattern)
    // -----------------------------------------------------------------------

    @Test
    public void testBasicIntegerToBigintPushedDown()
    {
        VariableReferenceExpression narrowVar = builder.variable("shippriority", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("expr", BIGINT);

        TableScanNode scan = builder.tableScan(
                ordersTableHandle,
                ImmutableList.of(narrowVar),
                ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        PlanNode result = runOptimizer(
                builder.project(Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), scan),
                sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) result;
        assertEquals(rp.getAssignments().get(wideVar), wideVar);

        TableScanNode rs = (TableScanNode) rp.getSource();
        assertEquals(rs.getOutputVariables(), ImmutableList.of(wideVar));
        assertEquals(rs.getAssignments().get(wideVar), shippriorityColumnHandle);
    }

    @Test
    public void testRealToDoublePushedDown()
    {
        TpchColumnHandle realCol = new TpchColumnHandle("realcol", REAL);
        VariableReferenceExpression narrowVar = builder.variable("realcol", REAL);
        VariableReferenceExpression wideVar = builder.variable("doublecol", DOUBLE);

        TableScanNode scan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, realCol));

        PlanNode result = runOptimizer(
                builder.project(Assignments.of(wideVar, castExpr(narrowVar, DOUBLE)), scan),
                sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideVar), wideVar);
        assertEquals(((TableScanNode) ((ProjectNode) result).getSource()).getAssignments().get(wideVar), realCol);
    }

    @Test
    public void testSmallintToBigintPushedDown()
    {
        TpchColumnHandle smallCol = new TpchColumnHandle("smallcol", SMALLINT);
        VariableReferenceExpression smallVar = builder.variable("smallcol", SMALLINT);
        VariableReferenceExpression bigVar = builder.variable("bigcol", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(smallVar), ImmutableMap.of(smallVar, smallCol));

        PlanNode result = runOptimizer(
                builder.project(Assignments.of(bigVar, castExpr(smallVar, BIGINT)), scan),
                sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(bigVar), bigVar);
    }

    @Test
    public void testNarrowingCastIsNotPushedDown()
    {
        TpchColumnHandle bigintCol = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression bigintVar = builder.variable("orderkey_nd", BIGINT);
        VariableReferenceExpression intVar = builder.variable("narrowed_nd", INTEGER);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(bigintVar), ImmutableMap.of(bigintVar, bigintCol));

        PlanNode result = runOptimizer(
                builder.project(Assignments.of(intVar, castExpr(bigintVar, INTEGER)), scan),
                sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        assertTrue(((ProjectNode) result).getAssignments().get(intVar) instanceof CallExpression,
                "narrowing cast should NOT be pushed down");
    }

    @Test
    public void testNarrowVarUsedElsewherePushedViaAdd()
    {
        // Project has BOTH `wide := CAST(narrow AS BIGINT)` AND `pass := narrow`. Phase 1 REPLACE
        // bails (narrow used in two assignments), but Phase 2 ADD picks it up: a new Project is
        // injected above the scan that computes wide := CAST(narrow AS BIGINT); the upper Project
        // rewrites the original CAST to a bare wide reference. narrow stays available for `pass`.
        VariableReferenceExpression narrowVar = builder.variable("shippriority_ue", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("expr_ue", BIGINT);
        VariableReferenceExpression passVar = builder.variable("pass_ue", INTEGER);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        Assignments a = Assignments.builder()
                .put(wideVar, castExpr(narrowVar, BIGINT))
                .put(passVar, narrowVar)
                .build();

        PlanNode result = runOptimizer(builder.project(a, scan), sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) result;
        // Phase 2 allocates a FRESH wideVar (not the original `expr_ue` declared by the test); the
        // upper Project's `expr_ue := CAST(narrow)` gets rewritten to `expr_ue := <freshWide>`,
        // and the fresh wide var is computed by the new Project injected above the scan.
        RowExpression upperWideAssignment = rp.getAssignments().get(wideVar);
        assertTrue(upperWideAssignment instanceof VariableReferenceExpression,
                "upper Project's wideVar assignment should be a bare ref to the fresh wideVar");
        VariableReferenceExpression freshWide = (VariableReferenceExpression) upperWideAssignment;
        assertEquals(freshWide.getType(), BIGINT);
        // passthrough unchanged
        assertEquals(rp.getAssignments().get(passVar), narrowVar);

        // Below sits the synthetic Project that ADD injected, computing freshWide := CAST(narrow).
        assertTrue(rp.getSource() instanceof ProjectNode);
        ProjectNode lower = (ProjectNode) rp.getSource();
        RowExpression freshWideAssignment = lower.getAssignments().get(freshWide);
        assertTrue(freshWideAssignment instanceof CallExpression
                && ((CallExpression) freshWideAssignment).getDisplayName().equals("CAST")
                && ((CallExpression) freshWideAssignment).getArguments().get(0).equals(narrowVar));
        // Scan is untouched — narrow is still the only variable mapped to the column.
        assertTrue(lower.getSource() instanceof TableScanNode);
        TableScanNode rScan = (TableScanNode) lower.getSource();
        assertEquals(rScan.getAssignments().get(narrowVar), shippriorityColumnHandle);
        assertEquals(rScan.getAssignments().size(), 1);
    }

    @Test
    public void testOptimizerDisabledIsNoOp()
    {
        VariableReferenceExpression narrowVar = builder.variable("shippriority_dis", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("expr_dis", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));
        ProjectNode project = builder.project(Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), scan);

        PlanNode result = runOptimizer(project, getQueryRunner().getDefaultSession());
        assertSame(result, project, "plan should be unchanged when optimizer is disabled");
    }

    @Test
    public void testMultipleEligibleCastsAllPushedDown()
    {
        TpchColumnHandle colA = new TpchColumnHandle("shippriority", INTEGER);
        TpchColumnHandle colB = new TpchColumnHandle("smallcol", SMALLINT);
        VariableReferenceExpression narrowA = builder.variable("a_narrow_m", INTEGER);
        VariableReferenceExpression narrowB = builder.variable("b_narrow_m", SMALLINT);
        VariableReferenceExpression wideA = builder.variable("a_wide_m", BIGINT);
        VariableReferenceExpression wideB = builder.variable("b_wide_m", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowA, narrowB), ImmutableMap.of(narrowA, colA, narrowB, colB));

        Assignments a = Assignments.builder()
                .put(wideA, castExpr(narrowA, BIGINT))
                .put(wideB, castExpr(narrowB, BIGINT))
                .build();

        PlanNode result = runOptimizer(builder.project(a, scan), sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) result;
        assertEquals(rp.getAssignments().get(wideA), wideA);
        assertEquals(rp.getAssignments().get(wideB), wideB);

        TableScanNode rs = (TableScanNode) rp.getSource();
        assertEquals(rs.getAssignments().get(wideA), colA);
        assertEquals(rs.getAssignments().get(wideB), colB);
    }

    // -----------------------------------------------------------------------
    // Project -> FilterNode -> TableScan
    // -----------------------------------------------------------------------

    @Test
    public void testWideningCastPushedThroughFilter()
    {
        VariableReferenceExpression keyVar = builder.variable("orderkey_f", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("shippriority_f", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_f", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(keyVar, narrowVar),
                ImmutableMap.of(
                        keyVar, new TpchColumnHandle("orderkey", BIGINT),
                        narrowVar, shippriorityColumnHandle));

        // Filter on keyVar — does NOT reference narrowVar
        FilterNode filter = builder.filter(builder.rowExpression("orderkey_f > 0"), scan);
        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), filter);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // Cast should have been pushed through the FilterNode to the TableScan
        assertTrue(result instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) result;
        assertEquals(rp.getAssignments().get(wideVar), wideVar);

        // Filter is still there, but its source is the updated TableScan
        assertTrue(rp.getSource() instanceof FilterNode);
        FilterNode rf = (FilterNode) rp.getSource();
        assertTrue(rf.getSource() instanceof TableScanNode);
        TableScanNode rs = (TableScanNode) rf.getSource();
        assertEquals(rs.getAssignments().get(wideVar), shippriorityColumnHandle);
    }

    @Test
    public void testFilterReferencingNarrowVarFallsBackToAdd()
    {
        // Filter references narrowVar → Phase 1 REPLACE bails (would orphan the predicate).
        // Phase 2 ADD picks up the slack: injects a Project above the scan that computes
        // wide := CAST(narrow), leaves the scan + filter untouched, and rewrites the upper
        // Project's CAST to a bare reference to the fresh wideVar.
        VariableReferenceExpression narrowVar = builder.variable("shippriority_fb", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_fb", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        FilterNode filter = builder.filter(builder.rowExpression("shippriority_fb > 0"), scan);
        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), filter);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // Upper Project: wideVar's CAST replaced by a bare reference to the fresh wide var.
        ProjectNode rp = (ProjectNode) result;
        assertTrue(rp.getAssignments().get(wideVar) instanceof VariableReferenceExpression);
        VariableReferenceExpression freshWide = (VariableReferenceExpression) rp.getAssignments().get(wideVar);
        // Filter still there, predicate still references narrow.
        FilterNode rf = (FilterNode) rp.getSource();
        // Below the filter, a new Project was injected that computes freshWide := CAST(narrow).
        ProjectNode lower = (ProjectNode) rf.getSource();
        RowExpression freshWideAssignment = lower.getAssignments().get(freshWide);
        assertTrue(freshWideAssignment instanceof CallExpression
                && ((CallExpression) freshWideAssignment).getDisplayName().equals("CAST")
                && ((CallExpression) freshWideAssignment).getArguments().get(0).equals(narrowVar));
        // Scan untouched.
        TableScanNode rScan = (TableScanNode) lower.getSource();
        assertEquals(rScan.getAssignments().size(), 1);
        assertEquals(rScan.getAssignments().get(narrowVar), shippriorityColumnHandle);
    }

    // -----------------------------------------------------------------------
    // Project -> LimitNode -> TableScan
    // -----------------------------------------------------------------------

    @Test
    public void testWideningCastPushedThroughLimit()
    {
        VariableReferenceExpression narrowVar = builder.variable("shippriority_l", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_l", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        LimitNode limit = new LimitNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                scan,
                100,
                LimitNode.Step.FINAL);

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), limit);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideVar), wideVar);

        assertTrue(((ProjectNode) result).getSource() instanceof LimitNode);
        LimitNode rl = (LimitNode) ((ProjectNode) result).getSource();
        assertTrue(rl.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rl.getSource()).getAssignments().get(wideVar), shippriorityColumnHandle);
    }

    // -----------------------------------------------------------------------
    // Project -> SortNode -> TableScan
    // -----------------------------------------------------------------------

    @Test
    public void testWideningCastPushedThroughSort()
    {
        VariableReferenceExpression keyVar = builder.variable("orderkey_s", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("shippriority_s", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_s", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(keyVar, narrowVar),
                ImmutableMap.of(
                        keyVar, new TpchColumnHandle("orderkey", BIGINT),
                        narrowVar, shippriorityColumnHandle));

        // Sort by keyVar — does NOT use narrowVar
        OrderingScheme ordering = new OrderingScheme(
                ImmutableList.of(new Ordering(keyVar, ASC_NULLS_FIRST)));
        SortNode sort = new SortNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                scan,
                ordering,
                false,
                ImmutableList.of());

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), sort);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideVar), wideVar);

        assertTrue(((ProjectNode) result).getSource() instanceof SortNode);
        SortNode rs = (SortNode) ((ProjectNode) result).getSource();
        assertTrue(rs.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rs.getSource()).getAssignments().get(wideVar), shippriorityColumnHandle);
    }

    @Test
    public void testSortKeyIsNarrowVarFallsBackToAdd()
    {
        // Sort orders by narrowVar → Phase 1 REPLACE bails (would change sort semantics).
        // Phase 2 ADD pushes: synthetic Project above scan computes wide := CAST(narrow); the
        // sort still operates on narrow.
        VariableReferenceExpression narrowVar = builder.variable("shippriority_sk", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_sk", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        OrderingScheme ordering = new OrderingScheme(
                ImmutableList.of(new Ordering(narrowVar, ASC_NULLS_FIRST)));
        SortNode sort = new SortNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                scan,
                ordering,
                false,
                ImmutableList.of());

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), sort);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        ProjectNode rp = (ProjectNode) result;
        assertTrue(rp.getAssignments().get(wideVar) instanceof VariableReferenceExpression);
        VariableReferenceExpression freshWide = (VariableReferenceExpression) rp.getAssignments().get(wideVar);
        SortNode rs = (SortNode) rp.getSource();
        // Sort still orders by narrow.
        assertTrue(rs.getOrderingScheme().getOrderByVariables().contains(narrowVar));
        // New Project below Sort produces freshWide := CAST(narrow).
        ProjectNode lower = (ProjectNode) rs.getSource();
        RowExpression freshWideAssignment = lower.getAssignments().get(freshWide);
        assertTrue(freshWideAssignment instanceof CallExpression
                && ((CallExpression) freshWideAssignment).getDisplayName().equals("CAST")
                && ((CallExpression) freshWideAssignment).getArguments().get(0).equals(narrowVar));
        // Scan untouched.
        TableScanNode rScan = (TableScanNode) lower.getSource();
        assertEquals(rScan.getAssignments().get(narrowVar), shippriorityColumnHandle);
    }

    // -----------------------------------------------------------------------
    // Project -> TopNNode -> TableScan
    // -----------------------------------------------------------------------

    @Test
    public void testWideningCastPushedThroughTopN()
    {
        VariableReferenceExpression keyVar = builder.variable("orderkey_tn", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("shippriority_tn", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_tn", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(keyVar, narrowVar),
                ImmutableMap.of(
                        keyVar, new TpchColumnHandle("orderkey", BIGINT),
                        narrowVar, shippriorityColumnHandle));

        OrderingScheme ordering = new OrderingScheme(
                ImmutableList.of(new Ordering(keyVar, ASC_NULLS_FIRST)));
        TopNNode topN = new TopNNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                scan,
                10,
                ordering,
                TopNNode.Step.SINGLE);

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), topN);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideVar), wideVar);

        assertTrue(((ProjectNode) result).getSource() instanceof TopNNode);
        TopNNode rtn = (TopNNode) ((ProjectNode) result).getSource();
        assertTrue(rtn.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rtn.getSource()).getAssignments().get(wideVar), shippriorityColumnHandle);
    }

    // -----------------------------------------------------------------------
    // Project -> (intermediate) ProjectNode -> TableScan
    // -----------------------------------------------------------------------

    @Test
    public void testWideningCastPushedThroughIntermediateIdentityProject()
    {
        VariableReferenceExpression narrowVar = builder.variable("shippriority_ip", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_ip", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        // Intermediate project that just passes narrowVar through (identity)
        ProjectNode innerProject = builder.project(
                Assignments.of(narrowVar, narrowVar), scan);

        ProjectNode outerProject = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), innerProject);

        PlanNode result = runOptimizer(outerProject, sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) result;
        assertEquals(rp.getAssignments().get(wideVar), wideVar);

        // The inner project should now pass wideVar through (identity)
        assertTrue(rp.getSource() instanceof ProjectNode);
        ProjectNode ri = (ProjectNode) rp.getSource();
        assertEquals(ri.getAssignments().get(wideVar), wideVar,
                "inner project identity assignment should be updated to wideVar");

        assertTrue(ri.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) ri.getSource()).getAssignments().get(wideVar), shippriorityColumnHandle);
    }

    @Test
    public void testIntermediateProjectComputingNarrowVarPreventsPushDown()
    {
        VariableReferenceExpression rawVar = builder.variable("raw_ip", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("shippriority_ipc", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_ipc", BIGINT);

        // Inner project COMPUTES narrowVar from rawVar (not a passthrough)
        TpchColumnHandle rawCol = new TpchColumnHandle("orderkey", BIGINT);
        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(rawVar), ImmutableMap.of(rawVar, rawCol));

        FunctionHandle castToInt = metadata.getFunctionAndTypeManager().lookupCast(CastType.CAST, BIGINT, INTEGER);
        CallExpression computeNarrow = new CallExpression("CAST", castToInt, INTEGER, ImmutableList.of(rawVar));
        ProjectNode innerProject = builder.project(Assignments.of(narrowVar, computeNarrow), scan);

        ProjectNode outerProject = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), innerProject);

        PlanNode result = runOptimizer(outerProject, sessionWithOptimizerEnabled());

        // outer CAST should NOT be pushed through the inner project that computes narrowVar
        assertTrue(result instanceof ProjectNode);
        assertTrue(((ProjectNode) result).getAssignments().get(wideVar) instanceof CallExpression,
                "cast should not be pushed when inner project computes the narrow variable");
    }

    // -----------------------------------------------------------------------
    // Project -> JoinNode -> TableScan
    // -----------------------------------------------------------------------

    @Test
    public void testWideningCastPushedThroughJoinLeftSide()
    {
        TpchColumnHandle ordersKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression ordersKeyVar = builder.variable("o_orderkey", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("o_shippriority", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_shippriority", BIGINT);

        TableScanNode leftScan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(ordersKeyVar, narrowVar),
                ImmutableMap.of(ordersKeyVar, ordersKey, narrowVar, shippriorityColumnHandle));

        TpchColumnHandle lineitemKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression lineitemKeyVar = builder.variable("l_orderkey", BIGINT);

        TableScanNode rightScan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(lineitemKeyVar),
                ImmutableMap.of(lineitemKeyVar, lineitemKey));

        JoinNode join = new JoinNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new EquiJoinClause(ordersKeyVar, lineitemKeyVar)),
                ImmutableList.of(ordersKeyVar, narrowVar, lineitemKeyVar), // left vars first
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), join);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) result;
        assertEquals(rp.getAssignments().get(wideVar), wideVar);

        assertTrue(rp.getSource() instanceof JoinNode);
        JoinNode rj = (JoinNode) rp.getSource();
        assertTrue(rj.getOutputVariables().contains(wideVar));
        assertTrue(!rj.getOutputVariables().contains(narrowVar));

        assertTrue(rj.getLeft() instanceof TableScanNode);
        assertEquals(((TableScanNode) rj.getLeft()).getAssignments().get(wideVar), shippriorityColumnHandle);
    }

    @Test
    public void testWideningCastPushedThroughJoinRightSide()
    {
        TpchColumnHandle ordersKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression ordersKeyVar = builder.variable("o_orderkey_rs", BIGINT);

        TableScanNode leftScan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(ordersKeyVar),
                ImmutableMap.of(ordersKeyVar, ordersKey));

        TpchColumnHandle lineitemKey = new TpchColumnHandle("orderkey", BIGINT);
        TpchColumnHandle linenumberCol = new TpchColumnHandle("linenumber", INTEGER);
        VariableReferenceExpression lineitemKeyVar = builder.variable("l_orderkey_rs", BIGINT);
        VariableReferenceExpression linenumberVar = builder.variable("l_linenumber_rs", INTEGER);
        VariableReferenceExpression wideLinenumberVar = builder.variable("wide_linenumber_rs", BIGINT);

        TableScanNode rightScan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(lineitemKeyVar, linenumberVar),
                ImmutableMap.of(lineitemKeyVar, lineitemKey, linenumberVar, linenumberCol));

        JoinNode join = new JoinNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new EquiJoinClause(ordersKeyVar, lineitemKeyVar)),
                ImmutableList.of(ordersKeyVar, lineitemKeyVar, linenumberVar),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        ProjectNode project = builder.project(
                Assignments.of(wideLinenumberVar, castExpr(linenumberVar, BIGINT)), join);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideLinenumberVar), wideLinenumberVar);

        JoinNode rj = (JoinNode) ((ProjectNode) result).getSource();
        assertTrue(rj.getOutputVariables().contains(wideLinenumberVar));
        assertTrue(!rj.getOutputVariables().contains(linenumberVar));
        assertEquals(((TableScanNode) rj.getRight()).getAssignments().get(wideLinenumberVar), linenumberCol);
    }

    @Test
    public void testJoinKeyVariableFallsBackToAdd()
    {
        // narrow is the join's left-side equi-clause variable → REPLACE bails (joins pin clause
        // variables). ADD pushes: new Project under the join's left side computes wide :=
        // CAST(narrow); the join keeps using narrow as its key.
        TpchColumnHandle ordersKey = new TpchColumnHandle("orderkey", BIGINT);
        TpchColumnHandle shippriorityCol = new TpchColumnHandle("shippriority", INTEGER);
        VariableReferenceExpression ordersKeyVar = builder.variable("o_orderkey_jk", BIGINT);
        VariableReferenceExpression shippriorityVar = builder.variable("o_shippriority_jk", INTEGER);
        VariableReferenceExpression widePriorityVar = builder.variable("wide_priority_jk", BIGINT);

        TableScanNode leftScan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(ordersKeyVar, shippriorityVar),
                ImmutableMap.of(ordersKeyVar, ordersKey, shippriorityVar, shippriorityCol));

        TpchColumnHandle lineitemKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression lineitemKeyVar = builder.variable("l_orderkey_jk", BIGINT);

        TableScanNode rightScan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(lineitemKeyVar),
                ImmutableMap.of(lineitemKeyVar, lineitemKey));

        JoinNode join = new JoinNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new EquiJoinClause(shippriorityVar, lineitemKeyVar)),
                ImmutableList.of(ordersKeyVar, shippriorityVar, lineitemKeyVar),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        PlanNode result = runOptimizer(
                builder.project(Assignments.of(widePriorityVar, castExpr(shippriorityVar, BIGINT)), join),
                sessionWithOptimizerEnabled());

        ProjectNode rp = (ProjectNode) result;
        // CAST replaced with a bare wide-var reference.
        assertTrue(rp.getAssignments().get(widePriorityVar) instanceof VariableReferenceExpression);
        // Join still uses shippriorityVar as the equi-clause key (untouched).
        JoinNode rj = (JoinNode) rp.getSource();
        assertEquals(rj.getCriteria().get(0).getLeft(), shippriorityVar);
        // Below the join's left side: a new Project that computes the wide var from narrow.
        assertTrue(rj.getLeft() instanceof ProjectNode);
    }

    @Test
    public void testJoinFilterVariableFallsBackToAdd()
    {
        // narrow appears in the join's residual filter → REPLACE bails. ADD pushes; the join's
        // filter still references narrow.
        TpchColumnHandle ordersKey = new TpchColumnHandle("orderkey", BIGINT);
        TpchColumnHandle shippriorityCol = new TpchColumnHandle("shippriority", INTEGER);
        VariableReferenceExpression ordersKeyVar = builder.variable("o_orderkey_jf", BIGINT);
        VariableReferenceExpression shippriorityVar = builder.variable("o_shippriority_jf", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_jf", BIGINT);

        TableScanNode leftScan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(ordersKeyVar, shippriorityVar),
                ImmutableMap.of(ordersKeyVar, ordersKey, shippriorityVar, shippriorityCol));

        TpchColumnHandle lineitemKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression lineitemKeyVar = builder.variable("l_orderkey_jf", BIGINT);

        TableScanNode rightScan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(lineitemKeyVar),
                ImmutableMap.of(lineitemKeyVar, lineitemKey));

        JoinNode join = new JoinNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new EquiJoinClause(ordersKeyVar, lineitemKeyVar)),
                ImmutableList.of(ordersKeyVar, shippriorityVar, lineitemKeyVar),
                Optional.of(builder.rowExpression("o_shippriority_jf > 0")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        PlanNode result = runOptimizer(
                builder.project(Assignments.of(wideVar, castExpr(shippriorityVar, BIGINT)), join),
                sessionWithOptimizerEnabled());

        ProjectNode rp = (ProjectNode) result;
        // CAST replaced with a bare wide-var reference.
        assertTrue(rp.getAssignments().get(wideVar) instanceof VariableReferenceExpression);
        // Join's residual filter unchanged.
        JoinNode rj = (JoinNode) rp.getSource();
        assertTrue(rj.getFilter().isPresent());
    }

    // -----------------------------------------------------------------------
    // Multiple intermediate nodes between ProjectNode and TableScanNode
    // -----------------------------------------------------------------------

    @Test
    public void testWideningCastPushedThroughFilterThenSort()
    {
        // Plan: Project(cast) -> Filter(key > 0) -> Sort(by key) -> TableScan
        // Semantics: SELECT CAST(shippriority AS BIGINT) FROM
        //            (SELECT * FROM orders ORDER BY orderkey) WHERE orderkey > 0
        VariableReferenceExpression keyVar = builder.variable("orderkey_fts", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("shippriority_fts", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_fts", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(keyVar, narrowVar),
                ImmutableMap.of(
                        keyVar, new TpchColumnHandle("orderkey", BIGINT),
                        narrowVar, shippriorityColumnHandle));

        OrderingScheme sortByKey = new OrderingScheme(
                ImmutableList.of(new Ordering(keyVar, ASC_NULLS_FIRST)));
        SortNode sort = new SortNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                scan,
                sortByKey,
                false,
                ImmutableList.of());

        // Filter on keyVar — does NOT reference narrowVar
        FilterNode filter = builder.filter(builder.rowExpression("orderkey_fts > 0"), sort);

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), filter);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // Cast is pushed all the way through Filter and Sort to the TableScan
        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideVar), wideVar);

        assertTrue(((ProjectNode) result).getSource() instanceof FilterNode);
        FilterNode rf = (FilterNode) ((ProjectNode) result).getSource();

        assertTrue(rf.getSource() instanceof SortNode);
        SortNode rs = (SortNode) rf.getSource();
        // Sort ordering key (keyVar) is unchanged
        assertEquals(rs.getOrderingScheme().getOrderByVariables(), ImmutableList.of(keyVar));

        assertTrue(rs.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rs.getSource()).getAssignments().get(wideVar),
                shippriorityColumnHandle);
    }

    @Test
    public void testWideningCastPushedThroughLimitThenFilter()
    {
        // Plan: Project(cast) -> Limit(50) -> Filter(key > 0) -> TableScan
        // Semantics: SELECT CAST(shippriority AS BIGINT) FROM
        //            (SELECT * FROM orders WHERE orderkey > 0 LIMIT 50)
        VariableReferenceExpression keyVar = builder.variable("orderkey_ltf", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("shippriority_ltf", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_ltf", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(keyVar, narrowVar),
                ImmutableMap.of(
                        keyVar, new TpchColumnHandle("orderkey", BIGINT),
                        narrowVar, shippriorityColumnHandle));

        // Filter on keyVar — does NOT reference narrowVar
        FilterNode filter = builder.filter(builder.rowExpression("orderkey_ltf > 0"), scan);

        LimitNode limit = new LimitNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                filter,
                50,
                LimitNode.Step.FINAL);

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), limit);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // Cast is pushed through Limit and Filter to the TableScan
        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideVar), wideVar);

        assertTrue(((ProjectNode) result).getSource() instanceof LimitNode);
        LimitNode rl = (LimitNode) ((ProjectNode) result).getSource();
        assertEquals(rl.getCount(), 50);

        assertTrue(rl.getSource() instanceof FilterNode);
        FilterNode rf = (FilterNode) rl.getSource();

        assertTrue(rf.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rf.getSource()).getAssignments().get(wideVar),
                shippriorityColumnHandle);
    }

    @Test
    public void testWideningCastPushedThroughTopNThenFilter()
    {
        // Plan: Project(cast) -> TopN(10 by key) -> Filter(key > 0) -> TableScan
        // Semantics: SELECT CAST(shippriority AS BIGINT) FROM
        //            (SELECT * FROM orders WHERE orderkey > 0 ORDER BY orderkey LIMIT 10)
        VariableReferenceExpression keyVar = builder.variable("orderkey_tnf", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("shippriority_tnf", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_tnf", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(keyVar, narrowVar),
                ImmutableMap.of(
                        keyVar, new TpchColumnHandle("orderkey", BIGINT),
                        narrowVar, shippriorityColumnHandle));

        // Filter on keyVar — does NOT reference narrowVar
        FilterNode filter = builder.filter(builder.rowExpression("orderkey_tnf > 0"), scan);

        OrderingScheme sortByKey = new OrderingScheme(
                ImmutableList.of(new Ordering(keyVar, ASC_NULLS_FIRST)));
        TopNNode topN = new TopNNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                filter,
                10,
                sortByKey,
                TopNNode.Step.SINGLE);

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), topN);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // Cast is pushed through TopN and Filter to the TableScan
        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideVar), wideVar);

        assertTrue(((ProjectNode) result).getSource() instanceof TopNNode);
        TopNNode rtn = (TopNNode) ((ProjectNode) result).getSource();
        assertEquals(rtn.getCount(), 10);
        // TopN ordering key (keyVar) is unchanged
        assertEquals(rtn.getOrderingScheme().getOrderByVariables(), ImmutableList.of(keyVar));

        assertTrue(rtn.getSource() instanceof FilterNode);
        FilterNode rf = (FilterNode) rtn.getSource();

        assertTrue(rf.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rf.getSource()).getAssignments().get(wideVar),
                shippriorityColumnHandle);
    }

    // -----------------------------------------------------------------------
    // PlanNodes above (parent of) the ProjectNode are not affected
    // -----------------------------------------------------------------------

    @Test
    public void testFilterAboveProjectIsUnaffected()
    {
        // Plan: Filter(wideVar > 0) -> Project(wideVar := CAST(narrowVar AS BIGINT)) -> TableScan
        // The optimizer should push the cast down, and leave the Filter structurally intact.
        VariableReferenceExpression narrowVar = builder.variable("shippriority_abvf", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_abvf", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar),
                ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), scan);

        // Filter on the project's output variable (wideVar)
        FilterNode filter = builder.filter(builder.rowExpression("wide_sp_abvf > 0"), project);

        PlanNode result = runOptimizer(filter, sessionWithOptimizerEnabled());

        // Top-level node is still a FilterNode (not removed or replaced)
        assertTrue(result instanceof FilterNode);
        FilterNode rf = (FilterNode) result;
        // Filter predicate is still a comparison call (wide_sp_abvf > 0), not a plain variable
        assertTrue(rf.getPredicate() instanceof CallExpression,
                "filter predicate should remain unchanged");

        // The project directly below has been optimized to an identity assignment
        assertTrue(rf.getSource() instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) rf.getSource();
        assertEquals(rp.getAssignments().get(wideVar), wideVar,
                "project assignment should be identity after cast push-down");

        // The scan now outputs wideVar (BIGINT) directly
        assertTrue(rp.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rp.getSource()).getAssignments().get(wideVar),
                shippriorityColumnHandle);
    }

    @Test
    public void testLimitAboveProjectIsUnaffected()
    {
        // Plan: Limit(10) -> Project(wideVar := CAST(narrowVar AS BIGINT)) -> TableScan
        // The optimizer should push the cast down, and leave the Limit structurally intact.
        VariableReferenceExpression narrowVar = builder.variable("shippriority_abvl", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_abvl", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar),
                ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), scan);

        LimitNode limit = new LimitNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                project,
                10,
                LimitNode.Step.FINAL);

        PlanNode result = runOptimizer(limit, sessionWithOptimizerEnabled());

        // Top-level node is still a LimitNode with the same count
        assertTrue(result instanceof LimitNode);
        assertEquals(((LimitNode) result).getCount(), 10);

        // The project directly below has been optimized to an identity assignment
        assertTrue(((LimitNode) result).getSource() instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) ((LimitNode) result).getSource();
        assertEquals(rp.getAssignments().get(wideVar), wideVar,
                "project assignment should be identity after cast push-down");

        // The scan now outputs wideVar (BIGINT) directly
        assertTrue(rp.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rp.getSource()).getAssignments().get(wideVar),
                shippriorityColumnHandle);
    }

    @Test
    public void testSortAboveProjectIsUnaffected()
    {
        // Plan: Sort(wideVar ASC) -> Project(wideVar := CAST(narrowVar AS BIGINT)) -> TableScan
        // The optimizer should push the cast down, and leave the Sort structurally intact.
        // The sort on wideVar (BIGINT) produces the same order as sorting CAST(INTEGER->BIGINT).
        VariableReferenceExpression narrowVar = builder.variable("shippriority_abvs", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_abvs", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar),
                ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), scan);

        // Sort on wideVar (the project output)
        OrderingScheme sortByWide = new OrderingScheme(
                ImmutableList.of(new Ordering(wideVar, ASC_NULLS_FIRST)));
        SortNode sort = new SortNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                project,
                sortByWide,
                false,
                ImmutableList.of());

        PlanNode result = runOptimizer(sort, sessionWithOptimizerEnabled());

        // Top-level node is still a SortNode with the same ordering on wideVar
        assertTrue(result instanceof SortNode);
        SortNode rs = (SortNode) result;
        assertEquals(rs.getOrderingScheme().getOrderByVariables(), ImmutableList.of(wideVar),
                "sort ordering should remain unchanged");

        // The project directly below has been optimized to an identity assignment
        assertTrue(rs.getSource() instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) rs.getSource();
        assertEquals(rp.getAssignments().get(wideVar), wideVar,
                "project assignment should be identity after cast push-down");

        // The scan now outputs wideVar (BIGINT) directly
        assertTrue(rp.getSource() instanceof TableScanNode);
        assertEquals(((TableScanNode) rp.getSource()).getAssignments().get(wideVar),
                shippriorityColumnHandle);
    }

    // -----------------------------------------------------------------------
    // Multi-level: Project -> Filter -> JoinNode -> TableScan
    // -----------------------------------------------------------------------

    @Test
    public void testWideningCastPushedThroughFilterAndJoin()
    {
        TpchColumnHandle ordersKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression ordersKeyVar = builder.variable("o_orderkey_fj", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("o_shippriority_fj", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_sp_fj", BIGINT);

        TableScanNode leftScan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(ordersKeyVar, narrowVar),
                ImmutableMap.of(ordersKeyVar, ordersKey, narrowVar, shippriorityColumnHandle));

        TpchColumnHandle lineitemKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression lineitemKeyVar = builder.variable("l_orderkey_fj", BIGINT);

        TableScanNode rightScan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(lineitemKeyVar),
                ImmutableMap.of(lineitemKeyVar, lineitemKey));

        JoinNode join = new JoinNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new EquiJoinClause(ordersKeyVar, lineitemKeyVar)),
                ImmutableList.of(ordersKeyVar, narrowVar, lineitemKeyVar), // left vars first
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        // Filter above the join does NOT reference narrowVar
        FilterNode filter = builder.filter(builder.rowExpression("o_orderkey_fj > 0"), join);
        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), filter);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // Expect: Project(identity) -> Filter -> Join -> [TableScan(wide), TableScan]
        assertTrue(result instanceof ProjectNode);
        assertEquals(((ProjectNode) result).getAssignments().get(wideVar), wideVar);

        assertTrue(((ProjectNode) result).getSource() instanceof FilterNode);
        FilterNode rf = (FilterNode) ((ProjectNode) result).getSource();

        assertTrue(rf.getSource() instanceof JoinNode);
        JoinNode rj = (JoinNode) rf.getSource();
        assertTrue(rj.getOutputVariables().contains(wideVar));
        assertTrue(!rj.getOutputVariables().contains(narrowVar));
        assertEquals(((TableScanNode) rj.getLeft()).getAssignments().get(wideVar), shippriorityColumnHandle);
    }

    @Test
    public void testWideningCastPushedThroughNestedJoins()
    {
        // Reproduces the production failure: a CAST sits above an outer join whose
        // left child is itself a join (e.g. a CTE-internal join), so the narrow
        // variable is produced by a TableScan two levels below the join the
        // optimizer first sees.  Pattern:
        //   Project(CAST(shippriority AS BIGINT))
        //     OuterJoin (on o_orderkey = lineitem_other_key)
        //       InnerJoin (on o_orderkey = l_orderkey)
        //         TableScan(orders: o_orderkey, shippriority)   -- narrowVar lives here
        //         TableScan(lineitem: l_orderkey)
        //       TableScan(lineitem: other_orderkey)
        TpchColumnHandle ordersKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression ordersKeyVar = builder.variable("o_orderkey_nj", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("o_shippriority_nj", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_shippriority_nj", BIGINT);

        TableScanNode ordersScan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(ordersKeyVar, narrowVar),
                ImmutableMap.of(ordersKeyVar, ordersKey, narrowVar, shippriorityColumnHandle));

        TpchColumnHandle lineitemKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression innerLineitemKeyVar = builder.variable("l_orderkey_nj", BIGINT);
        TableScanNode innerLineitemScan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(innerLineitemKeyVar),
                ImmutableMap.of(innerLineitemKeyVar, lineitemKey));

        JoinNode innerJoin = new JoinNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.LEFT,
                ordersScan,
                innerLineitemScan,
                ImmutableList.of(new EquiJoinClause(ordersKeyVar, innerLineitemKeyVar)),
                ImmutableList.of(ordersKeyVar, narrowVar, innerLineitemKeyVar),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        VariableReferenceExpression outerLineitemKeyVar = builder.variable("l_orderkey_nj_outer", BIGINT);
        TableScanNode outerLineitemScan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(outerLineitemKeyVar),
                ImmutableMap.of(outerLineitemKeyVar, lineitemKey));

        JoinNode outerJoin = new JoinNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.LEFT,
                innerJoin,
                outerLineitemScan,
                ImmutableList.of(new EquiJoinClause(ordersKeyVar, outerLineitemKeyVar)),
                ImmutableList.of(ordersKeyVar, narrowVar, innerLineitemKeyVar, outerLineitemKeyVar),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        ProjectNode project = builder.project(
                Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), outerJoin);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // Expect: identity assignment at the top, both joins now produce wideVar,
        // narrowVar replaced by wideVar all the way down to the orders TableScan.
        assertTrue(result instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) result;
        assertEquals(rp.getAssignments().get(wideVar), wideVar);

        assertTrue(rp.getSource() instanceof JoinNode);
        JoinNode rOuter = (JoinNode) rp.getSource();
        assertTrue(rOuter.getOutputVariables().contains(wideVar));
        assertTrue(!rOuter.getOutputVariables().contains(narrowVar));

        assertTrue(rOuter.getLeft() instanceof JoinNode);
        JoinNode rInner = (JoinNode) rOuter.getLeft();
        assertTrue(rInner.getOutputVariables().contains(wideVar));
        assertTrue(!rInner.getOutputVariables().contains(narrowVar));

        assertTrue(rInner.getLeft() instanceof TableScanNode);
        TableScanNode rScan = (TableScanNode) rInner.getLeft();
        assertEquals(rScan.getAssignments().get(wideVar), shippriorityColumnHandle);
        assertTrue(!rScan.getAssignments().containsKey(narrowVar));
    }

    // -----------------------------------------------------------------------
    // Subexpression CAST push (ADD semantics)
    // -----------------------------------------------------------------------

    @Test
    public void testSubexpressionCastInsideFunctionIsPushed()
    {
        // Plan: Project(out := abs(CAST(narrow AS BIGINT))) -> TableScan(narrow:INT)
        // narrow is used ONLY inside the CAST subexpression — safe to REPLACE narrow with wide
        // in the scan. Expect scan to produce only wide:BIGINT, and Project to be rewritten to
        // abs(wide).
        VariableReferenceExpression narrowVar = builder.variable("shippriority_se", INTEGER);
        VariableReferenceExpression outVar = builder.variable("out_se", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        RowExpression wrapped = builder.rowExpression("abs(CAST(shippriority_se AS BIGINT))");
        ProjectNode project = builder.project(Assignments.of(outVar, wrapped), scan);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // Top is still a Project (cast rewritten, not removed since the wrapping function remains).
        assertTrue(result instanceof ProjectNode);
        ProjectNode rp = (ProjectNode) result;

        // RHS should now reference the wideVar instead of CAST(...).
        RowExpression newRhs = rp.getAssignments().get(outVar);
        assertTrue(newRhs instanceof CallExpression);
        CallExpression call = (CallExpression) newRhs;
        assertEquals(call.getDisplayName(), "abs");
        assertTrue(call.getArguments().get(0) instanceof VariableReferenceExpression,
                "CAST should be replaced with a bare wide variable reference");
        VariableReferenceExpression wideRef = (VariableReferenceExpression) call.getArguments().get(0);
        assertEquals(wideRef.getType(), BIGINT);

        // Source is a TableScan that now produces ONLY the wide variable for the shippriority column.
        assertTrue(rp.getSource() instanceof TableScanNode);
        TableScanNode rScan = (TableScanNode) rp.getSource();
        assertEquals(rScan.getAssignments().get(wideRef), shippriorityColumnHandle);
        assertTrue(!rScan.getAssignments().containsKey(narrowVar),
                "narrowVar must be replaced (REPLACE semantics); ADD-style would create a BiMap conflict downstream");
        assertTrue(rScan.getOutputVariables().contains(wideRef));
    }

    @Test
    public void testSubexpressionCastSharedAcrossAssignments()
    {
        // Two assignments both contain CAST(narrow AS BIGINT) — they must share the same wideVar.
        VariableReferenceExpression narrowVar = builder.variable("shippriority_sh", INTEGER);
        VariableReferenceExpression out1 = builder.variable("out1_sh", BIGINT);
        VariableReferenceExpression out2 = builder.variable("out2_sh", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        RowExpression e1 = builder.rowExpression("abs(CAST(shippriority_sh AS BIGINT))");
        RowExpression e2 = builder.rowExpression("abs(CAST(shippriority_sh AS BIGINT) + BIGINT '1')");
        Assignments a = Assignments.builder().put(out1, e1).put(out2, e2).build();
        ProjectNode project = builder.project(a, scan);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        ProjectNode rp = (ProjectNode) result;
        TableScanNode rScan = (TableScanNode) rp.getSource();

        // Exactly one variable in the scan maps to the shippriority column — the wide one.
        long entriesForColumn = rScan.getAssignments().entrySet().stream()
                .filter(e -> e.getValue().equals(shippriorityColumnHandle))
                .count();
        assertEquals(entriesForColumn, 1L, "narrowVar replaced; only the wide var maps to the column");
        assertTrue(!rScan.getAssignments().containsKey(narrowVar));

        // Both RHS expressions now reference that one wide var.
        VariableReferenceExpression wideArg1 = (VariableReferenceExpression) ((CallExpression) rp.getAssignments().get(out1)).getArguments().get(0);
        CallExpression absCall = (CallExpression) rp.getAssignments().get(out2);
        CallExpression plusCall = (CallExpression) absCall.getArguments().get(0);
        VariableReferenceExpression wideArg2 = (VariableReferenceExpression) plusCall.getArguments().get(0);
        assertEquals(wideArg1, wideArg2, "both subexpression casts should be rewritten to the same wideVar");
        assertEquals(rScan.getAssignments().get(wideArg1), shippriorityColumnHandle);
    }

    @Test
    public void testTopLevelCastUntouchedByAddPass()
    {
        // Sanity: a plain top-level CAST should be handled by the REPLACE pass (existing behaviour),
        // not by the new ADD pass.  After the rule, the scan should produce only wideVar (no narrow).
        VariableReferenceExpression narrowVar = builder.variable("shippriority_tl", INTEGER);
        VariableReferenceExpression wideVar = builder.variable("wide_tl", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        ProjectNode project = builder.project(Assignments.of(wideVar, castExpr(narrowVar, BIGINT)), scan);
        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        // REPLACE pass: scan now produces only wideVar, narrow is gone.
        ProjectNode rp = (ProjectNode) result;
        TableScanNode rScan = (TableScanNode) rp.getSource();
        assertTrue(rScan.getAssignments().containsKey(wideVar));
        assertTrue(!rScan.getAssignments().containsKey(narrowVar),
                "narrowVar must be removed when REPLACE handles a top-level CAST");
    }

    @Test
    public void testSubexpressionCastPushedThroughJoinLeftSide()
    {
        // Plan: Project(out := abs(CAST(narrow AS BIGINT))) -> Join -> TableScan(narrow:INT)
        // narrow is used only in the CAST — safe to REPLACE through the join into the scan.
        TpchColumnHandle ordersKey = new TpchColumnHandle("orderkey", BIGINT);
        VariableReferenceExpression ordersKeyVar = builder.variable("o_key_seJ", BIGINT);
        VariableReferenceExpression narrowVar = builder.variable("shippriority_seJ", INTEGER);
        VariableReferenceExpression outVar = builder.variable("out_seJ", BIGINT);

        TableScanNode leftScan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(ordersKeyVar, narrowVar),
                ImmutableMap.of(ordersKeyVar, ordersKey, narrowVar, shippriorityColumnHandle));

        VariableReferenceExpression rightKey = builder.variable("l_key_seJ", BIGINT);
        TpchColumnHandle lineitemKey = new TpchColumnHandle("orderkey", BIGINT);
        TableScanNode rightScan = builder.tableScan(lineitemTableHandle,
                ImmutableList.of(rightKey),
                ImmutableMap.of(rightKey, lineitemKey));

        JoinNode join = new JoinNode(
                Optional.empty(),
                new PlanNodeIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new EquiJoinClause(ordersKeyVar, rightKey)),
                ImmutableList.of(ordersKeyVar, narrowVar, rightKey),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        ProjectNode project = builder.project(
                Assignments.of(outVar, builder.rowExpression("abs(CAST(shippriority_seJ AS BIGINT))")),
                join);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        ProjectNode rp = (ProjectNode) result;
        JoinNode rj = (JoinNode) rp.getSource();
        // wideVar replaced narrow throughout: join outputs contain wide, not narrow.
        VariableReferenceExpression wide = (VariableReferenceExpression)
                ((CallExpression) rp.getAssignments().get(outVar)).getArguments().get(0);
        assertTrue(rj.getOutputVariables().contains(wide));
        assertTrue(!rj.getOutputVariables().contains(narrowVar));

        TableScanNode rLeftScan = (TableScanNode) rj.getLeft();
        assertEquals(rLeftScan.getAssignments().get(wide), shippriorityColumnHandle);
        assertTrue(!rLeftScan.getAssignments().containsKey(narrowVar));
    }

    @Test
    public void testSubexpressionCastAddedWhenNarrowVarUsedElsewhereInProject()
    {
        // narrow is used both as a passthrough AND inside a CAST. The rule cannot REPLACE
        // (narrow needed elsewhere), so it ADDs wide alongside narrow at the scan. The
        // four downstream BiMap call-sites are patched to tolerate duplicate ColumnHandles.
        VariableReferenceExpression narrowVar = builder.variable("shippriority_mu", INTEGER);
        VariableReferenceExpression passVar = builder.variable("pass_mu", INTEGER);
        VariableReferenceExpression outVar = builder.variable("out_mu", BIGINT);

        TableScanNode scan = builder.tableScan(ordersTableHandle,
                ImmutableList.of(narrowVar), ImmutableMap.of(narrowVar, shippriorityColumnHandle));

        Assignments a = Assignments.builder()
                .put(passVar, narrowVar)
                .put(outVar, builder.rowExpression("abs(CAST(shippriority_mu AS BIGINT))"))
                .build();
        ProjectNode project = builder.project(a, scan);

        PlanNode result = runOptimizer(project, sessionWithOptimizerEnabled());

        ProjectNode rp = (ProjectNode) result;
        // The rule injects a Project ABOVE the scan that computes wide := CAST(narrow AS T);
        // the upper Project's RHS is rewritten to abs(wide).
        VariableReferenceExpression wide = (VariableReferenceExpression)
                ((CallExpression) rp.getAssignments().get(outVar)).getArguments().get(0);
        assertEquals(wide.getType(), BIGINT);
        // Passthrough still references narrow
        assertEquals(rp.getAssignments().get(passVar), narrowVar);

        // Below the upper Project sits the freshly inserted Project (wide := CAST(narrow))
        assertTrue(rp.getSource() instanceof ProjectNode);
        ProjectNode lowerProject = (ProjectNode) rp.getSource();
        RowExpression wideAssignment = lowerProject.getAssignments().get(wide);
        assertTrue(wideAssignment instanceof CallExpression
                && ((CallExpression) wideAssignment).getDisplayName().equals("CAST")
                && ((CallExpression) wideAssignment).getArguments().get(0).equals(narrowVar),
                "lower Project should compute wide := CAST(narrow AS BIGINT)");

        // Scan is untouched — narrow remains the only variable mapped to the column.
        assertTrue(lowerProject.getSource() instanceof TableScanNode);
        TableScanNode rScan = (TableScanNode) lowerProject.getSource();
        assertEquals(rScan.getAssignments().get(narrowVar), shippriorityColumnHandle);
        assertEquals(rScan.getAssignments().size(), 1);
    }
}
