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
package com.facebook.presto.sql.planner.iterative.rule.materializedview;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ChangedRowsPredicate;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDifferentialPlanRewriter
{
    private static final String CATALOG = "local";
    private static final String SCHEMA = "tiny";
    // The rewriter keys stale constraints on the SchemaTableName the connector reports for a scan,
    // not on the name used to address the table. TPCH canonicalizes the "tiny" schema to its scale
    // factor, so constraints keyed on "tiny.orders" would never bind and every delta would collapse
    // to Filter(FALSE). setUp asserts these constants still match what the connector resolves.
    private static final SchemaTableName ORDERS_TABLE = new SchemaTableName("sf0.01", "orders");
    private static final SchemaTableName CUSTOMER_TABLE = new SchemaTableName("sf0.01", "customer");

    private LocalQueryRunner queryRunner;
    private Metadata metadata;
    private Session session;
    private PlanNodeIdAllocator idAllocator;
    private VariableAllocator variableAllocator;
    private Lookup lookup;

    @BeforeClass
    public void setUp()
    {
        Session baseSession = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
        queryRunner = new LocalQueryRunner(baseSession);
        queryRunner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());
        metadata = queryRunner.getMetadata();
        // Create a session with a transaction for metadata access
        session = baseSession.beginTransactionId(
                queryRunner.getTransactionManager().beginTransaction(false),
                queryRunner.getTransactionManager(),
                queryRunner.getAccessControl());
        idAllocator = new PlanNodeIdAllocator();
        variableAllocator = new VariableAllocator();
        lookup = Lookup.noLookup();

        assertEquals(resolveTableName("orders"), ORDERS_TABLE, "ORDERS_TABLE must match the name the connector reports, or stale constraints will not bind");
        assertEquals(resolveTableName("customer"), CUSTOMER_TABLE, "CUSTOMER_TABLE must match the name the connector reports, or stale constraints will not bind");
    }

    private SchemaTableName resolveTableName(String table)
    {
        QualifiedObjectName name = QualifiedObjectName.valueOf(CATALOG + "." + SCHEMA + "." + table);
        TableHandle handle = metadata.getHandleVersion(session, name, Optional.empty())
                .orElseThrow(() -> new IllegalStateException("Table not found: " + name));
        return metadata.getTableMetadata(session, handle).getTable();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testTableScanDeltaStructure()
    {
        // Given: A TableScan with stale partition predicate on 'orderdate' column
        TableScanNode tableScan = createOrdersTableScan();
        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))));

        PassthroughColumnEquivalences columnEquivalences = createSimplePassthroughColumnEquivalences(ORDERS_TABLE, "orderdate");

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called
        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                createIdentityMapping(tableScan.getOutputVariables());
        DifferentialPlanRewriter.NodeWithMapping result = builder.buildDeltaPlan(tableScan, identityMapping);

        // Then: Result should be a FilterNode (stale predicate) over TableScan
        assertNotNull(result);
        assertNotNull(result.getNode());
        assertTrue(result.getNode() instanceof FilterNode, "Delta should be FilterNode, got: " + result.getNode().getClass().getSimpleName());

        FilterNode filterNode = (FilterNode) result.getNode();
        assertTrue(filterNode.getSource() instanceof TableScanNode, "Filter source should be TableScan");
    }

    @Test
    public void testJoinDeltaProducesUnion()
    {
        // Given: Join of two TableScans, both with stale partitions
        TableScanNode orders = createOrdersTableScan();
        TableScanNode customers = createCustomerTableScan();
        JoinNode join = createInnerJoin(orders, customers);

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))),
                CUSTOMER_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "mktsegment", Domain.singleValue(VARCHAR, utf8Slice("BUILDING"))))));

        PassthroughColumnEquivalences columnEquivalences = createJoinPassthroughColumnEquivalences();

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called
        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                createIdentityMapping(join.getOutputVariables());
        DifferentialPlanRewriter.NodeWithMapping result = builder.buildDeltaPlan(join, identityMapping);

        // Then: Result should be UnionNode (∆R ⋈ S') ∪ (R ⋈ ∆S)
        assertNotNull(result);
        assertTrue(result.getNode() instanceof UnionNode, "Join delta should be UnionNode, got: " + result.getNode().getClass().getSimpleName());

        UnionNode unionNode = (UnionNode) result.getNode();
        assertEquals(unionNode.getSources().size(), 2, "Union should have 2 sources");
        assertTrue(unionNode.getSources().get(0) instanceof JoinNode, "First union source should be JoinNode");
        assertTrue(unionNode.getSources().get(1) instanceof JoinNode, "Second union source should be JoinNode");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*Outer joins not supported.*")
    public void testOuterJoinThrowsException()
    {
        // Given: LEFT JOIN
        TableScanNode orders = createOrdersTableScan();
        TableScanNode customers = createCustomerTableScan();
        JoinNode leftJoin = createJoin(orders, customers, LEFT);

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(TupleDomain.all()));

        PassthroughColumnEquivalences columnEquivalences = createJoinPassthroughColumnEquivalences();

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called
        // Then: UnsupportedOperationException is thrown
        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                createIdentityMapping(leftJoin.getOutputVariables());
        builder.buildDeltaPlan(leftJoin, identityMapping);
    }

    @Test
    public void testUnionDeltaIsUnionOfDeltas()
    {
        // Given: Union of two TableScans of same table
        TableScanNode orders1 = createOrdersTableScan();
        TableScanNode orders2 = createOrdersTableScan();
        UnionNode unionNode = createUnion(orders1, orders2);

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))));

        PassthroughColumnEquivalences columnEquivalences = createSimplePassthroughColumnEquivalences(ORDERS_TABLE, "orderdate");

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called
        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                createIdentityMapping(unionNode.getOutputVariables());
        DifferentialPlanRewriter.NodeWithMapping result = builder.buildDeltaPlan(unionNode, identityMapping);

        // Then: Result should be UnionNode (∆R ∪ ∆S)
        assertNotNull(result);
        assertTrue(result.getNode() instanceof UnionNode, "Union delta should be UnionNode");

        UnionNode resultUnion = (UnionNode) result.getNode();
        assertEquals(resultUnion.getSources().size(), 2, "Delta union should have 2 sources");
    }

    @Test
    public void testMappingsAreComposedCorrectly()
    {
        // Given: A simple TableScan with known variables
        TableScanNode tableScan = createOrdersTableScan();

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))));

        PassthroughColumnEquivalences columnEquivalences = createSimplePassthroughColumnEquivalences(ORDERS_TABLE, "orderdate");

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called with a non-identity mapping
        VariableReferenceExpression mvOutputVar = variableAllocator.newVariable("mv_orderkey", BIGINT);
        VariableReferenceExpression viewQueryVar = tableScan.getOutputVariables().get(0);
        Map<VariableReferenceExpression, VariableReferenceExpression> viewQueryMapping =
                ImmutableMap.of(mvOutputVar, viewQueryVar);

        DifferentialPlanRewriter.NodeWithMapping result = builder.buildDeltaPlan(tableScan, viewQueryMapping);

        // Then: The result mapping should map mvOutputVar to the delta variable
        assertNotNull(result.getMapping());
        assertTrue(result.getMapping().containsKey(mvOutputVar), "Result mapping should contain MV output variable");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*Sort cannot be differentially stitched.*")
    public void testSortThrowsException()
    {
        // Given: Sort over TableScan
        TableScanNode tableScan = createOrdersTableScan();
        SortNode sortNode = createSort(tableScan);

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))));

        PassthroughColumnEquivalences columnEquivalences = createSimplePassthroughColumnEquivalences(ORDERS_TABLE, "orderdate");

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called
        // Then: UnsupportedOperationException is thrown
        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                createIdentityMapping(sortNode.getOutputVariables());
        builder.buildDeltaPlan(sortNode, identityMapping);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*Limit cannot be differentially stitched.*")
    public void testLimitThrowsException()
    {
        // Given: Limit over TableScan
        TableScanNode tableScan = createOrdersTableScan();
        LimitNode limitNode = createLimit(tableScan, 10);

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))));

        PassthroughColumnEquivalences columnEquivalences = createSimplePassthroughColumnEquivalences(ORDERS_TABLE, "orderdate");

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called
        // Then: UnsupportedOperationException is thrown
        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                createIdentityMapping(limitNode.getOutputVariables());
        builder.buildDeltaPlan(limitNode, identityMapping);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*TopN cannot be differentially stitched.*")
    public void testTopNThrowsException()
    {
        // Given: TopN over TableScan
        TableScanNode tableScan = createOrdersTableScan();
        TopNNode topNNode = createTopN(tableScan, 10);

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))));

        PassthroughColumnEquivalences columnEquivalences = createSimplePassthroughColumnEquivalences(ORDERS_TABLE, "orderdate");

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called
        // Then: UnsupportedOperationException is thrown
        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                createIdentityMapping(topNNode.getOutputVariables());
        builder.buildDeltaPlan(topNNode, identityMapping);
    }

    @Test
    public void testExceptDeltaRightUsesUnchanged()
    {
        TableScanNode orders = createOrdersTableScan();
        TableScanNode customers = createCustomerTableScan();
        ExceptNode exceptNode = createExcept(orders, customers);

        // Both tables have stale partitions
        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))),
                CUSTOMER_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "mktsegment", Domain.singleValue(VARCHAR, utf8Slice("BUILDING"))))));

        PassthroughColumnEquivalences columnEquivalences = createJoinPassthroughColumnEquivalences();

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                columnEquivalences,
                lookup,
                WarningCollector.NOOP);

        // When: buildDeltaPlan is called
        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                createIdentityMapping(exceptNode.getOutputVariables());
        DifferentialPlanRewriter.NodeWithMapping result = builder.buildDeltaPlan(exceptNode, identityMapping);

        // Then: Result should be UnionNode (deltaLeft ∪ deltaRight)
        assertNotNull(result);
        assertTrue(result.getNode() instanceof UnionNode, "Except delta should be UnionNode, got: " + result.getNode().getClass().getSimpleName());

        UnionNode unionNode = (UnionNode) result.getNode();
        assertEquals(unionNode.getSources().size(), 2, "Union should have 2 sources");

        // Both union sources should be ExceptNodes
        assertTrue(unionNode.getSources().get(0) instanceof ExceptNode, "First union source (deltaLeft) should be ExceptNode");
        assertTrue(unionNode.getSources().get(1) instanceof ExceptNode, "Second union source (deltaRight) should be ExceptNode");

        // deltaRight: The left side of the EXCEPT should use R (unchanged), not R' (current)
        // This means: Filter[S's stale predicate] -> Filter[NOT R's stale predicate] -> TableScan
        ExceptNode deltaRight = (ExceptNode) unionNode.getSources().get(1);
        PlanNode deltaRightLeftSource = deltaRight.getSources().get(0);

        // The left source should be: Filter[S's stale predicate] -> Filter[NOT R's stale predicate] -> TableScan
        assertTrue(deltaRightLeftSource instanceof FilterNode, "deltaRight left source should be FilterNode");
        FilterNode outerFilter = (FilterNode) deltaRightLeftSource;

        // The source of the outer filter should be another Filter (the unchanged filter for R)
        assertTrue(outerFilter.getSource() instanceof FilterNode,
                "deltaRight should use R (unchanged - Filter -> TableScan), not R' (current - TableScan directly). " +
                "This prevents double-counting when R and S share stale partitions. " +
                "Got: " + outerFilter.getSource().getClass().getSimpleName());

        // Verify the inner filter wraps a TableScan
        FilterNode innerFilter = (FilterNode) outerFilter.getSource();
        assertTrue(innerFilter.getSource() instanceof TableScanNode,
                "Inner filter should wrap TableScan. Got: " + innerFilter.getSource().getClass().getSimpleName());
    }

    @Test
    public void testAggregationExpandsAffectedGroupsWhenStaleBoundaryIsNotGrouped()
    {
        TableScanNode orders = createOrdersTableScan();
        AggregationNode aggregation = new AggregationNode(
                Optional.empty(),
                idAllocator.getNextId(),
                orders,
                ImmutableMap.of(),
                new AggregationNode.GroupingSetDescriptor(
                        ImmutableList.of(orders.getOutputVariables().get(0)),
                        1,
                        ImmutableSet.of()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))));

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                createSimplePassthroughColumnEquivalences(ORDERS_TABLE, "orderdate"),
                lookup,
                WarningCollector.NOOP);

        DifferentialPlanRewriter.NodeWithMapping result = builder.buildDeltaPlan(aggregation, createIdentityMapping(aggregation.getOutputVariables()));

        assertTrue(result.getNode() instanceof AggregationNode);
        PlanNode expandedSource = ((AggregationNode) result.getNode()).getSource();
        assertTrue(expandedSource instanceof ProjectNode, "Case B should project the current rows after matching affected groups");
        assertTrue(((ProjectNode) expandedSource).getSource() instanceof JoinNode,
                "Case B should match current rows to the affected groups");
    }

    @Test
    public void testPartitionLevelFreshBranchFiltersStorageColumns()
    {
        // A partition-level stale predicate maps to the storage table's own columns, so the fresh
        // branch excludes affected rows with a filter the connector can prune partitions from.
        TableScanNode storage = createCustomerTableScan();
        TableScanNode base = createOrdersTableScanWithCustkey();

        Optional<PlanNode> stitched = stitch(storage, base, ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of("custkey", Domain.singleValue(BIGINT, 1L)))));

        assertTrue(stitched.isPresent(), "expected a stitched plan");
        UnionNode union = (UnionNode) stitched.get();

        FilterNode fresh = (FilterNode) union.getSources().get(0);
        assertEquals(fresh.getSource(), storage, "the filter applies directly to the storage scan");
        assertTrue(fresh.getPredicate().toString().contains("custkey"),
                "the fresh branch is excluded by the storage column the stale column maps to, got: " + fresh.getPredicate());

        // The correctness property this shape carries: excluding by predicate does not depend on a
        // base row surviving to identify the partition. An anti-join against the base would leave
        // the stale storage rows of a fully deleted partition in the fresh branch, because nothing
        // would be left to match them against.
        assertTrue(searchFrom(union.getSources().get(0), lookup).where(JoinNode.class::isInstance).findAll().isEmpty(),
                "the partition-level fresh branch must not depend on scanning the base table");
    }

    @Test
    public void testStitchingDeclinedWhenStaleColumnHasNoStorageEquivalent()
    {
        // orderstatus is stale but maps to no storage column, so there is no anti-join key and the
        // fresh branch cannot be constrained. Stitching must decline rather than emit a plan whose
        // two branches overlap.
        assertFalse(stitch(
                createCustomerTableScan(),
                createOrdersTableScanWithCustkey(),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "orderstatus", Domain.singleValue(VARCHAR, utf8Slice("O")))))).isPresent());
    }

    private Optional<PlanNode> stitch(TableScanNode storage, TableScanNode base, List<TupleDomain<String>> staleDisjuncts)
    {
        VariableReferenceExpression output = variableAllocator.newVariable("mv_key", BIGINT);
        MaterializedViewScanNode node = new MaterializedViewScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                storage,
                base,
                QualifiedObjectName.valueOf(CATALOG + "." + SCHEMA + ".test_mv"),
                ImmutableMap.of(output, storage.getOutputVariables().get(0)),
                ImmutableMap.of(output, base.getOutputVariables().get(0)),
                ImmutableList.of(output));

        return DifferentialPlanRewriter.buildStitchedPlan(
                metadata,
                session,
                node,
                ImmutableMap.of(ORDERS_TABLE, new MaterializedViewStatus.MaterializedDataPredicates(staleDisjuncts, ImmutableList.of())),
                customerBackedMaterializedView(),
                variableAllocator,
                idAllocator,
                lookup,
                WarningCollector.NOOP);
    }

    /**
     * A single-base materialized view stored in customer, so the storage scan's reported table name
     * matches the definition's data table and custkey is a passthrough identifier.
     */
    private MaterializedViewDefinition customerBackedMaterializedView()
    {
        return new MaterializedViewDefinition(
                "SELECT custkey FROM orders",
                CUSTOMER_TABLE.getSchemaName(),
                CUSTOMER_TABLE.getTableName(),
                ImmutableList.of(ORDERS_TABLE),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new MaterializedViewDefinition.ColumnMapping(
                        new MaterializedViewDefinition.TableColumn(CUSTOMER_TABLE, "custkey"),
                        ImmutableList.of(new MaterializedViewDefinition.TableColumn(ORDERS_TABLE, "custkey")))),
                ImmutableList.of(),
                Optional.empty());
    }

    private TableScanNode createOrdersTableScanWithCustkey()
    {
        QualifiedObjectName tableName = QualifiedObjectName.valueOf(CATALOG + "." + SCHEMA + ".orders");
        TableHandle tableHandle = metadata.getHandleVersion(session, tableName, Optional.empty())
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        VariableReferenceExpression orderkey = variableAllocator.newVariable("orderkey", BIGINT);
        VariableReferenceExpression custkey = variableAllocator.newVariable("custkey", BIGINT);

        return new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                ImmutableList.of(orderkey, custkey),
                ImmutableMap.of(orderkey, columnHandles.get("orderkey"), custkey, columnHandles.get("custkey")),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }

    /**
     * A change set that is not additions only must decline, not stitch.
     *
     * <p>The disjuncts cannot tell a modified row from an appended one -- a modified row is still
     * present and still matches them -- so reusing the materialized rows would keep a value that
     * should have been replaced. Only the connector can assert the range holds no modifications,
     * which it does through {@code additionsOnly}; without it there is nothing safe to reuse, and
     * the partition-level candidate standing beside this one is both correct and cheaper.
     *
     * <p>Recomputing the affected groups from the base instead would be correct, and is what this
     * test used to assert. That path was withdrawn because it reads the whole base: the affected
     * keys are only known once the delta has been read, so nothing can prune the scan, and it
     * measured as more expensive than recomputing the view outright.
     */
    @Test
    public void testRowLevelCandidateDeclinesWhenNotAdditionsOnly()
    {
        // An aggregating MV grouped on custkey, whose connector reports changed rows through a
        // predicate on orderkey -- a column the view query does not project, standing in for
        // Iceberg's _last_updated_sequence_number.
        TableScanNode storage = createCustomerTableScan();
        TableScanNode base = createOrdersTableScanWithCustkey();
        VariableReferenceExpression custkey = base.getOutputVariables().get(1);
        AggregationNode viewQuery = new AggregationNode(
                Optional.empty(),
                idAllocator.getNextId(),
                base,
                ImmutableMap.of(),
                new AggregationNode.GroupingSetDescriptor(ImmutableList.of(custkey), 1, ImmutableSet.of()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        ColumnHandle orderkeyHandle = metadata.getColumnHandles(session, base.getTable()).get("orderkey");
        Map<SchemaTableName, ChangedRowsPredicate> changedRows = ImmutableMap.of(
                ORDERS_TABLE,
                new ChangedRowsPredicate(
                        ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                                orderkeyHandle, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 100L)), false)))),
                        TupleDomain.all()));

        VariableReferenceExpression output = variableAllocator.newVariable("mv_key", BIGINT);
        MaterializedViewScanNode node = new MaterializedViewScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                storage,
                viewQuery,
                QualifiedObjectName.valueOf(CATALOG + "." + SCHEMA + ".test_mv"),
                ImmutableMap.of(output, storage.getOutputVariables().get(0)),
                ImmutableMap.of(output, custkey),
                ImmutableList.of(output));

        Optional<PlanNode> stitched = DifferentialPlanRewriter.buildStitchedPlan(
                metadata,
                session,
                node,
                ImmutableMap.of(ORDERS_TABLE, new MaterializedViewStatus.MaterializedDataPredicates(
                        ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of("custkey", Domain.singleValue(BIGINT, 1L)))),
                        ImmutableList.of())),
                changedRows,
                customerBackedMaterializedView(),
                variableAllocator,
                idAllocator,
                lookup,
                WarningCollector.NOOP);

        assertFalse(stitched.isPresent(),
                "a change set that is not additions only must decline rather than stitch");
    }

    /**
     * The same view and the same row-level boundary, with the one difference that the connector
     * reports the range as additions only -- and that alone replaces the expansion with a merge.
     *
     * <p>SUM over an append-only range is a function of the stored value and the delta, so the delta
     * aggregates the appended rows by themselves and re-aggregating the union adds each group's
     * increment onto what storage holds. The expansion that
     * {@code testRowLevelCandidateDeclinesWhenNotAdditionsOnly} covers exists only because that range is
     * not known to be additions only; this pins the other side of that branch so neither can be
     * changed without the other failing.
     *
     * <p>The two assertions that matter are the ones about what is absent: no anti-join on the fresh
     * branch, because a merge consumes exactly the stored values an anti-join would discard, and no
     * join in the delta branch, because the increment never needs its groups' other rows.
     */
    @Test
    public void testRowLevelCandidateMergesAdditionsOnlyRange()
    {
        TableScanNode storage = createCustomerTableScan();
        TableScanNode base = createOrdersTableScanWithCustkey();
        VariableReferenceExpression custkey = base.getOutputVariables().get(1);
        VariableReferenceExpression total = variableAllocator.newVariable("total", BIGINT);

        FunctionHandle sum = metadata.getFunctionAndTypeManager().lookupFunction("sum", fromTypes(BIGINT));
        AggregationNode viewQuery = new AggregationNode(
                Optional.empty(),
                idAllocator.getNextId(),
                base,
                ImmutableMap.of(total, new AggregationNode.Aggregation(
                        new CallExpression("sum", sum, BIGINT, ImmutableList.of(base.getOutputVariables().get(0))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                new AggregationNode.GroupingSetDescriptor(ImmutableList.of(custkey), 1, ImmutableSet.of()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        ColumnHandle orderkeyHandle = metadata.getColumnHandles(session, base.getTable()).get("orderkey");
        Map<SchemaTableName, ChangedRowsPredicate> changedRows = ImmutableMap.of(
                ORDERS_TABLE,
                new ChangedRowsPredicate(
                        ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                                orderkeyHandle, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 100L)), false)))),
                        TupleDomain.all(),
                        true));

        VariableReferenceExpression keyOutput = variableAllocator.newVariable("mv_key", BIGINT);
        VariableReferenceExpression totalOutput = variableAllocator.newVariable("mv_total", BIGINT);
        MaterializedViewScanNode node = new MaterializedViewScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                storage,
                viewQuery,
                QualifiedObjectName.valueOf(CATALOG + "." + SCHEMA + ".test_mv"),
                ImmutableMap.of(keyOutput, storage.getOutputVariables().get(0), totalOutput, storage.getOutputVariables().get(1)),
                ImmutableMap.of(keyOutput, custkey, totalOutput, total),
                ImmutableList.of(keyOutput, totalOutput));

        Optional<PlanNode> stitched = DifferentialPlanRewriter.buildStitchedPlan(
                metadata,
                session,
                node,
                ImmutableMap.of(ORDERS_TABLE, new MaterializedViewStatus.MaterializedDataPredicates(
                        ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of("custkey", Domain.singleValue(BIGINT, 1L)))),
                        ImmutableList.of())),
                changedRows,
                customerBackedMaterializedView(),
                variableAllocator,
                idAllocator,
                lookup,
                WarningCollector.NOOP);

        assertTrue(stitched.isPresent(), "expected a row-level stitched plan");

        // The merge renames its aggregates back to the view's outputs, so the root is a projection
        // over the merging aggregation over the union.
        // Located by type rather than by a fixed cast chain: the merge sits above the union with a
        // rename projection above it, and a chain breaks whenever a node is inserted between them.
        AggregationNode merge = (AggregationNode) searchFrom(stitched.get(), lookup)
                .where(AggregationNode.class::isInstance)
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected a merging aggregation, got:\n" + stitched.get()));
        assertEquals(merge.getGroupingKeys(), ImmutableList.of(keyOutput), "the merge groups by the view's grouping key");
        assertEquals(merge.getAggregations().size(), 1, "one aggregate to merge");
        assertTrue(merge.getAggregations().values().iterator().next().getCall().getDisplayName().contains("sum"),
                "SUM merges by being summed, got: " + merge.getAggregations().values().iterator().next().getCall());

        UnionNode union = (UnionNode) searchFrom(merge, lookup)
                .where(UnionNode.class::isInstance)
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected the merge to sit over a union, got:\n" + merge));
        assertFalse(searchFrom(union.getSources().get(0), lookup).where(JoinNode.class::isInstance).findFirst().isPresent(),
                "a merged stitch must keep every storage row, so the fresh branch must not anti-join");
        assertFalse(searchFrom(union.getSources().get(1), lookup).where(JoinNode.class::isInstance).findFirst().isPresent(),
                "the delta is an increment, so it must not be expanded back to the affected groups");
    }

    /**
     * The shape classification decides whether row-level can identify a materialized row at all, so
     * each verdict is pinned directly rather than only through the plans built on top of it.
     *
     * <p>The global-aggregate case is the one that was wrong: counting only grouping aggregations
     * left {@code SELECT SUM(x) FROM t} looking indistinguishable from a view with no aggregation,
     * so it was claimed as row-preserving. Row-preserving is a promise that one output row
     * corresponds to one base row, and a global aggregate breaks it in the direction that duplicates
     * output -- the stale row stays and the delta's partial aggregate joins it.
     */
    @Test
    public void testClassifyViewShape()
    {
        TableScanNode base = createOrdersTableScanWithCustkey();
        VariableReferenceExpression amount = base.getOutputVariables().get(0);
        VariableReferenceExpression custkey = base.getOutputVariables().get(1);

        assertEquals(DifferentialPlanRewriter.classifyViewShape(base, lookup), DifferentialPlanRewriter.ViewShape.ROW_PRESERVING,
                "a scan with no aggregation is one output row per base row");

        AggregationNode grouped = aggregation(base, ImmutableList.of(custkey), amount);
        assertEquals(DifferentialPlanRewriter.classifyViewShape(grouped, lookup), DifferentialPlanRewriter.ViewShape.AGGREGATING,
                "one grouping aggregation names its rows by its grouping keys");

        AggregationNode global = aggregation(base, ImmutableList.of(), amount);
        assertEquals(DifferentialPlanRewriter.classifyViewShape(global, lookup), DifferentialPlanRewriter.ViewShape.UNSUPPORTED,
                "a global aggregate has no grouping key to name its single row by");

        assertEquals(DifferentialPlanRewriter.classifyViewShape(aggregation(grouped, ImmutableList.of(custkey), amount), lookup),
                DifferentialPlanRewriter.ViewShape.UNSUPPORTED,
                "stacked aggregations leave the materialized row's identity ambiguous");
    }

    private AggregationNode aggregation(PlanNode source, List<VariableReferenceExpression> groupingKeys, VariableReferenceExpression input)
    {
        FunctionHandle sum = metadata.getFunctionAndTypeManager().lookupFunction("sum", fromTypes(BIGINT));
        return new AggregationNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                ImmutableMap.of(variableAllocator.newVariable("agg", BIGINT), new AggregationNode.Aggregation(
                        new CallExpression("sum", sum, BIGINT, ImmutableList.of(input)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                new AggregationNode.GroupingSetDescriptor(groupingKeys, 1, groupingKeys.isEmpty() ? ImmutableSet.of(0) : ImmutableSet.of()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testAggregationTakesCaseAWhenStaleBoundaryIsAGroupingColumn()
    {
        // The stale boundary is orderdate and the view groups by it, so every group is wholly
        // inside or wholly outside the delta and the aggregation can be applied to the delta
        // directly. A projection renames orderdate on the way up, which is what previously
        // discarded the closure and forced the expansion for every plan with a projection.
        TableScanNode orders = createOrdersTableScan();
        VariableReferenceExpression orderdate = orders.getOutputVariables().get(1);
        VariableReferenceExpression renamed = variableAllocator.newVariable("ds", VARCHAR);

        ProjectNode rename = new ProjectNode(
                idAllocator.getNextId(),
                orders,
                Assignments.builder().put(renamed, orderdate).build());

        AggregationNode aggregation = new AggregationNode(
                Optional.empty(),
                idAllocator.getNextId(),
                rename,
                ImmutableMap.of(),
                new AggregationNode.GroupingSetDescriptor(ImmutableList.of(renamed), 1, ImmutableSet.of()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints = ImmutableMap.of(
                ORDERS_TABLE, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                "orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))))));

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                staleConstraints,
                createSimplePassthroughColumnEquivalences(ORDERS_TABLE, "orderdate"),
                lookup,
                WarningCollector.NOOP);

        DifferentialPlanRewriter.NodeWithMapping result =
                builder.buildDeltaPlan(aggregation, createIdentityMapping(aggregation.getOutputVariables()));

        assertTrue(result.getNode() instanceof AggregationNode);
        PlanNode source = ((AggregationNode) result.getNode()).getSource();
        assertTrue(source instanceof ProjectNode, "expected the view's own projection, got: " + source.getClass().getSimpleName());
        // Case B would put a join here to match the current base against the affected groups.
        assertFalse(((ProjectNode) source).getSource() instanceof JoinNode,
                "Case A should aggregate the delta directly, not expand affected groups");
        assertTrue(((ProjectNode) source).getSource() instanceof FilterNode,
                "the delta is the stale-filtered base scan, got: " + ((ProjectNode) source).getSource().getClass().getSimpleName());
    }

    // Helper methods

    private TableScanNode createOrdersTableScan()
    {
        QualifiedObjectName tableName = QualifiedObjectName.valueOf(CATALOG + ".tiny.orders");
        TableHandle tableHandle = metadata.getHandleVersion(session, tableName, Optional.empty())
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle orderkeyHandle = columnHandles.get("orderkey");
        ColumnHandle orderdateHandle = columnHandles.get("orderdate");

        VariableReferenceExpression orderkey = variableAllocator.newVariable("orderkey", BIGINT);
        VariableReferenceExpression orderdate = variableAllocator.newVariable("orderdate", VARCHAR);

        return new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                ImmutableList.of(orderkey, orderdate),
                ImmutableMap.of(orderkey, orderkeyHandle, orderdate, orderdateHandle),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }

    private TableScanNode createCustomerTableScan()
    {
        QualifiedObjectName tableName = QualifiedObjectName.valueOf(CATALOG + ".tiny.customer");
        TableHandle tableHandle = metadata.getHandleVersion(session, tableName, Optional.empty())
                .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle custkeyHandle = columnHandles.get("custkey");
        ColumnHandle mktsegmentHandle = columnHandles.get("mktsegment");

        VariableReferenceExpression custkey = variableAllocator.newVariable("custkey", BIGINT);
        VariableReferenceExpression mktsegment = variableAllocator.newVariable("mktsegment", VARCHAR);

        return new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                ImmutableList.of(custkey, mktsegment),
                ImmutableMap.of(custkey, custkeyHandle, mktsegment, mktsegmentHandle),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }

    private JoinNode createInnerJoin(TableScanNode left, TableScanNode right)
    {
        return createJoin(left, right, INNER);
    }

    private JoinNode createJoin(TableScanNode left, TableScanNode right, JoinType joinType)
    {
        VariableReferenceExpression leftJoinKey = left.getOutputVariables().get(0);
        VariableReferenceExpression rightJoinKey = right.getOutputVariables().get(0);

        ImmutableList.Builder<VariableReferenceExpression> outputVariables = ImmutableList.builder();
        outputVariables.addAll(left.getOutputVariables());
        outputVariables.addAll(right.getOutputVariables());

        return new JoinNode(
                Optional.empty(),
                idAllocator.getNextId(),
                joinType,
                left,
                right,
                ImmutableList.of(new EquiJoinClause(leftJoinKey, rightJoinKey)),
                outputVariables.build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private ExceptNode createExcept(TableScanNode left, TableScanNode right)
    {
        ImmutableList.Builder<VariableReferenceExpression> outputVariables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> variableMapping = ImmutableMap.builder();

        // Use left source's output variables as output schema
        for (VariableReferenceExpression outputVar : left.getOutputVariables()) {
            VariableReferenceExpression exceptOutput = variableAllocator.newVariable(outputVar.getName() + "_except", outputVar.getType());
            outputVariables.add(exceptOutput);
            variableMapping.put(exceptOutput, ImmutableList.of(
                    outputVar,
                    right.getOutputVariables().get(left.getOutputVariables().indexOf(outputVar))));
        }

        return new ExceptNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(left, right),
                outputVariables.build(),
                variableMapping.build());
    }

    private UnionNode createUnion(TableScanNode... sources)
    {
        ImmutableList.Builder<PlanNode> sourceNodes = ImmutableList.builder();
        ImmutableList.Builder<VariableReferenceExpression> outputVariables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> variableMapping = ImmutableMap.builder();

        // Use first source's output variables as output schema
        for (VariableReferenceExpression outputVar : sources[0].getOutputVariables()) {
            VariableReferenceExpression unionOutput = variableAllocator.newVariable(outputVar.getName() + "_union", outputVar.getType());
            outputVariables.add(unionOutput);

            ImmutableList.Builder<VariableReferenceExpression> sourceVars = ImmutableList.builder();
            for (int i = 0; i < sources.length; i++) {
                sourceVars.add(sources[i].getOutputVariables().get(sources[0].getOutputVariables().indexOf(outputVar)));
            }
            variableMapping.put(unionOutput, sourceVars.build());
        }

        for (TableScanNode source : sources) {
            sourceNodes.add(source);
        }

        return new UnionNode(
                Optional.empty(),
                idAllocator.getNextId(),
                sourceNodes.build(),
                outputVariables.build(),
                variableMapping.build());
    }

    private Map<VariableReferenceExpression, VariableReferenceExpression> createIdentityMapping(
            List<VariableReferenceExpression> variables)
    {
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> mapping = ImmutableMap.builder();
        for (VariableReferenceExpression variable : variables) {
            mapping.put(variable, variable);
        }
        return mapping.build();
    }

    private PassthroughColumnEquivalences createSimplePassthroughColumnEquivalences(SchemaTableName table, String partitionColumn)
    {
        SchemaTableName dataTable = new SchemaTableName("schema", "__mv_storage__test_mv");
        MaterializedViewDefinition mvDefinition = new MaterializedViewDefinition(
                "SELECT * FROM " + table.getTableName(),
                dataTable.getSchemaName(),
                dataTable.getTableName(),
                ImmutableList.of(table),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new MaterializedViewDefinition.ColumnMapping(
                                new MaterializedViewDefinition.TableColumn(dataTable, partitionColumn),
                                ImmutableList.of(new MaterializedViewDefinition.TableColumn(table, partitionColumn)))),
                ImmutableList.of(),
                Optional.empty());

        return new PassthroughColumnEquivalences(mvDefinition, dataTable);
    }

    private PassthroughColumnEquivalences createJoinPassthroughColumnEquivalences()
    {
        SchemaTableName dataTable = new SchemaTableName("schema", "__mv_storage__test_mv");
        MaterializedViewDefinition mvDefinition = new MaterializedViewDefinition(
                "SELECT * FROM orders JOIN customer ON orders.orderdate = customer.mktsegment",
                dataTable.getSchemaName(),
                dataTable.getTableName(),
                ImmutableList.of(ORDERS_TABLE, CUSTOMER_TABLE),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new MaterializedViewDefinition.ColumnMapping(
                                new MaterializedViewDefinition.TableColumn(dataTable, "orderdate"),
                                ImmutableList.of(new MaterializedViewDefinition.TableColumn(ORDERS_TABLE, "orderdate"))),
                        new MaterializedViewDefinition.ColumnMapping(
                                new MaterializedViewDefinition.TableColumn(dataTable, "mktsegment"),
                                ImmutableList.of(new MaterializedViewDefinition.TableColumn(CUSTOMER_TABLE, "mktsegment")))),
                ImmutableList.of(),
                Optional.empty());

        return new PassthroughColumnEquivalences(mvDefinition, dataTable);
    }

    private SortNode createSort(PlanNode source)
    {
        VariableReferenceExpression sortKey = source.getOutputVariables().get(0);
        OrderingScheme orderingScheme = new OrderingScheme(
                ImmutableList.of(new Ordering(sortKey, SortOrder.ASC_NULLS_FIRST)));

        return new SortNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                orderingScheme,
                false,
                ImmutableList.of());
    }

    private LimitNode createLimit(PlanNode source, long count)
    {
        return new LimitNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                count,
                LimitNode.Step.FINAL);
    }

    private TopNNode createTopN(PlanNode source, long count)
    {
        VariableReferenceExpression sortKey = source.getOutputVariables().get(0);
        OrderingScheme orderingScheme = new OrderingScheme(
                ImmutableList.of(new Ordering(sortKey, SortOrder.ASC_NULLS_FIRST)));

        return new TopNNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                count,
                orderingScheme,
                TopNNode.Step.SINGLE);
    }
}
