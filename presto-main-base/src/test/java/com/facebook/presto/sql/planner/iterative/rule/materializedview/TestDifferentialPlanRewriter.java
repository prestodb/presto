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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDifferentialPlanRewriter
{
    private static final String CATALOG = "local";
    private static final SchemaTableName ORDERS_TABLE = new SchemaTableName("tiny", "orders");
    private static final SchemaTableName CUSTOMER_TABLE = new SchemaTableName("tiny", "customer");

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
                .setSchema("tiny")
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
