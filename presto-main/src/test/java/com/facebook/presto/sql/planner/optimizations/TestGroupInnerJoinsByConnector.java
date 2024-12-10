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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.connector.MockConnectorFactory;
import com.facebook.presto.connector.informationSchema.InformationSchemaColumnHandle;
import com.facebook.presto.connector.informationSchema.InformationSchemaMetadata;
import com.facebook.presto.connector.informationSchema.InformationSchemaTableHandle;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.JoinTableInfo;
import com.facebook.presto.spi.JoinTableSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.testing.assertions.Assert;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.INEQUALITY_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.INNER_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_JOIN_PUSHDOWN;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.immutableEnumSet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestGroupInnerJoinsByConnector
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);

    @Test
    public void testDoesNotPushDownOuterJoin()
    {
        String catalogName = "test_catalog";
        String connectorName = "test_catalog";
        ConnectorId connectorId = new ConnectorId(catalogName);

        MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
        ImmutableSet<ConnectorCapabilities> connectorCapabilities = immutableEnumSet(SUPPORTS_JOIN_PUSHDOWN);
        MockConnectorFactory mockConnectorFactory = builder.withListSchemaNames(connectorSession -> ImmutableList.of("test_schema"))
                .withConnectorCapabilities(connectorCapabilities)
                .withListTables((connectorSession, schemaNameOrNull) ->
                        ImmutableList.of(
                                new SchemaTableName("test_schema", "test_view"),
                                new SchemaTableName("test_schema", "another_table"))).build();
        Connector testConnector = mockConnectorFactory.create(catalogName, ImmutableMap.of(), new TestingConnectorContext());

        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                testConnector,
                createInformationSchemaConnectorId(connectorId),
                testConnector,
                createSystemTablesConnectorId(connectorId),
                testConnector));

        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        Metadata metadata = createTestMetadataManager(transactionManager, new FeaturesConfig(), new FunctionsConfig());

        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = testSessionBuilder().setTransactionId(transactionId).build();

        ImmutableMap.Builder<ColumnHandle, Domain> domains = new ImmutableMap.Builder<>();
        domains.put(new InformationSchemaColumnHandle("table_schema"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        Constraint<ColumnHandle> constraint = new Constraint<>(TupleDomain.withColumnDomains(domains.build()));

        InformationSchemaMetadata informationSchemaMetadata = new InformationSchemaMetadata(catalogName, metadata);
        informationSchemaMetadata.getTableLayouts(
                createNewSession(transactionId),
                new InformationSchemaTableHandle(catalogName, "information_schema", "views"),
                constraint,
                Optional.empty());

        VariableReferenceExpression left = newBigintVariable("a1");
        VariableReferenceExpression right = newBigintVariable("a2");
        EquiJoinClause joinClause = new EquiJoinClause(left, right);

        PlanNode plan = output(join(FULL,
                tableScan(connectorName, "a1", "b1"),
                tableScan(connectorName, "a2", "b2"),
                joinClause));
        PlanNode actual = optimize(plan, session, ImmutableMap.of(), metadata);
        Assert.assertEquals(plan, actual);
    }

    @Test
    public void testDoesNotJoinPushDownTwoDifferentConnectors()
    {
        String catalog1 = "test_catalog_1";
        String catalog2 = "test_catalog_2";

        ConnectorId connectorId1 = new ConnectorId(catalog1);
        ConnectorId connectorId2 = new ConnectorId(catalog2);

        MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
        MockConnectorFactory mockConnectorFactory = builder.withListSchemaNames(connectorSession -> ImmutableList.of("test_schema"))
                .withListTables((connectorSession, schemaNameOrNull) ->
                        ImmutableList.of(
                                new SchemaTableName("test_schema", "test_view"),
                                new SchemaTableName("test_schema", "another_table")))
                .withConnectorCapabilities(ImmutableSet.of()).build();

        Connector connector1 = mockConnectorFactory.create(catalog1, ImmutableMap.of(), new TestingConnectorContext());
        Connector connector2 = mockConnectorFactory.create(catalog2, ImmutableMap.of(), new TestingConnectorContext());

        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(new Catalog(
                catalog1,
                connectorId1,
                connector1,
                createInformationSchemaConnectorId(connectorId1),
                connector1,
                createSystemTablesConnectorId(connectorId1),
                connector1));

        catalogManager.registerCatalog(new Catalog(
                catalog2,
                connectorId2,
                connector2,
                createInformationSchemaConnectorId(connectorId2),
                connector2,
                createSystemTablesConnectorId(connectorId2),
                connector2));

        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        Metadata metadata = createTestMetadataManager(transactionManager, new FeaturesConfig(), new FunctionsConfig());
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = testSessionBuilder().setTransactionId(transactionId).build();

        ImmutableMap.Builder<ColumnHandle, Domain> domains = new ImmutableMap.Builder<>();
        domains.put(new InformationSchemaColumnHandle("table_schema"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        Constraint<ColumnHandle> constraint = new Constraint<>(TupleDomain.withColumnDomains(domains.build()));

        InformationSchemaMetadata informationSchemaMetadata1 = new InformationSchemaMetadata(catalog1, metadata);
        informationSchemaMetadata1.getTableLayouts(
                createNewSession(transactionId),
                new InformationSchemaTableHandle(catalog1, "information_schema", "views"),
                constraint,
                Optional.empty());

        InformationSchemaMetadata informationSchemaMetadata2 = new InformationSchemaMetadata(catalog2, metadata);
        informationSchemaMetadata2.getTableLayouts(
                createNewSession(transactionId),
                new InformationSchemaTableHandle(catalog2, "information_schema", "views"),
                constraint,
                Optional.empty());

        VariableReferenceExpression left = newBigintVariable("a1");
        VariableReferenceExpression right = newBigintVariable("a2");
        EquiJoinClause joinClause = new EquiJoinClause(left, right);

        PlanNode plan = output(join(INNER,
                tableScan("test_catalog_1", "a1", "b1"),
                tableScan("test_catalog_2", "a2", "b2"),
                joinClause));
        PlanNode actual = optimize(plan, session, ImmutableMap.of(), metadata);
        Assert.assertEquals(plan, actual);
    }

    @Test
    public void testJoinPushDownHappenedForSameConnectors()
    {
        String catalogName = "test_catalog";
        String connectorName = "test_catalog";
        ConnectorId connectorId = new ConnectorId(catalogName);

        MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
        ImmutableSet<ConnectorCapabilities> connectorCapabilities = immutableEnumSet(SUPPORTS_JOIN_PUSHDOWN);
        MockConnectorFactory mockConnectorFactory = builder.withListSchemaNames(connectorSession -> ImmutableList.of("test_schema"))
                .withConnectorCapabilities(connectorCapabilities)
                .withListTables((connectorSession, schemaNameOrNull) ->
                        ImmutableList.of(
                                new SchemaTableName("test_schema", "test_view"),
                                new SchemaTableName("test_schema", "another_table"))).build();
        Connector testConnector = mockConnectorFactory.create(catalogName, ImmutableMap.of(), new TestingConnectorContext());

        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                testConnector,
                createInformationSchemaConnectorId(connectorId),
                testConnector,
                createSystemTablesConnectorId(connectorId),
                testConnector));

        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        Metadata metadata = createTestMetadataManager(transactionManager, new FeaturesConfig(), new FunctionsConfig());

        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = testSessionBuilder()
                .setSystemProperty(INNER_JOIN_PUSHDOWN_ENABLED, "true")
                .setTransactionId(transactionId).build();

        ImmutableMap.Builder<ColumnHandle, Domain> domains = new ImmutableMap.Builder<>();
        domains.put(new InformationSchemaColumnHandle("table_schema"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        Constraint<ColumnHandle> constraint = new Constraint<>(TupleDomain.withColumnDomains(domains.build()));

        InformationSchemaMetadata informationSchemaMetadata = new InformationSchemaMetadata(catalogName, metadata);
        informationSchemaMetadata.getTableLayouts(
                createNewSession(transactionId),
                new InformationSchemaTableHandle(catalogName, "information_schema", "views"),
                constraint,
                Optional.empty());

        VariableReferenceExpression left = newBigintVariable("a1");
        VariableReferenceExpression right = newBigintVariable("a2");
        EquiJoinClause joinClause = new EquiJoinClause(left, right);

        PlanNode plan = output(join(INNER,
                tableScan(connectorName, "a1", "b1"),
                tableScan(connectorName, "a2", "b2"),
                joinClause));
        PlanNode actual = optimize(plan, session, ImmutableMap.of(), metadata);

        Set<JoinTableInfo> joinTableInfos = new HashSet<>();
        Map<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.of(newBigintVariable("a1"), new TestingColumnHandle("a1"), newBigintVariable("a2"),
                new TestingColumnHandle("a2"), newBigintVariable("b1"), new TestingColumnHandle("b1"), newBigintVariable("b2"),
                new TestingColumnHandle("b2"));
        List<VariableReferenceExpression> outputVariables = ImmutableList.of(newBigintVariable("a1"), newBigintVariable("a2"), newBigintVariable("b1"), newBigintVariable("b2"));
        JoinTableInfo joinTableInfo = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName("table_schema", "test-table")),
                assignments, outputVariables);
        joinTableInfos.add(joinTableInfo);
        JoinTableSet tableHandleSet = new JoinTableSet(ImmutableSet.copyOf(joinTableInfos));
        TableHandle tableHandle = new TableHandle(
                new ConnectorId(catalogName),
                tableHandleSet,
                TestingTransactionHandle.create(),
                Optional.empty());
        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.filter(
                                "a1 = a2 and true",
                                JoinTableScanMatcher.tableScan(connectorName, tableHandle, "a1", "a2"))));
    }

    @Test
    public void testJoinPushDownHappenedWithFilters()
    {
        String catalogName = "test_catalog";
        String connectorName = "test_catalog";
        ConnectorId connectorId = new ConnectorId(catalogName);

        MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
        ImmutableSet<ConnectorCapabilities> connectorCapabilities = immutableEnumSet(SUPPORTS_JOIN_PUSHDOWN);
        MockConnectorFactory mockConnectorFactory = builder.withListSchemaNames(connectorSession -> ImmutableList.of("test_schema"))
                .withConnectorCapabilities(connectorCapabilities)
                .withListTables((connectorSession, schemaNameOrNull) ->
                        ImmutableList.of(
                                new SchemaTableName("test_schema", "test_view"),
                                new SchemaTableName("test_schema", "another_table"))).build();
        Connector testConnector = mockConnectorFactory.create(catalogName, ImmutableMap.of(), new TestingConnectorContext());

        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                testConnector,
                createInformationSchemaConnectorId(connectorId),
                testConnector,
                createSystemTablesConnectorId(connectorId),
                testConnector));

        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        Metadata metadata = createTestMetadataManager(transactionManager, new FeaturesConfig(), new FunctionsConfig());
        TestingRowExpressionTranslator sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(metadata);

        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = testSessionBuilder()
                .setSystemProperty(INNER_JOIN_PUSHDOWN_ENABLED, "true")
                .setSystemProperty(INEQUALITY_JOIN_PUSHDOWN_ENABLED, "true")
                .setTransactionId(transactionId).build();

        ImmutableMap.Builder<ColumnHandle, Domain> domains = new ImmutableMap.Builder<>();
        domains.put(new InformationSchemaColumnHandle("table_schema"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        Constraint<ColumnHandle> constraint = new Constraint<>(TupleDomain.withColumnDomains(domains.build()));

        InformationSchemaMetadata informationSchemaMetadata = new InformationSchemaMetadata(catalogName, metadata);
        informationSchemaMetadata.getTableLayouts(
                createNewSession(transactionId),
                new InformationSchemaTableHandle(catalogName, "information_schema", "views"),
                constraint,
                Optional.empty());

        PlanBuilder planBuilder = new PlanBuilder(session, new PlanNodeIdAllocator(), metadata);
        String expression = "a1 > b1";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("a1", BIGINT, "b1", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        PlanNode plan = output(
                filter(
                        join(INNER,
                                tableScan(connectorName, "a1", "a2"),
                                tableScan(connectorName, "b1", "b2", "b3")), rowExpression));

        PlanNode actual = optimize(plan, session, ImmutableMap.of(), metadata);

        Set<JoinTableInfo> joinTableInfos = new HashSet<>();
        Map<VariableReferenceExpression, ColumnHandle> assignments1 = ImmutableMap.of(newBigintVariable("a1"), new TestingColumnHandle("a1"), newBigintVariable("a2"),
                new TestingColumnHandle("a2"));
        Map<VariableReferenceExpression, ColumnHandle> assignments2 = ImmutableMap.of(newBigintVariable("b1"), new TestingColumnHandle("b1"), newBigintVariable("b2"),
                new TestingColumnHandle("b2"), newBigintVariable("b3"), new TestingColumnHandle("b3"));
        List<VariableReferenceExpression> outputVariables1 = ImmutableList.of(newBigintVariable("a1"), newBigintVariable("a2"));
        List<VariableReferenceExpression> outputVariables2 = ImmutableList.of(newBigintVariable("b1"), newBigintVariable("b2"), newBigintVariable("b3"));
        JoinTableInfo joinTableInfo1 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName("table_schema", "test-table")),
                assignments1, outputVariables1);
        JoinTableInfo joinTableInfo2 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName("table_schema", "test-table")),
                assignments2, outputVariables2);
        joinTableInfos.add(joinTableInfo1);
        joinTableInfos.add(joinTableInfo2);
        JoinTableSet tableHandleSet = new JoinTableSet(ImmutableSet.copyOf(joinTableInfos));
        TableHandle tableHandle = new TableHandle(
                new ConnectorId(catalogName),
                tableHandleSet,
                TestingTransactionHandle.create(),
                Optional.empty());
        assertPlanMatch(
                actual,
                PlanMatchPattern.output(
                        PlanMatchPattern.filter(
                                "a1 > b1 and true",
                                JoinTableScanMatcher.tableScan(connectorName, tableHandle, "a1", "b1"))));
    }

    private PlanNode optimize(PlanNode plan, Session session, Map<ConnectorId, Set<ConnectorPlanOptimizer>> optimizers, Metadata metadata)
    {
        GroupInnerJoinsByConnector optimizer = new GroupInnerJoinsByConnector(metadata);
        return optimizer.optimize(plan, session, TypeProvider.empty(), new VariableAllocator(), new PlanNodeIdAllocator(), WarningCollector.NOOP).getPlanNode();
    }

    private OutputNode output(PlanNode source, String... columnNames)
    {
        return PLAN_BUILDER.output(
                Arrays.stream(columnNames).collect(toImmutableList()),
                Arrays.stream(columnNames).map(TestGroupInnerJoinsByConnector::newBigintVariable).collect(toImmutableList()),
                source);
    }

    private FilterNode filter(PlanNode source, RowExpression predicate)
    {
        return PLAN_BUILDER.filter(predicate, source);
    }

    private TableScanNode tableScan(String connectorName, String... columnNames)
    {
        return PLAN_BUILDER.tableScan(
                connectorName,
                Arrays.stream(columnNames).map(TestGroupInnerJoinsByConnector::newBigintVariable).collect(toImmutableList()),
                Arrays.stream(columnNames).map(TestGroupInnerJoinsByConnector::newBigintVariable).collect(toMap(identity(), variable -> new ColumnHandle() {})));
    }

    private JoinNode join(JoinType joinType, PlanNode left, PlanNode right, EquiJoinClause... criteria)
    {
        return PLAN_BUILDER.join(joinType, left, right, criteria);
    }

    private static VariableReferenceExpression newBigintVariable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }

    private ConnectorSession createNewSession(TransactionId transactionId)
    {
        return testSessionBuilder()
                .setCatalog("test_catalog")
                .setSchema("information_schema")
                .setTransactionId(transactionId)
                .build()
                .toConnectorSession();
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected)
    {
        assertPlanMatch(actual, expected, TypeProvider.empty());
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected, TypeProvider typeProvider)
    {
        PlanAssert.assertPlan(
                TEST_SESSION,
                METADATA,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, typeProvider, StatsAndCosts.empty()),
                expected);
    }

    private static final class JoinTableScanMatcher
            implements Matcher
    {
        private final ConnectorId connectorId;
        private final TableHandle tableHandle;
        private final String[] columns;

        public static PlanMatchPattern tableScan(String connectorName, TableHandle tableHandle, String... columnNames)
        {
            return node(TableScanNode.class)
                    .with(new JoinTableScanMatcher(
                            new ConnectorId(connectorName),
                            tableHandle,
                            columnNames));
        }

        public static PlanMatchPattern tableScan(String connectorName, String... columnNames)
        {
            return tableScan(connectorName, null, columnNames);
        }

        private JoinTableScanMatcher(
                ConnectorId connectorId,
                TableHandle tableHandle,
                String... columns)
        {
            this.connectorId = connectorId;
            this.tableHandle = tableHandle;
            this.columns = columns;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
            TableScanNode tableScanNode = (TableScanNode) node;
            ConnectorTableHandle connectorHandle = tableScanNode.getTable().getConnectorHandle();

            if (connectorId.equals(tableScanNode.getTable().getConnectorId()) &&
                    connectorHandle instanceof JoinTableSet && connectorHandle.equals(tableScanNode.getTable().getConnectorHandle())) {
                return MatchResult.match(SymbolAliases.builder().putAll(Arrays.stream(columns).collect(toMap(identity(), SymbolReference::new))).build());
            }

            return MatchResult.NO_MATCH;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .omitNullValues()
                    .add("connectorId", connectorId)
                    .toString();
        }
    }
}
