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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.JoinTableInfo;
import com.facebook.presto.spi.JoinTableSet;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.optimizations.GroupInnerJoinsByConnectorRuleSet;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizerResult;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchNodePartitioningProvider;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.tpch.TpchTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.INEQUALITY_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.INNER_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_JOIN_PUSHDOWN;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestGroupInnerJoinsByConnectorRuleSet
{
    public static final String CATALOG_SUPPORTING_JOIN_PUSHDOWN = "catalog_join_pushdown_supported";
    public static final String LOCAL = "local";
    public static final String TEST_SCHEMA = "test-schema";
    public static final String TEST_TABLE = "test-table";
    public static final String OTHER_CATALOG_SUPPORTING_JOIN_PUSHDOWN = "other_catalog_join_pushdown_supported";
    private PlanBuilder planBuilder;
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        LocalQueryRunner runner = new LocalQueryRunner(TEST_SESSION);
        ConnectorFactory pushdownConnectorFactory = new TestingJoinPushdownConnectorFactory()
        {
            @Override
            public String getName()
            {
                return "tpch_with_join_pushdown";
            }
        };
        ConnectorFactory pushdownConnectorFactory1 = new TestingJoinPushdownConnectorFactory()
        {
            @Override
            public String getName()
            {
                return "tpch_with_join_pushdown1";
            }
        };
        runner.createCatalog(CATALOG_SUPPORTING_JOIN_PUSHDOWN, pushdownConnectorFactory, ImmutableMap.of());
        runner.createCatalog(OTHER_CATALOG_SUPPORTING_JOIN_PUSHDOWN, pushdownConnectorFactory1, ImmutableMap.of());

        tester = new RuleTester(
                ImmutableList.of(),
                RuleTester.getSession(ImmutableMap.of(INNER_JOIN_PUSHDOWN_ENABLED, "true", INEQUALITY_JOIN_PUSHDOWN_ENABLED, "true"), createTestingSessionPropertyManager()),
                runner,
                new TpchConnectorFactory(1));

        planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), runner.getMetadata());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testDoesNotPushDownOuterJoin()
    {
        String connectorName = "test_catalog";

        assertGroupInnerJoinsByConnectorRuleSet()
                .on(p ->
                        p.join(
                                FULL,
                                tableScan(connectorName, "a1", "b1"),
                                tableScan(connectorName, "a2", "b2"),
                                new EquiJoinClause(newBigintVariable("a1"), newBigintVariable("a2"))))
                .doesNotFire();
    }

    @Test
    public void testDPartialPushDownTwoDifferentConnectors()
    {
        Set<JoinTableInfo> joinTableInfos = new HashSet<>();

        JoinTableInfo joinTableInfo1 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(newBigintVariable("a1"), new TestingColumnHandle("a1"), newBigintVariable("a2"),
                        new TestingColumnHandle("a2")), ImmutableList.of(newBigintVariable("a1"), newBigintVariable("a2")));
        JoinTableInfo joinTableInfo2 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(newBigintVariable("c1"), new TestingColumnHandle("c1"), newBigintVariable("c2"),
                        new TestingColumnHandle("c2")), ImmutableList.of(newBigintVariable("c1"), newBigintVariable("c2")));
        joinTableInfos.add(joinTableInfo1);
        joinTableInfos.add(joinTableInfo2);
        JoinTableSet tableHandleSet = new JoinTableSet(joinTableInfos);
        TableHandle tableHandle1 = new TableHandle(
                new ConnectorId(CATALOG_SUPPORTING_JOIN_PUSHDOWN),
                tableHandleSet,
                TestingTransactionHandle.create(),
                Optional.empty());
        TableHandle tableHandle2 = new TableHandle(
                new ConnectorId(LOCAL),
                new TestingMetadata.TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        assertGroupInnerJoinsByConnectorRuleSet()
                .on(p ->
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "a1", "a2"),
                                        tableScan(LOCAL, "b1", "b2"),
                                        new EquiJoinClause(newBigintVariable("a1"), newBigintVariable("b1"))),
                                tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "c1", "c2"),
                                new EquiJoinClause(newBigintVariable("a1"), newBigintVariable("c1"))))
                .matches(
                        project(
                                filter(
                                        "a1 = b1 and a1 = c1 and true",
                                        join(
                                                JoinTableScanMatcher.tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, tableHandle1, "a1", "a2", "c1", "c2"),
                                                JoinTableScanMatcher.tableScan(LOCAL, tableHandle2, "b1", "b2")))));
    }

    @Test
    public void testValidPushdownForSameConnector()
    {
        Set<JoinTableInfo> joinTableInfos = new HashSet<>();

        JoinTableInfo joinTableInfo1 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(newBigintVariable("b1"), new TestingColumnHandle("b1"), newBigintVariable("a1"),
                        new TestingColumnHandle("a1")), ImmutableList.of(newBigintVariable("a1"), newBigintVariable("b1")));
        JoinTableInfo joinTableInfo2 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(newBigintVariable("b2"), new TestingColumnHandle("b2"), newBigintVariable("a2"),
                        new TestingColumnHandle("a2")), ImmutableList.of(newBigintVariable("a2"), newBigintVariable("b2")));
        joinTableInfos.add(joinTableInfo1);
        joinTableInfos.add(joinTableInfo2);
        JoinTableSet tableHandleSet = new JoinTableSet(joinTableInfos);
        TableHandle tableHandle = new TableHandle(
                new ConnectorId(CATALOG_SUPPORTING_JOIN_PUSHDOWN),
                tableHandleSet,
                TestingTransactionHandle.create(),
                Optional.empty());

        assertGroupInnerJoinsByConnectorRuleSet()
                .on(p ->
                        p.join(
                                INNER,
                                tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "a1", "b1"),
                                tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "a2", "b2"),
                                new EquiJoinClause(newBigintVariable("a1"), newBigintVariable("a2")))
                ).matches(
                        project(
                                filter(
                                        "a1 = a2 and true",
                                        JoinTableScanMatcher.tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, tableHandle, "a1", "a2"))));
    }

    @Test
    public void testJoinPushDownHappenedWithFilters()
    {
        Set<JoinTableInfo> joinTableInfos = new HashSet<>();

        JoinTableInfo joinTableInfo1 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(newBigintVariable("a1"), new TestingColumnHandle("a1"), newBigintVariable("b1"),
                        new TestingColumnHandle("b1")), ImmutableList.of(newBigintVariable("a1"), newBigintVariable("b1")));
        JoinTableInfo joinTableInfo2 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(newBigintVariable("a2"), new TestingColumnHandle("a2"), newBigintVariable("b2"),
                        new TestingColumnHandle("b2")), ImmutableList.of(newBigintVariable("a2"), newBigintVariable("b2")));
        joinTableInfos.add(joinTableInfo1);
        joinTableInfos.add(joinTableInfo2);
        JoinTableSet tableHandleSet = new JoinTableSet(joinTableInfos);
        TableHandle tableHandle = new TableHandle(
                new ConnectorId(CATALOG_SUPPORTING_JOIN_PUSHDOWN),
                tableHandleSet,
                TestingTransactionHandle.create(),
                Optional.empty());

        String expression = "a1 > b1";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("a1", BIGINT, "b1", BIGINT));
        TestingRowExpressionTranslator sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(tester.getMetadata());
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);

        assertGroupInnerJoinsByConnectorRuleSet()
                .on(p ->
                        p.join(
                                INNER,
                                tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "a1", "b1"),
                                tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "a2", "b2"),
                                rowExpression,
                                new EquiJoinClause(newBigintVariable("a1"), newBigintVariable("a2"))))
                .matches(
                        project(
                                filter(
                                        "a1 = a2 and a1 > b1 and true",
                                        JoinTableScanMatcher.tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, tableHandle, "a1", "a2", "b1"))));
    }

    @Test
    public void testPushDownWithTwoDifferentConnectors()
    {
        Set<JoinTableInfo> joinTableSet1 = new HashSet<>();

        JoinTableInfo tableInfo1 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(
                        newBigintVariable("a1"), new TestingColumnHandle("a1"),
                        newBigintVariable("a2"), new TestingColumnHandle("a2")), ImmutableList.of(newBigintVariable("a1"), newBigintVariable("a2")));
        JoinTableInfo tableInfo2 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(
                        newBigintVariable("b1"), new TestingColumnHandle("b1"),
                        newBigintVariable("b2"), new TestingColumnHandle("b2")), ImmutableList.of(newBigintVariable("b1"), newBigintVariable("b2")));

        joinTableSet1.add(tableInfo1);
        joinTableSet1.add(tableInfo2);
        JoinTableSet tableHandleSet1 = new JoinTableSet(joinTableSet1);
        TableHandle tableHandle1 = new TableHandle(
                new ConnectorId(CATALOG_SUPPORTING_JOIN_PUSHDOWN),
                tableHandleSet1,
                TestingTransactionHandle.create(),
                Optional.empty());

        Set<JoinTableInfo> joinTableSet2 = new HashSet<>();

        JoinTableInfo table2Info1 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(
                        newBigintVariable("c1"), new TestingColumnHandle("c1"),
                        newBigintVariable("c2"), new TestingColumnHandle("c2")), ImmutableList.of(newBigintVariable("c1"), newBigintVariable("c2")));
        JoinTableInfo table2Info2 = new JoinTableInfo(new TestingMetadata.TestingTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                ImmutableMap.of(
                        newBigintVariable("d1"), new TestingColumnHandle("d1"),
                        newBigintVariable("d2"), new TestingColumnHandle("d2")), ImmutableList.of(newBigintVariable("d1"), newBigintVariable("d2")));

        joinTableSet2.add(table2Info1);
        joinTableSet2.add(table2Info2);
        JoinTableSet tableHandleSet2 = new JoinTableSet(joinTableSet2);
        TableHandle tableHandle2 = new TableHandle(
                new ConnectorId(OTHER_CATALOG_SUPPORTING_JOIN_PUSHDOWN),
                tableHandleSet2,
                TestingTransactionHandle.create(),
                Optional.empty());

        assertGroupInnerJoinsByConnectorRuleSet()
                .on(p ->
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "a1", "a2"),
                                        tableScan(OTHER_CATALOG_SUPPORTING_JOIN_PUSHDOWN, "d1", "d2"),
                                        new EquiJoinClause(newBigintVariable("a1"), newBigintVariable("b1")),
                                        new EquiJoinClause(newBigintVariable("a1"), newBigintVariable("d1"))),
                                p.join(
                                        INNER,
                                        tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, "b1", "b2"),
                                        tableScan(OTHER_CATALOG_SUPPORTING_JOIN_PUSHDOWN, "c1", "c2"),
                                        new EquiJoinClause(newBigintVariable("b1"), newBigintVariable("c1"))),
                                new EquiJoinClause(newBigintVariable("c1"), newBigintVariable("d1"))))
                .matches(
                        project(
                                filter(
                                        "((a1 = b1 and a1 = d1) and (b1 = c1 and c1 = d1)) and true",
                                        join(
                                                JoinTableScanMatcher.tableScan(CATALOG_SUPPORTING_JOIN_PUSHDOWN, tableHandle1, "a1", "b1"),
                                                JoinTableScanMatcher.tableScan(OTHER_CATALOG_SUPPORTING_JOIN_PUSHDOWN, tableHandle2, "c1", "d1")))));
    }

    private RuleAssert assertGroupInnerJoinsByConnectorRuleSet()
    {
        // For testing, we do not wish to push down pulled up predicates
        return tester.assertThat(new GroupInnerJoinsByConnectorRuleSet.OnlyJoinRule(tester.getMetadata(), (plan, session, types, variableAllocator, idAllocator, warningCollector) -> PlanOptimizerResult.optimizerResult(plan, false)),
                ImmutableList.of(CATALOG_SUPPORTING_JOIN_PUSHDOWN, OTHER_CATALOG_SUPPORTING_JOIN_PUSHDOWN));
    }

    private TableScanNode tableScan(String connectorName, String... columnNames)
    {
        return planBuilder.tableScan(
                connectorName,
                Arrays.stream(columnNames).map(TestGroupInnerJoinsByConnectorRuleSet::newBigintVariable).collect(toImmutableList()),
                Arrays.stream(columnNames)
                        .map(TestGroupInnerJoinsByConnectorRuleSet::newBigintVariable)
                        .collect(Collectors.toMap(identity(),
                                variable -> new TestingColumnHandle(variable.getName()))));
    }

    private static VariableReferenceExpression newBigintVariable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }

    private static class TestingJoinPushdownConnectorFactory
            extends TpchConnectorFactory
    {
        @Override
        public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context)
        {
            int splitsPerNode = super.getSplitsPerNode(properties);
            ColumnNaming columnNaming = ColumnNaming.valueOf(properties.getOrDefault(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.SIMPLIFIED.name()).toUpperCase());
            NodeManager nodeManager = context.getNodeManager();

            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return TpchTransactionHandle.INSTANCE;
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                {
                    return new TpchMetadata(catalogName, columnNaming, isPredicatePushdownEnabled(), isPartitioningEnabled(properties))
                    {
                        @Override
                        public boolean isPushdownSupportedForFilter(ConnectorSession session, ConnectorTableHandle tableHandle, RowExpression filter, Map<VariableReferenceExpression, ColumnHandle> symbolToColumnHandleMap)
                        {
                            return true;
                        }
                    };
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new TpchSplitManager(nodeManager, splitsPerNode);
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new TpchRecordSetProvider();
                }

                @Override
                public ConnectorNodePartitioningProvider getNodePartitioningProvider()
                {
                    return new TpchNodePartitioningProvider(nodeManager, splitsPerNode);
                }

                @Override
                public Set<ConnectorCapabilities> getCapabilities()
                {
                    return ImmutableSet.of(SUPPORTS_JOIN_PUSHDOWN);
                }
            };
        }
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
            TableHandle otherTable = tableScanNode.getTable();
            ConnectorTableHandle connectorHandle = otherTable.getConnectorHandle();

            if (connectorId.equals(otherTable.getConnectorId()) && Objects.equals(otherTable.getConnectorId(), this.tableHandle.getConnectorId()) &&
                    Objects.equals(otherTable.getConnectorHandle(), this.tableHandle.getConnectorHandle()) &&
                    Objects.equals(otherTable.getLayout().isPresent(), this.tableHandle.getLayout().isPresent())) {
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
