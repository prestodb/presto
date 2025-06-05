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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.JoinTableInfo;
import com.facebook.presto.spi.JoinTableSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestJdbcJoinPushdown
{
    private final JdbcJoinPushdown jdbcJoinPushdown;

    private static final Metadata METADATA = createTestMetadataManager();
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
    private static final String CATALOG_NAME = "Jdbc";
    private static final String CONNECTOR_ID = new ConnectorId(CATALOG_NAME).toString();
    private static final PlanNodeIdAllocator ID_ALLOCATOR = new PlanNodeIdAllocator();

    public TestJdbcJoinPushdown()
    {
        this.jdbcJoinPushdown = new JdbcJoinPushdown();
    }

    // Verifies that join pushdown correctly builds expected JdbcTableHandles and layout with proper aliases and expressions
    @Test
    public void testJdbcJoinPushdownWithJoins()
    {
        PlanNode original = combinedJdbcTableScan(BIGINT, "c1", "c2");
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcJoinPushdown.optimize(original, session, null, ID_ALLOCATOR);

        final String aliasPrefix = "T";
        ImmutableList.Builder<ConnectorTableHandle> expectedJoinTableHandlesBuilder = ImmutableList.builder();
        for (int i = 1; i <= 3; i++) {
            String table = "test_table" + i;
            String schema = "test_schema" + i;
            JdbcTableHandle tableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, Collections.emptyList(), Optional.of(aliasPrefix + i));
            expectedJoinTableHandlesBuilder.add(tableHandle);
        }
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName("test_schema1", "test_table1"), CATALOG_NAME, "test_schema1", "test_table1", expectedJoinTableHandlesBuilder.build(), Optional.of("T1"));
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(session.getSqlFunctionProperties(), jdbcTableHandle, TupleDomain.none(), Optional.of(new JdbcExpression("(('c1' + 'c2') - 'c2')")));
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcJoinPushdown::integerJdbcColumnHandle).collect(Collectors.toSet());
        assertPlanMatch(
                actual,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
    }

    @Test
    public void testJdbcJoinPushdownWithoutJoins()
    {
        String table = "test_table";
        String schema = "test_schema";
        PlanNode original = jdbcTableScan(schema, table, BIGINT, "c1", "c2");
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = jdbcJoinPushdown.optimize(original, session, null, ID_ALLOCATOR);

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, Collections.emptyList(), Optional.empty());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(session.getSqlFunctionProperties(), jdbcTableHandle, TupleDomain.none(), Optional.of(new JdbcExpression("(('c1' + 'c2') - 'c2')")));
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcJoinPushdown::integerJdbcColumnHandle).collect(Collectors.toSet());
        assertPlanMatch(
                actual,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
    }

    private static VariableReferenceExpression newVariable(String name, Type type)
    {
        return new VariableReferenceExpression(Optional.empty(), name, type);
    }

    private static JdbcColumnHandle getColumnHandleForVariable(String name, Type type)
    {
        return type.equals(BOOLEAN) ? booleanJdbcColumnHandle(name) : integerJdbcColumnHandle(name);
    }

    private static JdbcColumnHandle integerJdbcColumnHandle(String name)
    {
        return new JdbcColumnHandle(CONNECTOR_ID, name, new JdbcTypeHandle(Types.BIGINT, "integer", 10, 0), BIGINT, false, Optional.empty(), Optional.empty());
    }

    private static JdbcColumnHandle booleanJdbcColumnHandle(String name)
    {
        return new JdbcColumnHandle(CONNECTOR_ID, name, new JdbcTypeHandle(Types.BOOLEAN, "boolean", 1, 0), BOOLEAN, false, Optional.empty(), Optional.empty());
    }

    /**
     * Gets a TableScanNode that represent the output of {@code GroupInnerJoinsByConnectorRuleSet} for a JDBC source -
     * TableScanNode
     *    - Pushed down predicate - `('c1' + 'c2') - 'c2')`
     *    - (T1,T2,T3)
     */
    private TableScanNode combinedJdbcTableScan(Type type, String... columnNames)
    {
        Set<ConnectorTableHandle> tableHandles = new LinkedHashSet<>();
        final String aliasPrefix = "T";
        for (int i = 1; i <= 3; i++) {
            String table = "test_table" + i;
            String schema = "test_schema" + i;
            JdbcTableHandle tableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, Collections.emptyList(), Optional.of(aliasPrefix + i));
            tableHandles.add(tableHandle);
        }
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableHandles.iterator().next();
        Set<JoinTableInfo> joinTableInfos = new LinkedHashSet<>();
        List<VariableReferenceExpression> outputVariables = Arrays.stream(columnNames).map(column -> newVariable(column, type)).collect(toImmutableList());
        Map<VariableReferenceExpression, ColumnHandle> assignments = Arrays.stream(columnNames)
                .map(column -> newVariable(column, type)).collect(toMap(identity(), entry -> getColumnHandleForVariable(entry.getName(), type)));
        tableHandles.forEach(tableHandle -> {
            joinTableInfos.add(new JoinTableInfo(tableHandle, assignments, outputVariables));
        });
        ConnectorTableHandle connectorTableHandle = new JoinTableSet(ImmutableSet.copyOf(joinTableInfos));

        Optional<JdbcExpression> additionalExpression = Optional.of(new JdbcExpression("(('c1' + 'c2') - 'c2')"));
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(TEST_SESSION.getSqlFunctionProperties(), jdbcTableHandle, TupleDomain.none(), additionalExpression);
        TableHandle tableHandle = new TableHandle(new ConnectorId(CATALOG_NAME), connectorTableHandle, new ConnectorTransactionHandle() {}, Optional.of(jdbcTableLayoutHandle));

        return PLAN_BUILDER.tableScan(
                tableHandle,
                outputVariables,
                assignments);
    }

    private TableScanNode jdbcTableScan(String schema, String table, Type type, String... columnNames)
    {
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, Collections.emptyList(), Optional.empty());
        ConnectorTableHandle connectorTableHandle = jdbcTableHandle;

        Optional<JdbcExpression> additionalExpression = Optional.of(new JdbcExpression("(('c1' + 'c2') - 'c2')"));
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(TEST_SESSION.getSqlFunctionProperties(), jdbcTableHandle, TupleDomain.none(), additionalExpression);
        TableHandle tableHandle = new TableHandle(new ConnectorId(CATALOG_NAME), connectorTableHandle, new ConnectorTransactionHandle() {}, Optional.of(jdbcTableLayoutHandle));

        return PLAN_BUILDER.tableScan(
                tableHandle,
                Arrays.stream(columnNames).map(column -> newVariable(column, type)).collect(toImmutableList()),
                Arrays.stream(columnNames)
                        .map(column -> newVariable(column, type))
                        .collect(toMap(identity(), entry -> getColumnHandleForVariable(entry.getName(), type))));
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

    private static final class JdbcTableScanMatcher
            implements Matcher
    {
        private final JdbcTableLayoutHandle jdbcTableLayoutHandle;
        private final Set<ColumnHandle> columns;

        static PlanMatchPattern jdbcTableScanPattern(JdbcTableLayoutHandle jdbcTableLayoutHandle, Set<ColumnHandle> columns)
        {
            return node(TableScanNode.class).with(new TestJdbcJoinPushdown.JdbcTableScanMatcher(jdbcTableLayoutHandle, columns));
        }

        private JdbcTableScanMatcher(JdbcTableLayoutHandle jdbcTableLayoutHandle, Set<ColumnHandle> columns)
        {
            this.jdbcTableLayoutHandle = jdbcTableLayoutHandle;
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
            JdbcTableLayoutHandle layoutHandle = (JdbcTableLayoutHandle) tableScanNode.getTable().getLayout().get();

            if (Objects.equals(jdbcTableLayoutHandle, layoutHandle)) {
                return MatchResult.match(
                        SymbolAliases.builder().putAll(
                                        columns.stream()
                                                .map(column -> ((JdbcColumnHandle) column).getColumnName())
                                                .collect(toMap(identity(), SymbolReference::new)))
                                .build());
            }

            return MatchResult.NO_MATCH;
        }
    }
}
