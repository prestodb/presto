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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.optimization.function.OperatorTranslators;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertTrue;

public class TestJdbcComputePushdown
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
    private static final PlanNodeIdAllocator ID_ALLOCATOR = new PlanNodeIdAllocator();
    private static final String CATALOG_NAME = "Jdbc";
    private static final String CONNECTOR_ID = new ConnectorId(CATALOG_NAME).toString();

    private final TestingRowExpressionTranslator sqlToRowExpressionTranslator;

    private final JdbcComputePushdown jdbcComputePushdown;

    public TestJdbcComputePushdown()
    {
        this.sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(METADATA);
        FunctionAndTypeManager functionAndTypeManager = METADATA.getFunctionAndTypeManager();
        StandardFunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager);
        DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);

        this.jdbcComputePushdown = new JdbcComputePushdown(
                functionAndTypeManager,
                functionResolution,
                determinismEvaluator,
                new RowExpressionOptimizer(METADATA),
                "'",
                getFunctionTranslators());
    }

    @Test
    public void testJdbcComputePushdownAll()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "(c1 + c2) - c2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::integerJdbcColumnHandle).collect(Collectors.toSet());
        PlanNode original = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(session.getSqlFunctionProperties(), jdbcTableHandle, TupleDomain.none(), Optional.of(new JdbcExpression("(('c1' + 'c2') - 'c2')")));

        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(
                actual,
                PlanMatchPattern.filter(expression, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    @Test
    public void testJdbcComputePushdownBooleanOperations()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "(((c1 + c2) - c2 <> c2) OR c2 = c1) AND c1 <> c2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::integerJdbcColumnHandle).collect(Collectors.toSet());
        PlanNode original = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                session.getSqlFunctionProperties(),
                jdbcTableHandle,
                TupleDomain.none(),
                Optional.of(new JdbcExpression("((((((('c1' + 'c2') - 'c2') <> 'c2')) OR (('c2' = 'c1')))) AND (('c1' <> 'c2')))")));

        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(
                actual,
                PlanMatchPattern.filter(expression, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    @Test
    public void testJdbcComputePushdownUnsupported()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "(c1 + c2) > c2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::integerJdbcColumnHandle).collect(Collectors.toSet());
        PlanNode original = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        // Test should expect an empty entry for translatedSql since > is an unsupported function currently in the optimizer
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(session.getSqlFunctionProperties(), jdbcTableHandle, TupleDomain.none(), Optional.empty());

        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                expression,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    @Test
    public void testJdbcComputePushdownWithConstants()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "(c1 + c2) = 3";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::integerJdbcColumnHandle).collect(Collectors.toSet());
        PlanNode original = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                session.getSqlFunctionProperties(),
                jdbcTableHandle,
                TupleDomain.none(),
                Optional.of(new JdbcExpression("(('c1' + 'c2') = ?)", ImmutableList.of(new ConstantExpression(Long.valueOf(3), INTEGER)))));

        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                expression,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    @Test
    public void testJdbcComputePushdownNotOperator()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "c1 AND NOT(c2)";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BOOLEAN, "c2", BOOLEAN));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        PlanNode original = filter(jdbcTableScan(schema, table, BOOLEAN, "c1", "c2"), rowExpression);

        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::booleanJdbcColumnHandle).collect(Collectors.toSet());
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                session.getSqlFunctionProperties(),
                jdbcTableHandle,
                TupleDomain.none(),
                Optional.of(new JdbcExpression("(('c1') AND ((NOT('c2'))))")));

        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                expression,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    private Set<Class<?>> getFunctionTranslators()
    {
        return ImmutableSet.of(OperatorTranslators.class);
    }

    private static VariableReferenceExpression newVariable(String name, Type type)
    {
        return new VariableReferenceExpression(name, type);
    }

    private static JdbcColumnHandle integerJdbcColumnHandle(String name)
    {
        return new JdbcColumnHandle(CONNECTOR_ID, name, new JdbcTypeHandle(Types.BIGINT, 10, 0), BIGINT, false, Optional.empty());
    }

    private static JdbcColumnHandle booleanJdbcColumnHandle(String name)
    {
        return new JdbcColumnHandle(CONNECTOR_ID, name, new JdbcTypeHandle(Types.BOOLEAN, 1, 0), BOOLEAN, false, Optional.empty());
    }

    private static JdbcColumnHandle getColumnHandleForVariable(String name, Type type)
    {
        return type.equals(BOOLEAN) ? booleanJdbcColumnHandle(name) : integerJdbcColumnHandle(name);
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected)
    {
        assertPlanMatch(actual, expected, TypeProvider.empty());
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected, TypeProvider typeProvider)
    {
        // always check the actual plan node has a filter node to prevent accidentally missing predicates
        Queue<PlanNode> nodes = new LinkedList<>(ImmutableSet.of(actual));
        boolean hasFilterNode = false;
        while (!nodes.isEmpty()) {
            PlanNode node = nodes.poll();
            nodes.addAll(node.getSources());

            // we always enforce the original filter to be present
            // the following assertPlan will guarantee the completeness of the filter
            if (node instanceof FilterNode && node.getSources().get(0) instanceof TableScanNode) {
                hasFilterNode = true;
                break;
            }
        }
        assertTrue(hasFilterNode, "filter is missing from the pushdown plan");

        PlanAssert.assertPlan(
                TEST_SESSION,
                METADATA,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, typeProvider, StatsAndCosts.empty()),
                expected);
    }

    private TableScanNode jdbcTableScan(String schema, String table, Type type, String... columnNames)
    {
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(TEST_SESSION.getSqlFunctionProperties(), jdbcTableHandle, TupleDomain.none(), Optional.empty());
        TableHandle tableHandle = new TableHandle(new ConnectorId(CATALOG_NAME), jdbcTableHandle, new ConnectorTransactionHandle() {}, Optional.of(jdbcTableLayoutHandle));

        return PLAN_BUILDER.tableScan(
                tableHandle,
                Arrays.stream(columnNames).map(column -> newVariable(column, type)).collect(toImmutableList()),
                Arrays.stream(columnNames)
                        .map(column -> newVariable(column, type))
                        .collect(toMap(identity(), entry -> getColumnHandleForVariable(entry.getName(), type))));
    }

    private FilterNode filter(PlanNode source, RowExpression predicate)
    {
        return PLAN_BUILDER.filter(predicate, source);
    }

    private static final class JdbcTableScanMatcher
            implements Matcher
    {
        private final JdbcTableLayoutHandle jdbcTableLayoutHandle;
        private final Set<ColumnHandle> columns;

        static PlanMatchPattern jdbcTableScanPattern(JdbcTableLayoutHandle jdbcTableLayoutHandle, Set<ColumnHandle> columns)
        {
            return node(TableScanNode.class).with(new JdbcTableScanMatcher(jdbcTableLayoutHandle, columns));
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
            if (jdbcTableLayoutHandle.getTable().equals(layoutHandle.getTable())
                    && jdbcTableLayoutHandle.getTupleDomain().equals(layoutHandle.getTupleDomain())
                    && ((!jdbcTableLayoutHandle.getAdditionalPredicate().isPresent() && !layoutHandle.getAdditionalPredicate().isPresent())
                        || jdbcTableLayoutHandle.getAdditionalPredicate().get().getExpression().equals(layoutHandle.getAdditionalPredicate().get().getExpression()))) {
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
