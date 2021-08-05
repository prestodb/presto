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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.plugin.jdbc.TestingBaseJdbcClient;
import com.facebook.presto.plugin.jdbc.optimization.function.OperatorTranslators;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airlift.slice.Slices;
import org.h2.Driver;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BOOLEAN;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_CHAR;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DOUBLE;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.sql.tree.SortItem.Ordering.DESCENDING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEntry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestJdbcComputePushdown
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
    private static final PlanNodeIdAllocator ID_ALLOCATOR = new PlanNodeIdAllocator();
    private static final String CATALOG_NAME = "Jdbc";
    private static final String CONNECTOR_ID = new ConnectorId(CATALOG_NAME).toString();

    private static List<JdbcColumnHandle> jdbcColumnHandles = Lists.newArrayList(
            new JdbcColumnHandle(CONNECTOR_ID, "l_orderkey", JDBC_BIGINT, BIGINT, false),
            new JdbcColumnHandle(CONNECTOR_ID, "l_partkey", JDBC_BIGINT, BIGINT, false),
            new JdbcColumnHandle(CONNECTOR_ID, "l_quantity", JDBC_DOUBLE, DOUBLE, false),
            new JdbcColumnHandle(CONNECTOR_ID, "l_extendedprice", JDBC_DOUBLE, DOUBLE, false),
            new JdbcColumnHandle(CONNECTOR_ID, "l_returnflag", JDBC_CHAR, createCharType(1), false),
            new JdbcColumnHandle(CONNECTOR_ID, "l_shipdate", JDBC_VARCHAR, VARCHAR, false),
            new JdbcColumnHandle(CONNECTOR_ID, "l_commitdate", JDBC_VARCHAR, VARCHAR, false),
            new JdbcColumnHandle(CONNECTOR_ID, "l_receiptdate", JDBC_VARCHAR, VARCHAR, false));

    private final TestingRowExpressionTranslator sqlToRowExpressionTranslator;

    private final JdbcComputePushdown jdbcComputePushdown;
    private final FunctionAndTypeManager typeManager = createTestFunctionAndTypeManager();
    private final JdbcClient jdbcClient;

    public TestJdbcComputePushdown()
    {
        this.sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(METADATA);
        FunctionAndTypeManager functionAndTypeManager = METADATA.getFunctionAndTypeManager();
        StandardFunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager);
        DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);

        String connectionUrl = "jdbc:h2:mem:test" + System.nanoTime();
        jdbcClient = new TestingBaseJdbcClient(
                new JdbcConnectorId(CONNECTOR_ID),
                new BaseJdbcConfig(),
                "\"",
                new DriverConnectionFactory(new Driver(), connectionUrl, Optional.empty(), Optional.empty(), new Properties()));

        this.jdbcComputePushdown = new JdbcComputePushdown(
                jdbcClient,
                typeManager,
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

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("(('c1' + 'c2') - 'c2')"));
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(
                actual,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
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

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("((((('c1' + 'c2') - 'c2') <> 'c2') OR ('c2' = 'c1')) AND ('c1' <> 'c2'))"));
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(
                actual,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
    }

    @Test
    public void testJdbcComputePushdownGreaterThen()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "(c1 + c2) > c2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::integerJdbcColumnHandle).collect(Collectors.toSet());
        PlanNode original = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("('c1' + 'c2') > 'c2'"));

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
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

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("(('c1' + 'c2') = ?)", ImmutableList.of(new ConstantExpression(3L, INTEGER))));

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
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

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("('c1' AND (NOT('c2')))"));
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::booleanJdbcColumnHandle).collect(Collectors.toSet());
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
    }

    @Test
    public void testJdbcComputePartialPushdown()
    {
        String table = "test_table";
        String schema = "test_schema";

        // CAST(c1 AS varchar(1024)) = '123' cannot be pushed down, but c1 + c2 = 3 can.
        String expression = "CAST(c1 AS varchar(1024)) = '123' and c1 + c2 = 3";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        PlanNode original = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("(('c1' + 'c2') = ?)", ImmutableList.of(new ConstantExpression(3L, INTEGER))));
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::booleanJdbcColumnHandle).collect(Collectors.toSet());
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                "CAST(c1 AS varchar(1024)) = '123'",
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    @Test
    public void testJdbcComputePartialPushdownWithOrOperator()
    {
        String table = "test_table";
        String schema = "test_schema";

        // CAST(c1 AS varchar(1024)) = '123' cannot be pushed down, but c1 + c2 = 3 or c1 <> c2 can.
        String expression = "CAST(c1 AS varchar(1024)) = '123' and (c1 + c2 = 3 or c1 <> c2)";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        PlanNode original = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("((('c1' + 'c2') = ?) OR ('c1' <> 'c2'))", ImmutableList.of(new ConstantExpression(3L, INTEGER))));
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::booleanJdbcColumnHandle).collect(Collectors.toSet());
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                "CAST(c1 AS varchar(1024)) = '123'",
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    @Test
    public void testJdbcComputeNoPushdown()
    {
        String table = "test_table";
        String schema = "test_schema";

        // no filter can be pushed down
        String expression = "CAST(c1 AS varchar(1024)) = '123' and ((c1 - c2) > c2 or CAST(c2 AS varchar(1024)) = '456')";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        PlanNode original = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);

        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::booleanJdbcColumnHandle).collect(Collectors.toSet());
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, Optional.empty());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                expression,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    @Test
    public void testJdbcComputeLimitPushdownWithoutFilter()
    {
        String table = "test_table";
        String schema = "test_schema";

        PlanNode original = limit(8, jdbcTableScan(schema, table, BIGINT, "c1", "c2"));

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(Optional.empty(), OptionalLong.of(8));
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::booleanJdbcColumnHandle).collect(Collectors.toSet());
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
    }

    @Test
    public void testJdbcComputeLimitPushdownWithFilter()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "c1 + c2 = 3";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        FilterNode filter = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);
        PlanNode original = limit(8, filter);

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("(('c1' + 'c2') = ?)", ImmutableList.of(new ConstantExpression(3L, INTEGER))),
                8L);
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::booleanJdbcColumnHandle).collect(Collectors.toSet());
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns));
    }

    @Test
    public void testJdbcComputeLimitNotPushdownWithFilter()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "CAST(c1 AS varchar(1024)) = '123' and c1 + c2 = 3";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);
        FilterNode filter = filter(jdbcTableScan(schema, table, BIGINT, "c1", "c2"), rowExpression);
        PlanNode original = limit(8, filter);

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                new JdbcExpression("(('c1' + 'c2') = ?)", ImmutableList.of(new ConstantExpression(3L, INTEGER))));
        Set<ColumnHandle> columns = Stream.of("c1", "c2").map(TestJdbcComputePushdown::booleanJdbcColumnHandle).collect(Collectors.toSet());
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);

        assertPlanMatch(actual, PlanMatchPattern.limit(8L,
                PlanMatchPattern.filter(
                        "CAST(c1 AS varchar(1024)) = '123'",
                        JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns))));
    }

    @Test
    public void testSimpleOrderByLimitPushdown()
    {
        ImmutableList<JdbcSortItem> expected = ImmutableList.of(new JdbcSortItem(getColumnHandleForVariable("l_orderkey", jdbcColumnHandles), SortOrder.DESC_NULLS_FIRST));
        simpleOrderByLimitPushdownCommon(ImmutableList.of("l_orderkey"), ImmutableList.of(false), expected);

        expected = ImmutableList.of(new JdbcSortItem(getColumnHandleForVariable("l_orderkey", jdbcColumnHandles), SortOrder.DESC_NULLS_FIRST),
                new JdbcSortItem(getColumnHandleForVariable("l_extendedprice", jdbcColumnHandles), SortOrder.ASC_NULLS_FIRST));
        simpleOrderByLimitPushdownCommon(ImmutableList.of("l_orderkey", "l_extendedprice"), ImmutableList.of(false, true), expected);
    }

    private void simpleOrderByLimitPushdownCommon(List<String> orderingColumns, List<Boolean> ascending, List<JdbcSortItem> expected)
    {
        String table = "test_table";
        String schema = "test_schema";

        TableScanNode tableScanNode = jdbcTableScan();
        PlanNode original = topN(8, orderingColumns, ascending, tableScanNode);

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                Optional.empty(),
                Optional.of(expected),
                OptionalLong.of(8));

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, Sets.newHashSet(jdbcColumnHandles)));
    }

    @Test
    public void testOrderByLimitWithFilterPushdown()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "l_orderkey = 3 and l_commitdate between '2021-07-19' and '2021-07-20'";
        TableScanNode tableScanNode = jdbcTableScan();
        TypeProvider typeProvider = TypeProvider.fromVariables(tableScanNode.getOutputVariables());
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider);

        FilterNode filter = filter(tableScanNode, rowExpression);
        PlanNode original = topN(8, ImmutableList.of("l_orderkey", "l_extendedprice"), ImmutableList.of(false, true), filter);

        List<ConstantExpression> constantBindValues = ImmutableList.of(new ConstantExpression(3L, INTEGER),
                new ConstantExpression(Slices.utf8Slice("2021-07-19"), createVarcharType(10)),
                new ConstantExpression(Slices.utf8Slice("2021-07-20"), createVarcharType(10)));

        List<JdbcSortItem> sortOrder = ImmutableList.of(new JdbcSortItem(getColumnHandleForVariable("l_orderkey", jdbcColumnHandles), SortOrder.DESC_NULLS_FIRST),
                new JdbcSortItem(getColumnHandleForVariable("l_extendedprice", jdbcColumnHandles), SortOrder.ASC_NULLS_FIRST));

        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                Optional.of(new JdbcExpression("(('l_orderkey' = ?) AND ('l_commitdate' BETWEEN ? AND ?))", constantBindValues)),
                Optional.of(sortOrder),
                OptionalLong.of(8));

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, Sets.newHashSet(jdbcColumnHandles)));
    }

    @Test
    public void testOrderByLimitNotPushdown()
    {
        String table = "test_table";
        String schema = "test_schema";

        TableScanNode tableScanNode = jdbcTableScan();
        Map<VariableReferenceExpression, RowExpression> assignments = tableScanNode.getOutputVariables().stream()
                .map(v -> immutableEntry(v, v))
                .collect(toMap(Map.Entry::getKey,
                        Map.Entry::getValue,
                        (val1, val2) -> val1,
                        LinkedHashMap::new));

        Map<SqlFunctionId, SqlInvokedFunction> functions = ImmutableMap.of(getSqlFunctionId(), getSqlInvokedFunction());

        FunctionHandle functionHandle = typeManager.resolveFunction(
                Optional.of(functions),
                Optional.empty(),
                getSqlFunctionId().getFunctionName(),
                fromTypes(VARCHAR, BIGINT, BIGINT));

        List<RowExpression> arguments = Lists.newArrayList(new VariableReferenceExpression("l_commitdate", VARCHAR),
                new ConstantExpression(1L, BIGINT),
                new ConstantExpression(2L, BIGINT));
        assignments.put(new VariableReferenceExpression("substring", VARCHAR),
                new CallExpression("substring", functionHandle, VARCHAR, arguments));

        ProjectNode project = project(Assignments.copyOf(assignments), tableScanNode);
        PlanNode original = topN(8, ImmutableList.of("substring", "l_orderkey"), ImmutableList.of(false, true), project);

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, Optional.empty());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);

        Map<String, ExpressionMatcher> assignmentsMap = tableScanNode.getOutputVariables().stream().map(v ->
                immutableEntry(v.getName(), PlanMatchPattern.expression(v.getName())))
                .collect(toMap(Map.Entry::getKey,
                        Map.Entry::getValue,
                        (val1, val2) -> val1,
                        LinkedHashMap::new));

        assignmentsMap.put("substring", PlanMatchPattern.expression("substring(l_commitdate, 1, 2)"));
        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("substring", DESCENDING, FIRST), sort("l_orderkey", ASCENDING, FIRST));

        assertPlanMatch(actual,
                PlanMatchPattern.topN(8, orderBy,
                        PlanMatchPattern.project(assignmentsMap,
                                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, Sets.newHashSet(jdbcColumnHandles)))));
    }

    @Test
    public void testOrderByLimitWithFilterNotPushdown()
    {
        String table = "test_table";
        String schema = "test_schema";

        String expression = "l_orderkey = 3 and substring(l_commitdate, 1, 2) = '20'";
        TableScanNode tableScanNode = jdbcTableScan();
        TypeProvider typeProvider = TypeProvider.fromVariables(tableScanNode.getOutputVariables());
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(expression), typeProvider, createSessionWithTempFunctionSubstring());

        FilterNode filter = filter(tableScanNode, rowExpression);
        PlanNode original = topN(8, ImmutableList.of("l_orderkey", "l_extendedprice"), ImmutableList.of(false, true), filter);
        Optional<JdbcQueryGeneratorContext> context = getJdbcQueryGeneratorContext(
                Optional.of(new JdbcExpression("('l_orderkey' = ?)", ImmutableList.of(new ConstantExpression(3L, INTEGER)))),
                Optional.empty(),
                OptionalLong.empty());

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, context);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(
                jdbcTableHandle,
                TupleDomain.none());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);

        List<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("l_orderkey", DESCENDING, FIRST), sort("l_extendedprice", ASCENDING, FIRST));
        assertPlanMatch(actual,
                PlanMatchPattern.topN(8, orderBy,
                        PlanMatchPattern.filter("substring(l_commitdate, 1, 2) = '20'",
                                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, Sets.newHashSet(jdbcColumnHandles)))));
    }

    private Session createSessionWithTempFunctionSubstring()
    {
        return testSessionBuilder()
                .addSessionFunction(getSqlFunctionId(), getSqlInvokedFunction())
                .build();
    }

    private SqlFunctionId getSqlFunctionId()
    {
        return new SqlFunctionId(QualifiedObjectName.valueOf("presto.default.substring"),
                ImmutableList.of(parseTypeSignature("varchar"), parseTypeSignature("bigint"), parseTypeSignature("bigint")));
    }

    private SqlInvokedFunction getSqlInvokedFunction()
    {
        return new SqlInvokedFunction(
                getSqlFunctionId().getFunctionName(),
                ImmutableList.of(new Parameter("x", parseTypeSignature("varchar")),
                        new Parameter("y", parseTypeSignature("bigint")),
                        new Parameter("z", parseTypeSignature("bigint"))),
                parseTypeSignature("varchar"),
                "",
                RoutineCharacteristics.builder().build(),
                "",
                notVersioned());
    }

    private Optional<JdbcQueryGeneratorContext> getJdbcQueryGeneratorContext(JdbcExpression jdbcExpression, Long limit)
    {
        return getJdbcQueryGeneratorContext(Optional.of(jdbcExpression), OptionalLong.of(limit));
    }

    private Optional<JdbcQueryGeneratorContext> getJdbcQueryGeneratorContext(JdbcExpression jdbcExpression)
    {
        return getJdbcQueryGeneratorContext(Optional.of(jdbcExpression), OptionalLong.empty());
    }

    private Optional<JdbcQueryGeneratorContext> getJdbcQueryGeneratorContext(Optional<JdbcExpression> jdbcExpression, OptionalLong limit)
    {
        return getJdbcQueryGeneratorContext(jdbcExpression, Optional.empty(), limit);
    }

    private Optional<JdbcQueryGeneratorContext> getJdbcQueryGeneratorContext(Optional<JdbcExpression> jdbcExpression, Optional<List<JdbcSortItem>> sortOrder, OptionalLong limit)
    {
        return Optional.of(new JdbcQueryGeneratorContext(jdbcExpression, sortOrder, limit));
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
        return new JdbcColumnHandle(CONNECTOR_ID, name, JDBC_BIGINT, BIGINT, false);
    }

    private static JdbcColumnHandle booleanJdbcColumnHandle(String name)
    {
        return new JdbcColumnHandle(CONNECTOR_ID, name, JDBC_BOOLEAN, BOOLEAN, false);
    }

    private static JdbcColumnHandle getColumnHandleForVariable(String name, List<JdbcColumnHandle> jdbcColumnHandles)
    {
        return jdbcColumnHandles.stream()
                .filter(h -> h.getColumnName().equalsIgnoreCase(name))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot find jdbcColumnHandle " + name));
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
        PlanAssert.assertPlan(
                TEST_SESSION,
                METADATA,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, typeProvider, StatsAndCosts.empty()),
                expected);
    }

    private TableScanNode jdbcTableScan(String schema, String table, Type type, String... columnNames)
    {
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, Optional.empty());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.none());
        TableHandle tableHandle = new TableHandle(new ConnectorId(CATALOG_NAME), jdbcTableHandle, new ConnectorTransactionHandle() {}, Optional.of(jdbcTableLayoutHandle));

        return PLAN_BUILDER.tableScan(
                tableHandle,
                Arrays.stream(columnNames).map(column -> newVariable(column, type)).collect(toImmutableList()),
                Arrays.stream(columnNames)
                        .map(column -> newVariable(column, type))
                        .collect(toMap(identity(), entry -> getColumnHandleForVariable(entry.getName(), type))));
    }

    private TableScanNode jdbcTableScan()
    {
        String table = "test_table";
        String schema = "test_schema";
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table, Optional.empty());
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.none());
        TableHandle tableHandle = new TableHandle(new ConnectorId(CATALOG_NAME), jdbcTableHandle, new ConnectorTransactionHandle() {}, Optional.of(jdbcTableLayoutHandle));

        return PLAN_BUILDER.tableScan(
                tableHandle,
                jdbcColumnHandles.stream().map(column -> newVariable(column.getColumnName(), column.getColumnType())).collect(toImmutableList()),
                jdbcColumnHandles.stream()
                        .map(column -> newVariable(column.getColumnName(), column.getColumnType()))
                        .collect(toMap(identity(), entry -> getColumnHandleForVariable(entry.getName(), jdbcColumnHandles))));
    }

    private ProjectNode project(Assignments assignments, PlanNode source)
    {
        return PLAN_BUILDER.project(assignments, source);
    }

    private FilterNode filter(PlanNode source, RowExpression predicate)
    {
        return PLAN_BUILDER.filter(predicate, source);
    }

    private LimitNode limit(long count, PlanNode source)
    {
        return PLAN_BUILDER.limit(count, source);
    }

    private TopNNode topN(long count, List<String> orderingColumns, List<Boolean> ascending, PlanNode source)
    {
        ImmutableList<Ordering> ordering = IntStream.range(0, orderingColumns.size())
                .boxed()
                .map(i -> new Ordering(variable(source.getOutputVariables(), orderingColumns.get(i)), ascending.get(i) ? SortOrder.ASC_NULLS_FIRST : SortOrder.DESC_NULLS_FIRST))
                .collect(toImmutableList());

        return new TopNNode(PLAN_BUILDER.getIdAllocator().getNextId(), source, count, new OrderingScheme(ordering), TopNNode.Step.PARTIAL);
    }

    private VariableReferenceExpression variable(List<VariableReferenceExpression> outputVariables, String name)
    {
        return outputVariables.stream().filter(v -> v.getName().equals(name))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot find variable " + name));
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

            JdbcTableHandle expectedTableHandle = jdbcTableLayoutHandle.getTable();
            JdbcTableHandle actualTableHandle = layoutHandle.getTable();

            if (jdbcTableLayoutHandle.getTable().equals(layoutHandle.getTable())
                    && jdbcTableLayoutHandle.getTupleDomain().equals(layoutHandle.getTupleDomain())
                    && ((!expectedTableHandle.getContext().isPresent() && !actualTableHandle.getContext().isPresent())
                    || expectedTableHandle.getContext().get().equals(actualTableHandle.getContext().get()))) {
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
