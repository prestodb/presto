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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerContext;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.BigintOperators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.annotations.ScalarFromAnnotationsParser.parseFunctionDefinitions;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.CPP;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.JAVA;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

/**
 * These are plan tests similar to what we have for other optimizers (e.g. {@link com.facebook.presto.sql.planner.TestPredicatePushdown})
 * They test that the plan for a query after the optimizer runs is as expected.
 * These are separate from {@link TestAddExchanges} because those are unit tests for
 * how layouts get chosen.
 * <p>
 * Key behavior tested: When CPP functions are used with system tables, the filter containing
 * the CPP function is preserved above the exchange (not pushed down) to ensure the filter
 * executes in a different fragment from the system table scan. This validates the fragment
 * boundary between CPP function evaluation and system table access.
 */
public class TestAddExchangesPlansWithFunctions
        extends BasePlanTest
{
    private static final String NO_OP_OPTIMIZER = "no-op-optimizer";

    public TestAddExchangesPlansWithFunctions()
    {
        super(TestAddExchangesPlansWithFunctions::createTestQueryRunner);
    }

    private static final SqlInvokedFunction CPP_FOO = new SqlInvokedFunction(
            new QualifiedObjectName("dummy", "unittest", "cpp_foo"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature(StandardTypes.BIGINT),
            "cpp_foo(x)",
            RoutineCharacteristics.builder().setLanguage(CPP).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction CPP_BAZ = new SqlInvokedFunction(
            new QualifiedObjectName("dummy", "unittest", "cpp_baz"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature(StandardTypes.BIGINT),
            "cpp_baz(x)",
            RoutineCharacteristics.builder().setLanguage(CPP).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction JAVA_BAR = new SqlInvokedFunction(
            new QualifiedObjectName("dummy", "unittest", "java_bar"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature(StandardTypes.BIGINT),
            "java_bar(x)",
            RoutineCharacteristics.builder().setLanguage(JAVA).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction JAVA_FEE = new SqlInvokedFunction(
            new QualifiedObjectName("dummy", "unittest", "java_fee"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature(StandardTypes.BIGINT),
            "java_fee(x)",
            RoutineCharacteristics.builder().setLanguage(JAVA).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction NOT = new SqlInvokedFunction(
            new QualifiedObjectName("dummy", "unittest", "not"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BOOLEAN))),
            parseTypeSignature(StandardTypes.BOOLEAN),
            "not(x)",
            RoutineCharacteristics.builder().setLanguage(CPP).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction CPP_ARRAY_CONSTRUCTOR = new SqlInvokedFunction(
            new QualifiedObjectName("dummy", "unittest", "array_constructor"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT)), new Parameter("y", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature("array(bigint)"),
            "array_constructor(x, y)",
            RoutineCharacteristics.builder().setLanguage(CPP).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static LocalQueryRunner createTestQueryRunner()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("expression_optimizer_name", NO_OP_OPTIMIZER)
                .build(),
                new FeaturesConfig(),
                new FunctionsConfig().setDefaultNamespacePrefix("dummy.unittest"));
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(), ImmutableMap.of());
        queryRunner.getMetadata().getFunctionAndTypeManager().addFunctionNamespace(
                "dummy",
                new InMemoryFunctionNamespaceManager(
                        "dummy",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(
                                        CPP, FunctionImplementationType.CPP,
                                        JAVA, FunctionImplementationType.JAVA),
                                new NoopSqlFunctionExecutor()),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("cpp")));
        queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(CPP_FOO, true);
        queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(CPP_BAZ, true);
        queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(JAVA_BAR, true);
        queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(JAVA_FEE, true);
        queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(NOT, true);
        queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(CPP_ARRAY_CONSTRUCTOR, true);
        parseFunctionDefinitions(BigintOperators.class).stream()
                .map(TestAddExchangesPlansWithFunctions::convertToSqlInvokedFunction)
                .forEach(function -> queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(function, true));
        parseFunctionDefinitions(CombineHashFunction.class).stream()
                .map(TestAddExchangesPlansWithFunctions::convertToSqlInvokedFunction)
                .forEach(function -> queryRunner.getMetadata().getFunctionAndTypeManager().createFunction(function, true));
        queryRunner.getExpressionManager().addExpressionOptimizerFactory(new NoOpExpressionOptimizerFactory());
        queryRunner.getExpressionManager().loadExpressionOptimizerFactory(NO_OP_OPTIMIZER, NO_OP_OPTIMIZER, ImmutableMap.of());
        return queryRunner;
    }

    public static SqlInvokedFunction convertToSqlInvokedFunction(SqlScalarFunction scalarFunction)
    {
        QualifiedObjectName functionName = new QualifiedObjectName("dummy", "unittest", scalarFunction.getSignature().getName().getObjectName());
        TypeSignature returnType = scalarFunction.getSignature().getReturnType();
        RoutineCharacteristics characteristics = RoutineCharacteristics.builder()
                .setLanguage(RoutineCharacteristics.Language.JAVA) // Assuming JAVA as the language
                .setDeterminism(RoutineCharacteristics.Determinism.DETERMINISTIC)
                .setNullCallClause(RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT)
                .build();

        // Convert scalar function arguments to SqlInvokedFunction parameters
        ImmutableList<Parameter> parameters = scalarFunction.getSignature().getArgumentTypes().stream()
                .map(type -> new Parameter(type.toString(), TypeSignature.parseTypeSignature(type.toString())))
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));

        // Create the SqlInvokedFunction
        return new SqlInvokedFunction(
                functionName,
                parameters,
                returnType,
                scalarFunction.getSignature().getName().toString(), // Using the function name as the body for simplicity
                characteristics,
                "", // Empty description
                notVersioned());
    }

    @Test
    public void testFilterWithCppFunctionDoesNotGetPushedIntoSystemTableScan()
    {
        // java_fee and java_bar are java functions, they are both pushed down past the exchange
        assertNativeDistributedPlan("SELECT java_fee(ordinal_position) FROM information_schema.columns WHERE java_bar(ordinal_position) = 1",
                anyTree(
                        exchange(REMOTE_STREAMING, GATHER,
                                project(ImmutableMap.of("java_fee", expression("java_fee(ordinal_position)")),
                                        filter("java_bar(ordinal_position) = BIGINT'1'",
                                                tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position")))))));
        // cpp_foo is a CPP function, it is not pushed down past the exchange because the source is a system table scan
        // The filter is preserved above the exchange to prove that the filter is not in the same fragment as the system table scan
        assertNativeDistributedPlan("SELECT cpp_baz(ordinal_position) FROM information_schema.columns WHERE cpp_foo(ordinal_position) = 1",
                anyTree(
                        project(ImmutableMap.of("cpp_baz", expression("cpp_baz(ordinal_position)")),
                                filter("cpp_foo(ordinal_position) = BIGINT'1'",
                                        exchange(REMOTE_STREAMING, GATHER,
                                                tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position")))))));
    }

    @Test
    public void testJoinWithCppFunctionDoesNotGetPushedIntoSystemTableScan()
    {
        // java_bar is a java function, it is pushed down past the exchange
        assertNativeDistributedPlan(
                "SELECT c1.table_name FROM information_schema.columns c1 JOIN information_schema.columns c2 ON c1.ordinal_position = c2.ordinal_position WHERE java_bar(c1.ordinal_position) = 1",
                anyTree(
                        exchange(
                                join(INNER, ImmutableList.of(equiJoinClause("ordinal_position", "ordinal_position_4")),
                                        anyTree(
                                                exchange(REMOTE_STREAMING, GATHER,
                                                        project(
                                                                filter("java_bar(ordinal_position) = BIGINT'1'",
                                                                        tableScan("columns", ImmutableMap.of(
                                                                                "ordinal_position", "ordinal_position",
                                                                                "table_name", "table_name")))))),
                                        anyTree(
                                                exchange(REMOTE_STREAMING, GATHER,
                                                        project(
                                                                filter("java_bar(ordinal_position_4) = BIGINT'1'",
                                                                        tableScan("columns", ImmutableMap.of(
                                                                                "ordinal_position_4", "ordinal_position"))))))))));

        // cpp_foo is a CPP function, it is not pushed down past the exchange because the source is a system table scan
        assertNativeDistributedPlan(
                "SELECT cpp_baz(c1.ordinal_position) FROM information_schema.columns c1 JOIN information_schema.columns c2 ON c1.ordinal_position = c2.ordinal_position WHERE cpp_foo(c1.ordinal_position) = 1",
                output(
                        exchange(
                                project(ImmutableMap.of("cpp_baz", expression("cpp_baz(ordinal_position)")),
                                        join(INNER, ImmutableList.of(equiJoinClause("ordinal_position", "ordinal_position_4")),
                                                anyTree(
                                                        filter("cpp_foo(ordinal_position) = BIGINT'1'",
                                                                exchange(REMOTE_STREAMING, GATHER,
                                                                        project(
                                                                                tableScan("columns", ImmutableMap.of(
                                                                                        "ordinal_position", "ordinal_position")))))),
                                                anyTree(
                                                        filter("cpp_foo(ordinal_position_4) = BIGINT'1'",
                                                                exchange(REMOTE_STREAMING, GATHER,
                                                                        project(
                                                                                tableScan("columns", ImmutableMap.of(
                                                                                        "ordinal_position_4", "ordinal_position")))))))))));
    }

    @Test
    public void testMixedFunctionTypesInComplexPredicates()
    {
        // Test AND condition with mixed Java and CPP functions
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE java_bar(ordinal_position) = 1 AND cpp_foo(ordinal_position) > 0",
                anyTree(
                        filter("java_bar(ordinal_position) = BIGINT'1' AND cpp_foo(ordinal_position) > BIGINT'0'",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));

        // Test OR condition with mixed functions - entire predicate should be evaluated after exchange
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE java_bar(ordinal_position) = 1 OR cpp_foo(ordinal_position) = 2",
                anyTree(
                        filter("java_bar(ordinal_position) = BIGINT'1' OR cpp_foo(ordinal_position) = BIGINT'2'",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testNestedFunctionCalls()
    {
        // CPP function nested inside Java function - should not push down
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE java_bar(cpp_foo(ordinal_position)) = 1",
                anyTree(
                        filter("java_bar(cpp_foo(ordinal_position)) = BIGINT'1'",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));

        // Java function nested inside CPP function - should not push down
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE cpp_foo(java_bar(ordinal_position)) = 1",
                anyTree(
                        filter("cpp_foo(java_bar(ordinal_position)) = BIGINT'1'",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));

        // Multiple levels of nesting
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE cpp_foo(java_bar(cpp_foo(ordinal_position))) = 1",
                anyTree(
                        filter("cpp_foo(java_bar(cpp_foo(ordinal_position))) = BIGINT'1'",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testMixedSystemAndRegularTables()
    {
        // System table with CPP function joined with regular table
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns c JOIN nation n ON c.ordinal_position = n.nationkey WHERE cpp_foo(c.ordinal_position) = 1",
                output(
                        join(INNER, ImmutableList.of(equiJoinClause("ordinal_position", "nationkey")),
                                filter("cpp_foo(ordinal_position) = BIGINT'1'",
                                        exchange(REMOTE_STREAMING, GATHER,
                                                project(ImmutableMap.of("ordinal_position", expression("ordinal_position")),
                                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))),
                                anyTree(
                                        project(ImmutableMap.of("nationkey", expression("nationkey")),
                                                filter(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))))));

        // Regular table with CPP function (should work normally without extra exchange)
        assertNativeDistributedPlan(
                "SELECT * FROM nation WHERE cpp_foo(nationkey) = 1",
                anyTree(
                        exchange(REMOTE_STREAMING, GATHER,
                                filter("cpp_foo(nationkey) = BIGINT'1'",
                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))));
    }

    @Test
    public void testAggregationsWithMixedFunctions()
    {
        // Aggregation with CPP function in GROUP BY
        assertNativeDistributedPlan(
                "SELECT DISTINCT cpp_foo(ordinal_position) FROM information_schema.columns",
                anyTree(
                        project(ImmutableMap.of("cpp_foo", expression("cpp_foo(ordinal_position)")),
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));

        // Aggregation with Java function in GROUP BY - can be pushed down
        assertNativeDistributedPlan(
                "SELECT DISTINCT java_bar(ordinal_position) FROM information_schema.columns",
                anyTree(
                        exchange(REMOTE_STREAMING, GATHER,
                                project(ImmutableMap.of("java_bar", expression("java_bar(ordinal_position)")),
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testComplexPredicateWithMultipleFunctions()
    {
        // Complex predicate with multiple CPP and Java functions
        // Since the predicate contains CPP functions (cpp_foo, baz), the exchange is inserted before the system table scan
        // The RemoveRedundantExchanges rule removes the inner exchange that was added by ExtractIneligiblePredicatesFromSystemTableScans
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE (cpp_foo(ordinal_position) > 0 AND java_bar(ordinal_position) < 100) OR cpp_baz(ordinal_position) = 50",
                anyTree(
                        filter(
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testProjectionWithMixedFunctions()
    {
        // Projection with both Java and CPP functions
        assertNativeDistributedPlan(
                "SELECT java_bar(ordinal_position) as java_result, cpp_foo(ordinal_position) as cpp_result FROM information_schema.columns",
                anyTree(
                        project(ImmutableMap.of(
                                        "java_result", expression("java_bar(ordinal_position)"),
                                        "cpp_result", expression("cpp_foo(ordinal_position)")),
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testCaseStatementsWithCppFunctions()
    {
        // CASE statement with CPP function in condition
        // The RemoveRedundantExchanges optimizer removes the redundant exchange
        assertNativeDistributedPlan(
                "SELECT CASE WHEN cpp_foo(ordinal_position) > 0 THEN 'positive' ELSE 'negative' END FROM information_schema.columns",
                anyTree(
                        project(
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));

        // CASE statement with CPP function in result
        // The RemoveRedundantExchanges optimizer removes the redundant exchange
        assertNativeDistributedPlan(
                "SELECT CASE WHEN ordinal_position > 0 THEN cpp_foo(ordinal_position) ELSE 0 END FROM information_schema.columns",
                anyTree(
                        project(
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testBuiltinFunctionWithExplicitNamespace()
    {
        // Test that built-in functions with explicit namespace are handled correctly
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE presto.default.length(table_name) > 10",
                anyTree(
                        exchange(REMOTE_STREAMING, GATHER,
                                filter("length(table_name) > BIGINT'10'",
                                        tableScan("columns", ImmutableMap.of("table_name", "table_name"))))));
    }

    @Test(enabled = false)  // TODO: Window functions are resolved with namespace which causes issues in tests
    public void testWindowFunctionsWithCppFunctions()
    {
        // Window function with CPP function in partition by
        assertNativeDistributedPlan(
                "SELECT row_number() OVER (PARTITION BY cpp_foo(ordinal_position)) FROM information_schema.columns",
                anyTree(
                        exchange(
                                project(
                                        anyTree(
                                                project(
                                                        exchange(REMOTE_STREAMING, GATHER,
                                                                tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position")))))))));

        // Window function with CPP function in order by
        assertNativeDistributedPlan(
                "SELECT row_number() OVER (ORDER BY cpp_foo(ordinal_position)) FROM information_schema.columns",
                anyTree(
                        exchange(
                                project(
                                        anyTree(
                                                project(
                                                        exchange(REMOTE_STREAMING, GATHER,
                                                                tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position")))))))));
    }

    @Test
    public void testMultipleSystemTableJoins()
    {
        // Multiple system tables with CPP functions
        // This test verifies that when joining two system tables with a CPP function comparison,
        // an exchange is added between the table scan and the join to ensure CPP functions
        // execute in a separate fragment from system table access
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns c1 " +
                        "JOIN information_schema.columns c2 ON cpp_foo(c1.ordinal_position) = cpp_foo(c2.ordinal_position)",
                anyTree(
                        exchange(
                                join(INNER, ImmutableList.of(equiJoinClause("cpp_foo", "foo_4")),
                                        exchange(
                                                project(ImmutableMap.of("cpp_foo", expression("cpp_foo")),
                                                        project(ImmutableMap.of("cpp_foo", expression("cpp_foo(ordinal_position)")),
                                                                exchange(REMOTE_STREAMING, GATHER,
                                                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position")))))),
                                        anyTree(
                                                exchange(
                                                        project(ImmutableMap.of("foo_4", expression("foo_4")),
                                                                project(ImmutableMap.of("foo_4", expression("cpp_foo(ordinal_position_4)")),
                                                                        exchange(REMOTE_STREAMING, GATHER,
                                                                                tableScan("columns", ImmutableMap.of("ordinal_position_4", "ordinal_position")))))))))));
    }

    @Test
    public void testInPredicateWithCppFunction()
    {
        // IN predicate with CPP function
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE cpp_foo(ordinal_position) IN (1, 2, 3)",
                anyTree(
                        filter("cpp_foo(ordinal_position) IN (BIGINT'1', BIGINT'2', BIGINT'3')",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testBetweenPredicateWithCppFunction()
    {
        // BETWEEN predicate with CPP function
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE cpp_foo(ordinal_position) BETWEEN 1 AND 10",
                anyTree(
                        filter("cpp_foo(ordinal_position) BETWEEN BIGINT'1' AND BIGINT'10'",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testNullHandlingWithCppFunctions()
    {
        // IS NULL check with CPP function
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE cpp_foo(ordinal_position) IS NULL",
                anyTree(
                        filter("cpp_foo(ordinal_position) IS NULL",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));

        // COALESCE with CPP function
        // The RemoveRedundantExchanges optimizer removes the redundant exchange
        assertNativeDistributedPlan(
                "SELECT COALESCE(cpp_foo(ordinal_position), 0) FROM information_schema.columns",
                anyTree(
                        project(
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testUnionWithCppFunctions()
    {
        // UNION ALL with CPP functions from system tables
        assertNativeDistributedPlan(
                "SELECT cpp_foo(ordinal_position) FROM information_schema.columns " +
                        "UNION ALL SELECT cpp_foo(nationkey) FROM nation",
                output(
                        exchange(
                                anyTree(
                                        exchange(REMOTE_STREAMING, GATHER,
                                                tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position")))),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))));
    }

    @Test
    public void testExistsSubqueryWithCppFunction()
    {
        // EXISTS subquery with CPP function
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns c WHERE EXISTS (SELECT 1 FROM nation n WHERE cpp_foo(c.ordinal_position) = n.nationkey)",
                anyTree(
                        join(
                                anyTree(
                                        exchange(REMOTE_STREAMING, GATHER,
                                                tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position")))),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))));
    }

    @Test
    public void testLimitWithCppFunction()
    {
        // LIMIT with CPP function in ORDER BY
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns ORDER BY cpp_foo(ordinal_position) LIMIT 10",
                output(
                        project(
                                anyTree(
                                        project(
                                                exchange(REMOTE_STREAMING, GATHER,
                                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))))));
    }

    @Test
    public void testCastOperationsWithCppFunctions()
    {
        // CAST operations with CPP functions
        // The RemoveRedundantExchanges optimizer removes the redundant exchange
        assertNativeDistributedPlan(
                "SELECT CAST(cpp_foo(ordinal_position) AS VARCHAR) FROM information_schema.columns",
                anyTree(
                        project(
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testArrayConstructorWithCppFunction()
    {
        // Array constructor with CPP function
        assertNativeDistributedPlan(
                "SELECT ARRAY[cpp_foo(ordinal_position), cpp_baz(ordinal_position)] FROM information_schema.columns",
                anyTree(
                        project(
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testRowConstructorWithCppFunction()
    {
        // ROW constructor with CPP function
        // The RemoveRedundantExchanges optimizer removes the redundant exchange
        assertNativeDistributedPlan(
                "SELECT ROW(cpp_foo(ordinal_position), table_name) FROM information_schema.columns",
                anyTree(
                        project(
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of(
                                                "ordinal_position", "ordinal_position",
                                                "table_name", "table_name"))))));
    }

    @Test
    public void testIsNotNullWithCppFunction()
    {
        // IS NOT NULL check with CPP function
        assertNativeDistributedPlan(
                "SELECT * FROM information_schema.columns WHERE cpp_foo(ordinal_position) IS NOT NULL",
                anyTree(
                        filter("cpp_foo(ordinal_position) IS NOT NULL",
                                exchange(REMOTE_STREAMING, GATHER,
                                        tableScan("columns", ImmutableMap.of("ordinal_position", "ordinal_position"))))));
    }

    @Test
    public void testComplexJoinWithMultipleCppFunctions()
    {
        // Complex join with multiple CPP functions in different positions
        // The filters are pushed into FilterProject nodes and the join happens on the expression cpp_foo(c1.ordinal_position)
        assertNativeDistributedPlan(
                "SELECT c1.table_name, n.name FROM information_schema.columns c1 " +
                        "JOIN nation n ON cpp_foo(c1.ordinal_position) = n.nationkey " +
                        "WHERE cpp_baz(c1.ordinal_position) > 0 AND cpp_foo(n.nationkey) < 100",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("cpp_foo", "nationkey")),
                                project(ImmutableMap.of("table_name", expression("table_name"), "cpp_foo", expression("cpp_foo")),
                                        project(ImmutableMap.of("cpp_foo", expression("cpp_foo(ordinal_position)")),
                                                filter("cpp_baz(ordinal_position) > BIGINT'0' AND cpp_foo(cpp_foo(ordinal_position)) < BIGINT'100'",
                                                        exchange(REMOTE_STREAMING, GATHER,
                                                                tableScan("columns", ImmutableMap.of(
                                                                        "ordinal_position", "ordinal_position",
                                                                        "table_name", "table_name")))))),
                                anyTree(
                                        project(ImmutableMap.of("nationkey", expression("nationkey"),
                                                        "name", expression("name")),
                                                filter("cpp_foo(nationkey) < BIGINT'100'",
                                                        tableScan("nation", ImmutableMap.of(
                                                                "nationkey", "nationkey",
                                                                "name", "name"))))))));
    }

    @Test
    public void testSystemTableFilterWithOutputVariableMismatch()
    {
        assertNativeDistributedPlan(
                "SELECT table_name FROM information_schema.columns WHERE cpp_foo(ordinal_position) > 5",
                output(
                        project(ImmutableMap.of("table_name", expression("table_name")),
                                filter("cpp_foo(ordinal_position) > BIGINT'5'",
                                        exchange(REMOTE_STREAMING, GATHER,
                                                tableScan("columns", ImmutableMap.of(
                                                        "ordinal_position", "ordinal_position",
                                                        "table_name", "table_name")))))));
    }

    @Test
    public void testSystemTableFilterWithMultipleColumnsAndPartialSelection()
    {
        assertNativeDistributedPlan(
                "SELECT table_schema, table_name FROM information_schema.columns " +
                        "WHERE cpp_foo(ordinal_position) > 0 AND cpp_baz(ordinal_position) < 100",
                output(
                        project(ImmutableMap.of("table_schema", expression("table_schema"),
                                               "table_name", expression("table_name")),
                                filter("cpp_foo(ordinal_position) > BIGINT'0' AND cpp_baz(ordinal_position) < BIGINT'100'",
                                        exchange(REMOTE_STREAMING, GATHER,
                                                tableScan("columns", ImmutableMap.of(
                                                        "ordinal_position", "ordinal_position",
                                                        "table_schema", "table_schema",
                                                        "table_name", "table_name")))))));
    }

    private static class NoOpExpressionOptimizerFactory
            implements ExpressionOptimizerFactory
    {
        @Override
        public ExpressionOptimizer createOptimizer(Map<String, String> config, ExpressionOptimizerContext context)
        {
            return new NoOpExpressionOptimizer();
        }

        @Override
        public String getName()
        {
            return NO_OP_OPTIMIZER;
        }
    }

    private static class NoOpExpressionOptimizer
            implements ExpressionOptimizer
    {
        @Override
        public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
        {
            return expression;
        }
    }
}
