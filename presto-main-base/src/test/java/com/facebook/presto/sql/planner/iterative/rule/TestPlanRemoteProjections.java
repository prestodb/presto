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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DesugarTryExpressionRewriter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.optimizations.ExternalCallExpressionChecker;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.FunctionImplementationType.THRIFT;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.PlanRemoteProjections.ProjectionContext;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPlanRemoteProjections
{
    private static final QualifiedObjectName REMOTE_FOO = QualifiedObjectName.valueOf(new CatalogSchemaName("unittest", "memory"), "remote_foo");
    private static final RoutineCharacteristics.Language JAVA = new RoutineCharacteristics.Language("java");

    private static final SqlInvokedFunction FUNCTION_REMOTE_FOO_0 = new SqlInvokedFunction(
            REMOTE_FOO,
            ImmutableList.of(),
            parseTypeSignature(StandardTypes.INTEGER),
            "remote_foo()",
            RoutineCharacteristics.builder().setLanguage(JAVA).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction FUNCTION_REMOTE_FOO_1 = new SqlInvokedFunction(
            REMOTE_FOO,
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.INTEGER))),
            parseTypeSignature(StandardTypes.INTEGER),
            "remote_foo(x)",
            RoutineCharacteristics.builder().setLanguage(JAVA).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction FUNCTION_REMOTE_FOO_2 = new SqlInvokedFunction(
            REMOTE_FOO,
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.INTEGER)), new Parameter("y", parseTypeSignature(StandardTypes.INTEGER))),
            parseTypeSignature(StandardTypes.INTEGER),
            "remote_foo(x, y)",
            RoutineCharacteristics.builder().setLanguage(JAVA).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction FUNCTION_REMOTE_FOO_3 = new SqlInvokedFunction(
            REMOTE_FOO,
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.INTEGER)), new Parameter("y", parseTypeSignature(StandardTypes.INTEGER)), new Parameter("z", parseTypeSignature(DOUBLE))),
            parseTypeSignature(StandardTypes.INTEGER),
            "remote_foo(x, y, z)",
            RoutineCharacteristics.builder().setLanguage(JAVA).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());

    private RuleTester tester;

    @BeforeClass
    public void setup()
    {
        tester = new RuleTester(ImmutableList.of(), ImmutableMap.of("remote_functions_enabled", "true"));
        FunctionAndTypeManager functionAndTypeManager = getFunctionAndTypeManager();
        functionAndTypeManager.addFunctionNamespace(
                "unittest",
                new InMemoryFunctionNamespaceManager(
                        "unittest",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(
                                        SQL, FunctionImplementationType.SQL,
                                        JAVA, THRIFT),
                                new NoopSqlFunctionExecutor()),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("sql,java")));
        functionAndTypeManager.createFunction(FUNCTION_REMOTE_FOO_0, true);
        functionAndTypeManager.createFunction(FUNCTION_REMOTE_FOO_1, true);
        functionAndTypeManager.createFunction(FUNCTION_REMOTE_FOO_2, true);
        functionAndTypeManager.createFunction(FUNCTION_REMOTE_FOO_3, true);
    }

    @Test
    void testLocalOnly()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("abs(x) + abs(y)"))
                .put(planBuilder.variable("b", BOOLEAN), planBuilder.rowExpression("x is null and y is null"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 1);
        assertEquals(rewritten.get(0).getProjections().size(), 2);
    }

    @Test
    void testRemoteOnly()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo()"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("unittest.memory.remote_foo(unittest.memory.remote_foo())"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 2);
        assertEquals(rewritten.get(1).getProjections().size(), 2);
    }

    @Test
    void testRemoteAndLocal()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo(x, y + unittest.memory.remote_foo(x))"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("abs(x)"))
                .put(planBuilder.variable("c"), planBuilder.rowExpression("abs(unittest.memory.remote_foo())"))
                .put(planBuilder.variable("d"), planBuilder.rowExpression("unittest.memory.remote_foo(x + y, abs(x))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 4);
        assertEquals(rewritten.get(3).getProjections().size(), 4);
    }

    @Test
    void testSpecialForm()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo(x, y + unittest.memory.remote_foo(x))"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("x IS NULL OR y IS NULL"))
                .put(planBuilder.variable("c"), planBuilder.rowExpression("IF(abs(unittest.memory.remote_foo()) > 0, x, y)"))
                .put(planBuilder.variable("d"), planBuilder.rowExpression("unittest.memory.remote_foo(x + y, abs(x))"))
                .put(planBuilder.variable("e"), planBuilder.rowExpression("TRUE OR FALSE"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 4);
        assertEquals(rewritten.get(3).getProjections().size(), 5);
    }

    @Test
    void testJsonPathArgumentKeptInline()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("json_extract_scalar(CAST(unittest.memory.remote_foo() AS VARCHAR), '$.key')"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 2);
        // Verify no projection context extracts a JsonPath-typed variable
        for (ProjectionContext context : rewritten) {
            context.getProjections().keySet().forEach(variable ->
                    assertFalse(variable.getType().equals(JSON_PATH),
                            "JsonPath type should not be extracted as a ProjectNode output"));
        }
    }

    @Test
    void testRemoteFunctionRewrite()
    {
        tester.assertThat(new PlanRemoteProjections(getFunctionAndTypeManager()))
                .on(p -> {
                    p.variable("x", INTEGER);
                    p.variable("y", INTEGER);
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a"), p.rowExpression("unittest.memory.remote_foo(x)"))
                                    .put(p.variable("b"), p.rowExpression("unittest.memory.remote_foo(unittest.memory.remote_foo())"))
                                    .build(),
                            p.values(p.variable("x", INTEGER)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("a"),
                                        "b", PlanMatchPattern.expression("unittest.memory.remote_foo(unittest_memory_remote_foo)")),
                                project(
                                        ImmutableMap.of(
                                                "a", PlanMatchPattern.expression("unittest.memory.remote_foo(x)"),
                                                "unittest_memory_remote_foo", PlanMatchPattern.expression("unittest.memory.remote_foo()")),
                                        project(
                                                ImmutableMap.of("x", PlanMatchPattern.expression("x")),
                                                values(ImmutableMap.of("x", 0))))));
    }

    @Test
    void testMixedExpressionRewrite()
    {
        tester.assertThat(new PlanRemoteProjections(getFunctionAndTypeManager()))
                .on(p -> {
                    p.variable("x", INTEGER);
                    p.variable("y", INTEGER);
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a"), p.rowExpression("unittest.memory.remote_foo(1, y + unittest.memory.remote_foo(x))"))
                                    .put(p.variable("b"), p.rowExpression("x IS NULL OR y IS NULL"))
                                    .put(p.variable("c"), p.rowExpression("abs(unittest.memory.remote_foo()) > 0"))
                                    .put(p.variable("d"), p.rowExpression("unittest.memory.remote_foo(x + y, abs(1))"))
                                    .build(),
                            p.values(p.variable("x", INTEGER), p.variable("y", INTEGER)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("unittest.memory.remote_foo(expr, add)"),
                                        "b", PlanMatchPattern.expression("b"),
                                        "c", PlanMatchPattern.expression("c"),
                                        "d", PlanMatchPattern.expression("d")),
                                project(
                                        ImmutableMap.of(
                                                "add", PlanMatchPattern.expression("y + unittest_memory_remote_foo"),
                                                "expr", PlanMatchPattern.expression("expr"),
                                                "b", PlanMatchPattern.expression("b"),
                                                "c", PlanMatchPattern.expression("abs(unittest_memory_remote_foo_7) > 0"),
                                                "d", PlanMatchPattern.expression("d")),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("y", PlanMatchPattern.expression("y"))
                                                        .put("expr", PlanMatchPattern.expression("expr"))
                                                        .put("unittest_memory_remote_foo", PlanMatchPattern.expression("unittest.memory.remote_foo(x)"))
                                                        .put("b", PlanMatchPattern.expression("b"))
                                                        .put("unittest_memory_remote_foo_7", PlanMatchPattern.expression("unittest.memory.remote_foo()"))
                                                        .put("d", PlanMatchPattern.expression("unittest.memory.remote_foo(add_10, abs_12)"))
                                                        .build(),
                                                project(
                                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                                .put("x", PlanMatchPattern.expression("x"))
                                                                .put("y", PlanMatchPattern.expression("y"))
                                                                .put("expr", PlanMatchPattern.expression("1"))
                                                                .put("b", PlanMatchPattern.expression("x IS NULL OR y is NULL"))
                                                                .put("add_10", PlanMatchPattern.expression("x + y"))
                                                                .put("abs_12", PlanMatchPattern.expression("abs(1)"))
                                                                .build(),
                                                        values(ImmutableMap.of("x", 0, "y", 1)))))));
    }

    @Test
    void testTryWithRemoteFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(remote_foo(x)) should extract the remote function from the lambda body
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(unittest.memory.remote_foo(x))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        // Expect: local(args) -> remote(remote_foo) -> local($internal$try)
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithNoRemoteFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(abs(x)) has no remote functions, should be fully local
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(abs(x))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 1);
        assertFalse(rewritten.get(0).isRemote());
    }

    @Test
    void testTryWithLocalWrappingRemoteFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(abs(remote_foo(x))): local function wrapping remote function inside TRY
        // The local function should stay inside the lambda, only the remote function is extracted
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(abs(unittest.memory.remote_foo(x)))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        // Expect: local(args) -> remote(remote_foo) -> local($internal$try(() -> abs(v)))
        // The local abs() is merged into the lambda, avoiding two consecutive local projections
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithRemoteWrappingLocalFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(remote_foo(abs(x))): remote function wrapping local function inside TRY
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(unittest.memory.remote_foo(abs(x)))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        // Expect: local(abs(x)) -> remote(remote_foo(v)) -> local($internal$try(() -> v))
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithLocalRemoteLocalNesting()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(abs(remote_foo(abs(x)))): local(remote(local(x))) nesting
        // local_func2 is extracted as remote arg, local_func stays inside the lambda
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(abs(unittest.memory.remote_foo(abs(x))))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        // Expect: local(abs(x)) -> remote(remote_foo(v)) -> local($internal$try(() -> abs(v)))
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithRemoteFunctionMixed()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // Mix of TRY with remote function and regular remote function
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(unittest.memory.remote_foo(x))"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("unittest.memory.remote_foo(y)"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        // Both should produce remote projections
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithRemoteFunctionAndArithmetic()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(remote_foo(x) + 3): remote function with local arithmetic inside TRY
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(unittest.memory.remote_foo(x) + 3)"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        // Expect: local(args) -> remote(remote_foo) -> local($internal$try(() -> add(v, 3)))
        // The local add is folded into the lambda body
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithNestedTryAndRemoteFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(TRY(remote_foo(x))): nested TRY with remote function
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(TRY(unittest.memory.remote_foo(x)))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithCaseAndRemoteFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(CASE WHEN x > 0 THEN remote_foo(x) ELSE 0 END)
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(CASE WHEN x > 0 THEN unittest.memory.remote_foo(x) ELSE 0 END)"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithIfAndRemoteFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(IF(x > 0, remote_foo(x), -1))
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(IF(x > 0, unittest.memory.remote_foo(x), -1))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
    }

    @Test
    void testTryWithDeeplyNestedRemoteFunctions()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // TRY(abs(remote_foo(abs(remote_foo(x))))): two remote calls nested with local functions
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "TRY(abs(unittest.memory.remote_foo(abs(unittest.memory.remote_foo(x)))))"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 5);
        assertFalse(rewritten.get(0).isRemote());
        assertEquals(rewritten.get(1).isRemote(), true);
        assertFalse(rewritten.get(2).isRemote());
        assertEquals(rewritten.get(3).isRemote(), true);
        assertFalse(rewritten.get(4).isRemote());
    }

    @Test
    void testExternalCallExpressionCheckerDetectsRemoteInLambda()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        ExternalCallExpressionChecker checker = new ExternalCallExpressionChecker(getFunctionAndTypeManager());

        // TRY(remote_foo(x)) produces $internal$try(BIND(x, (x_bind) -> remote_foo(x_bind)))
        // The checker should detect the remote function inside the lambda body
        RowExpression tryWithRemote = desugaredRowExpression(planBuilder, "TRY(unittest.memory.remote_foo(x))");
        assertTrue(tryWithRemote.accept(checker, null));
    }

    @Test
    void testExternalCallExpressionCheckerLocalOnlyLambda()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        ExternalCallExpressionChecker checker = new ExternalCallExpressionChecker(getFunctionAndTypeManager());

        // TRY(abs(x)) produces $internal$try(BIND(x, (x_bind) -> abs(x_bind)))
        // The checker should not flag a lambda with only local functions
        RowExpression tryWithLocal = desugaredRowExpression(planBuilder, "TRY(abs(x))");
        assertFalse(tryWithLocal.accept(checker, null));
    }

    @Test
    void testNullIfWithTryAndRemoteFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // NULLIF(TRY(remote_foo(x)), 0) compiles to SWITCH($internal$try(BIND(...)), WHEN(0, NULL), $internal$try(BIND(...)))
        // The WHEN clause must be kept inline, not extracted as a standalone variable
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "NULLIF(TRY(unittest.memory.remote_foo(x)), 0)"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        // Should not crash and should produce valid projections
        assertFalse(rewritten.isEmpty());
        // Verify no WHEN clause was extracted as a standalone projection
        for (ProjectionContext context : rewritten) {
            for (RowExpression expr : context.getProjections().values()) {
                if (expr instanceof SpecialFormExpression) {
                    SpecialFormExpression sf = (SpecialFormExpression) expr;
                    assertFalse(sf.getForm() == SpecialFormExpression.Form.WHEN,
                            "WHEN clause should not be a standalone projection value");
                }
            }
        }
    }

    @Test
    void testCaseWhenWithVariableAndTryRemoteFunction()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemoteProjections rule = new PlanRemoteProjections(getFunctionAndTypeManager());
        // CASE WHEN y > 0 THEN 0 ELSE TRY(remote_foo(x)) END
        // Compiles to SWITCH(true, WHEN(y > 0, 0), $internal$try(BIND(x, (p) -> remote_foo(p))))
        // The WHEN clause references 'y', which must be tracked through projection layers
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), desugaredRowExpression(planBuilder, "CASE WHEN y > 0 THEN 0 ELSE TRY(unittest.memory.remote_foo(x)) END"))
                .build(), new VariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 3);
        assertFalse(rewritten.get(0).isRemote());
        assertTrue(rewritten.get(1).isRemote());
        assertFalse(rewritten.get(2).isRemote());

        // Verify 'y' is not directly referenced in the final local projection.
        // It should be available through an intermediate variable allocated in the first local projection.
        Set<String> firstLocalOutputNames = rewritten.get(0).getProjections().keySet().stream()
                .map(VariableReferenceExpression::getName)
                .collect(java.util.stream.Collectors.toSet());
        // The first local projection should have extracted 'y > 0' into a variable
        boolean hasYDerivedVar = rewritten.get(0).getProjections().values().stream()
                .anyMatch(expr -> !(expr instanceof VariableReferenceExpression) && VariablesExtractor.extractAll(expr).stream()
                        .anyMatch(v -> v.getName().equals("y")));
        assertTrue(hasYDerivedVar, "First local projection should contain an expression referencing y");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Remote functions are not enabled")
    public void testRemoteFunctionDisabled()
    {
        RuleTester tester = new RuleTester(ImmutableList.of());
        FunctionAndTypeManager functionAndTypeManager = tester.getMetadata().getFunctionAndTypeManager();
        functionAndTypeManager.addFunctionNamespace(
                "unittest",
                new InMemoryFunctionNamespaceManager(
                        "unittest",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(
                                        SQL, FunctionImplementationType.SQL,
                                        JAVA, THRIFT),
                                new NoopSqlFunctionExecutor()),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("sql,java")));
        functionAndTypeManager.createFunction(FUNCTION_REMOTE_FOO_1, true);
        tester.assertThat(new PlanRemoteProjections(functionAndTypeManager))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a"), p.rowExpression("unittest.memory.remote_foo(x)"))
                                    .build(),
                            p.values(p.variable("x", INTEGER)));
                })
                .matches(anyTree());
    }

    private Metadata getMetadata()
    {
        return tester.getMetadata();
    }

    private FunctionAndTypeManager getFunctionAndTypeManager()
    {
        return getMetadata().getFunctionAndTypeManager();
    }

    /**
     * Translates a SQL expression to a RowExpression with TRY desugaring and lambda capture
     * desugaring applied, matching the real query execution path where $internal$try arguments
     * are wrapped in BIND rather than being raw LambdaDefinitionExpressions.
     */
    private RowExpression desugaredRowExpression(PlanBuilder planBuilder, String sql)
    {
        Expression expression = PlanBuilder.expression(sql);
        expression = DesugarTryExpressionRewriter.rewrite(expression);
        TypeProvider types = planBuilder.getTypes();
        expression = LambdaCaptureDesugaringRewriter.rewrite(expression, new VariableAllocator(types.allVariables()));
        Map<NodeRef<Expression>, com.facebook.presto.common.type.Type> expressionTypes = getExpressionTypes(
                TEST_SESSION,
                getMetadata(),
                new SqlParser(),
                types,
                expression,
                ImmutableMap.of(),
                WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(
                expression,
                expressionTypes,
                ImmutableMap.of(),
                getFunctionAndTypeManager(),
                TEST_SESSION);
    }
}
