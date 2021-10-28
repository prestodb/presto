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
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

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
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.PlanRemotePojections.ProjectionContext;
import static org.testng.Assert.assertEquals;

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
                                null),
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

        PlanRemotePojections rule = new PlanRemotePojections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("abs(x) + abs(y)"))
                .put(planBuilder.variable("b", BOOLEAN), planBuilder.rowExpression("x is null and y is null"))
                .build(), new PlanVariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 1);
        assertEquals(rewritten.get(0).getProjections().size(), 2);
    }

    @Test
    void testRemoteOnly()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());

        PlanRemotePojections rule = new PlanRemotePojections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo()"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("unittest.memory.remote_foo(unittest.memory.remote_foo())"))
                .build(), new PlanVariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 2);
        assertEquals(rewritten.get(1).getProjections().size(), 2);
    }

    @Test
    void testRemoteAndLocal()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemotePojections rule = new PlanRemotePojections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo(x, y + unittest.memory.remote_foo(x))"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("abs(x)"))
                .put(planBuilder.variable("c"), planBuilder.rowExpression("abs(unittest.memory.remote_foo())"))
                .put(planBuilder.variable("d"), planBuilder.rowExpression("unittest.memory.remote_foo(x + y, abs(x))"))
                .build(), new PlanVariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 4);
        assertEquals(rewritten.get(3).getProjections().size(), 4);
    }

    @Test
    void testSpecialForm()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), getMetadata());
        planBuilder.variable("x", INTEGER);
        planBuilder.variable("y", INTEGER);

        PlanRemotePojections rule = new PlanRemotePojections(getFunctionAndTypeManager());
        List<ProjectionContext> rewritten = rule.planRemoteAssignments(Assignments.builder()
                .put(planBuilder.variable("a"), planBuilder.rowExpression("unittest.memory.remote_foo(x, y + unittest.memory.remote_foo(x))"))
                .put(planBuilder.variable("b"), planBuilder.rowExpression("x IS NULL OR y IS NULL"))
                .put(planBuilder.variable("c"), planBuilder.rowExpression("IF(abs(unittest.memory.remote_foo()) > 0, x, y)"))
                .put(planBuilder.variable("d"), planBuilder.rowExpression("unittest.memory.remote_foo(x + y, abs(x))"))
                .put(planBuilder.variable("e"), planBuilder.rowExpression("TRUE OR FALSE"))
                .build(), new PlanVariableAllocator(planBuilder.getTypes().allVariables()));
        assertEquals(rewritten.size(), 4);
        assertEquals(rewritten.get(3).getProjections().size(), 5);
    }

    @Test
    void testRemoteFunctionRewrite()
    {
        tester.assertThat(new PlanRemotePojections(getFunctionAndTypeManager()))
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
        tester.assertThat(new PlanRemotePojections(getFunctionAndTypeManager()))
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
                                null),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("sql,java")));
        functionAndTypeManager.createFunction(FUNCTION_REMOTE_FOO_1, true);
        tester.assertThat(new PlanRemotePojections(functionAndTypeManager))
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
}
