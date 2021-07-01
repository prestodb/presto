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
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.FunctionImplementationType.THRIFT;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.TypeProvider.viewOf;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestInlineSqlFunctions
{
    private static final RoutineCharacteristics.Language JAVA = new RoutineCharacteristics.Language("java");
    private static final SqlInvokedFunction SQL_FUNCTION_SQUARE = new SqlInvokedFunction(
            QualifiedObjectName.valueOf(new CatalogSchemaName("unittest", "memory"), "square"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(INTEGER))),
            parseTypeSignature(INTEGER),
            "square",
            RoutineCharacteristics.builder()
                    .setDeterminism(DETERMINISTIC)
                    .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                    .build(),
            "RETURN x * x",
            notVersioned());

    private static final SqlInvokedFunction THRIFT_FUNCTION_FOO = new SqlInvokedFunction(
            QualifiedObjectName.valueOf(new CatalogSchemaName("unittest", "memory"), "foo"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(INTEGER))),
            parseTypeSignature(INTEGER),
            "thrift function foo",
            RoutineCharacteristics.builder()
                    .setLanguage(JAVA)
                    .setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                    .build(),
            "",
            notVersioned());

    private static final SqlInvokedFunction SQL_FUNCTION_ADD_1_TO_INT_ARRAY = new SqlInvokedFunction(
            QualifiedObjectName.valueOf(new CatalogSchemaName("unittest", "memory"), "add_1_int"),
            ImmutableList.of(new Parameter("x", parseTypeSignature("array(int)"))),
            parseTypeSignature("array(int)"),
            "add 1 to all elements of array",
            RoutineCharacteristics.builder()
                    .setDeterminism(DETERMINISTIC)
                    .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                    .build(),
            "RETURN transform(x, x -> x + 1)",
            notVersioned());

    private static final SqlInvokedFunction SQL_FUNCTION_ADD_1_TO_BIGINT_ARRAY = new SqlInvokedFunction(
            QualifiedObjectName.valueOf(new CatalogSchemaName("unittest", "memory"), "add_1_bigint"),
            ImmutableList.of(new Parameter("x", parseTypeSignature("array(bigint)"))),
            parseTypeSignature("array(bigint)"),
            "add 1 to all elements of array",
            RoutineCharacteristics.builder()
                    .setDeterminism(DETERMINISTIC)
                    .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                    .build(),
            "RETURN transform(x, x -> x + 1)",
            notVersioned());

    private RuleTester tester;

    @BeforeTest
    public void setup()
    {
        RuleTester tester = new RuleTester();
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
        functionAndTypeManager.createFunction(SQL_FUNCTION_SQUARE, true);
        functionAndTypeManager.createFunction(THRIFT_FUNCTION_FOO, true);
        functionAndTypeManager.createFunction(SQL_FUNCTION_ADD_1_TO_INT_ARRAY, true);
        functionAndTypeManager.createFunction(SQL_FUNCTION_ADD_1_TO_BIGINT_ARRAY, true);
        this.tester = tester;
    }

    @Test
    public void testInlineFunction()
    {
        assertInlined(tester, "unittest.memory.square(x)", "x * x", ImmutableMap.of("x", IntegerType.INTEGER));
    }

    @Test
    public void testInlineFunctionInsideFunction()
    {
        assertInlined(tester, "abs(unittest.memory.square(x))", "abs(x * x)", ImmutableMap.of("x", IntegerType.INTEGER));
    }

    @Test
    public void testInlineFunctionContainingLambda()
    {
        assertInlined(tester, "unittest.memory.add_1_int(x)", "transform(x, \"x$lambda\" -> \"x$lambda\" + 1)", ImmutableMap.of("x", new ArrayType(IntegerType.INTEGER)));
    }

    @Test
    public void testInlineSqlFunctionCoercesConstantWithCast()
    {
        assertInlined(tester,
                "unittest.memory.add_1_bigint(x)",
                "transform(x, \"x$lambda\" -> \"x$lambda\" + CAST(1 AS bigint))",
                ImmutableMap.of("x", new ArrayType(BigintType.BIGINT)));
    }

    @Test
    public void testInlineBuiltinSqlFunction()
    {
        assertInlined(tester,
                "array_sum(x)",
                "reduce(x, BIGINT '0', (\"s$lambda\", \"x$lambda\") -> \"s$lambda\" + COALESCE(\"x$lambda\", BIGINT '0'), \"s$lambda_0\" -> \"s$lambda_0\")",
                ImmutableMap.of("x", new ArrayType(IntegerType.INTEGER)));
    }

    @Test
    public void testNoInlineThriftFunction()
    {
        assertInlined(tester, "unittest.memory.foo(x)", "unittest.memory.foo(x)", ImmutableMap.of("x", IntegerType.INTEGER));
    }

    @Test
    public void testInlineFunctionIntoPlan()
    {
        tester.assertThat(new InlineSqlFunctions(tester.getMetadata(), tester.getSqlParser()).projectExpressionRewrite())
                .on(p -> p.project(
                        assignment(
                                p.variable("squared"),
                                new FunctionCall(QualifiedName.of("unittest", "memory", "square"), ImmutableList.of(new SymbolReference("a")))),
                        p.values(p.variable("a", IntegerType.INTEGER))))
                .matches(project(
                        ImmutableMap.of("squared", expression("x * x")),
                        values(ImmutableMap.of("x", 0))));
    }

    @Test
    public void testNoInlineIntoPlanWhenInlineIsDisabled()
    {
        tester.assertThat(new InlineSqlFunctions(tester.getMetadata(), tester.getSqlParser()).projectExpressionRewrite())
                .setSystemProperty("inline_sql_functions", "false")
                .on(p -> p.project(
                        assignment(
                                p.variable("squared"),
                                new FunctionCall(QualifiedName.of("unittest", "memory", "square"), ImmutableList.of(new SymbolReference("a")))),
                        p.values(p.variable("a", IntegerType.INTEGER))))
                .doesNotFire();
    }

    private void assertInlined(RuleTester tester, String inputSql, String expected, Map<String, Type> variableTypes)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("inline_sql_functions", "true")
                .build();
        Metadata metadata = tester.getMetadata();
        Expression inputSqlExpression = PlanBuilder.expression(inputSql);
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                tester.getSqlParser(),
                viewOf(variableTypes),
                inputSqlExpression,
                ImmutableList.of(),
                WarningCollector.NOOP);
        Expression inlinedExpression = InlineSqlFunctions.InlineSqlFunctionsRewriter.rewrite(
                inputSqlExpression,
                session,
                metadata,
                new PlanVariableAllocator(variableTypes.entrySet().stream()
                        .map(entry -> new VariableReferenceExpression(entry.getKey(), entry.getValue()))
                        .collect(toImmutableList())),
                expressionTypes);
        inlinedExpression = ExpressionUtils.rewriteIdentifiersToSymbolReferences(inlinedExpression);
        Expression expectedExpression = PlanBuilder.expression(expected);
        assertEquals(inlinedExpression, expectedExpression);
    }
}
