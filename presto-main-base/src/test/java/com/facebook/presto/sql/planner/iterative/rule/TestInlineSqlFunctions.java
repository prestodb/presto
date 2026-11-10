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
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP_WITH_TIMEZONE;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.function.FunctionImplementationType.THRIFT;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.Expressions.subExpressions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestInlineSqlFunctions
        extends BaseRuleTest
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

    @BeforeClass
    public final void setup()
    {
        tester = new RuleTester();
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
        functionAndTypeManager.createFunction(SQL_FUNCTION_SQUARE, true);
        functionAndTypeManager.createFunction(THRIFT_FUNCTION_FOO, true);
        functionAndTypeManager.createFunction(SQL_FUNCTION_ADD_1_TO_INT_ARRAY, true);
        functionAndTypeManager.createFunction(SQL_FUNCTION_ADD_1_TO_BIGINT_ARRAY, true);
    }

    @Test
    public void testInlineFunction()
    {
        assertInlined("unittest.memory.square(x)", "x * x", "x", IntegerType.INTEGER);
    }

    @Test
    public void testInlineFunctionInsideFunction()
    {
        assertInlined("abs(unittest.memory.square(x))", "abs(x * x)", "x", IntegerType.INTEGER);
    }

    @Test
    public void testInlineFunctionContainingLambda()
    {
        assertInlined(
                "unittest.memory.add_1_int(x)",
                "transform(x, \"x$lambda\" -> \"x$lambda\" + 1)",
                "x",
                new ArrayType(IntegerType.INTEGER));
    }

    @Test
    public void testInlineSqlFunctionCoercesConstantWithCast()
    {
        assertInlined(
                "unittest.memory.add_1_bigint(x)",
                "transform(x, \"x$lambda\" -> \"x$lambda\" + CAST(1 AS bigint))",
                "x",
                new ArrayType(BigintType.BIGINT));
    }

    @Test
    public void testNoInlineThriftFunction()
    {
        assertNotInlined("unittest.memory.foo(x)", "x", IntegerType.INTEGER);
    }

    @Test
    public void testNoInlineIntoPlanWhenInlineIsDisabled()
    {
        assertNotInlined("unittest.memory.square(x)",
                ImmutableMap.of("inline_sql_functions", "false"),
                "x",
                IntegerType.INTEGER);
    }

    @Test
    public void testInlineFunctionUsesNativeAtTimeZoneName()
    {
        MetadataManager metadata = createNativeAtTimeZoneMetadata(
                "at_zone",
                ImmutableList.of(new Parameter("x", parseTypeSignature(TIMESTAMP_WITH_TIME_ZONE))),
                "RETURN x AT TIME ZONE 'UTC'");
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, "false")
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .build();
        RowExpression input = new TestingRowExpressionTranslator(metadata, session).translate(
                "unittest_native.memory.at_zone(x)",
                ImmutableMap.of("x", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE));

        RowExpression rewritten = InlineSqlFunctions.InlineSqlFunctionsRewriter.rewrite(input, session, metadata);
        assertTrue(rewritten instanceof CallExpression);
        assertEquals(((CallExpression) rewritten).getDisplayName(), "at_timezone");
    }

    @Test
    public void testInterpreterUsesNativeAtTimeZoneNameInCompositeSqlFunction()
    {
        MetadataManager metadata = createNativeAtTimeZoneMetadata(
                "at_zone_composite",
                ImmutableList.of(),
                "RETURN IF(unittest_native.memory.remote_timestamp() IS NULL, " +
                        "CAST(NULL AS TIMESTAMP), unittest_native.memory.remote_timestamp() AT TIME ZONE 'UTC')");
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, "false")
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .build();
        RowExpression input = new TestingRowExpressionTranslator(metadata, session).translate(
                "unittest_native.memory.at_zone_composite()",
                ImmutableMap.of());

        RowExpression optimized = (RowExpression) new RowExpressionInterpreter(
                input,
                functionAndTypeManager,
                session.toConnectorSession(),
                OPTIMIZED).optimize();
        assertTrue(subExpressions(optimized).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .anyMatch(call -> call.getDisplayName().equals("at_timezone")));
        assertFalse(subExpressions(optimized).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .anyMatch(call -> call.getDisplayName().equals("at_timezone_convert")));
    }

    private static MetadataManager createNativeAtTimeZoneMetadata(String functionName, List<Parameter> parameters, String body)
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager(
                new FunctionsConfig().setLegacyTimestampWithTimezone(false));
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        functionAndTypeManager.addFunctionNamespace(
                "unittest_native",
                new InMemoryFunctionNamespaceManager(
                        "unittest_native",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(
                                        SQL, FunctionImplementationType.SQL,
                                        JAVA, THRIFT),
                                new NoopSqlFunctionExecutor()),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("sql,java")));
        functionAndTypeManager.createFunction(
                new SqlInvokedFunction(
                        QualifiedObjectName.valueOf(new CatalogSchemaName("unittest_native", "memory"), "remote_timestamp"),
                        ImmutableList.of(),
                        parseTypeSignature(TIMESTAMP_WITH_TIME_ZONE),
                        "remote timestamp",
                        RoutineCharacteristics.builder()
                                .setLanguage(JAVA)
                                .setDeterminism(DETERMINISTIC)
                                .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                                .build(),
                        "",
                        notVersioned()),
                true);
        functionAndTypeManager.createFunction(
                new SqlInvokedFunction(
                        QualifiedObjectName.valueOf(new CatalogSchemaName("unittest_native", "memory"), functionName),
                        parameters,
                        parseTypeSignature(TIMESTAMP),
                        "at zone",
                        RoutineCharacteristics.builder()
                                .setDeterminism(DETERMINISTIC)
                                .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                                .build(),
                        body,
                        notVersioned()),
                true);
        return metadata;
    }

    protected void assertInlined(String inputExpressionStr, String expectedExpressionStr, String variable, Type type)
    {
        RowExpression inputExpression = new TestingRowExpressionTranslator(tester.getMetadata()).translate(inputExpressionStr, ImmutableMap.of(variable, type));

        tester().assertThat(new InlineSqlFunctions(tester.getMetadata()).projectRowExpressionRewriteRule())
                .on(p -> p.project(assignment(p.variable("var"), inputExpression), p.values(p.variable(variable, type))))
                .matches(project(ImmutableMap.of("var", expression(expectedExpressionStr)), values(variable)));
    }

    private void assertNotInlined(String expression, String variable, Type type)
    {
        assertNotInlined(expression, ImmutableMap.of(), variable, type);
    }

    private void assertNotInlined(String expression, Map<String, String> sessionValues, String variable, Type type)
    {
        RowExpression inputExpression = new TestingRowExpressionTranslator(tester.getMetadata()).translate(expression, ImmutableMap.of(variable, type));
        RuleAssert ruleAssert = tester.assertThat(new SimplifyCardinalityMap(createTestFunctionAndTypeManager()).projectRowExpressionRewriteRule());
        sessionValues.forEach((k, v) -> ruleAssert.setSystemProperty(k, v));
        ruleAssert
                .on(p -> p.project(assignment(p.variable("var"), inputExpression), p.values(p.variable(variable, type))))
                .doesNotFire();
    }
}
