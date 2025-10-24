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
package com.facebook.presto.sql.relational;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.block.BlockAssertions.toValues;
import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_ARRAY_CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_MAP_CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_ROW_CAST;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.CPP;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.JAVA;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestRowExpressionOptimizer
{
    private static final SqlInvokedFunction CPP_FOO = new SqlInvokedFunction(
            new QualifiedObjectName("native", "default", "sqrt"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature(StandardTypes.DOUBLE),
            "sqrt(x)",
            RoutineCharacteristics.builder().setLanguage(CPP).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());
    private static final SqlInvokedFunction CPP_BAR = new SqlInvokedFunction(
            new QualifiedObjectName("native", "default", "cbrt"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature(StandardTypes.DOUBLE),
            "cbrt(x)",
            RoutineCharacteristics.builder().setLanguage(CPP).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());
    private static final SqlInvokedFunction CPP_CUSTOM_FUNCTION = new SqlInvokedFunction(
            new QualifiedObjectName("native", "default", "cpp_custom_func"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature(StandardTypes.BIGINT),
            "cpp_custom_func(x)",
            RoutineCharacteristics.builder().setLanguage(CPP).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
            "",
            notVersioned());
    private static final String nativePrefix = "native.default";

    private static final RowExpression CUBE_ROOT_EXP = call(
            "cbrt",
            new SqlFunctionHandle(
                    new SqlFunctionId(
                            QualifiedObjectName.valueOf(format("%s.cbrt", nativePrefix)),
                            ImmutableList.of(BIGINT.getTypeSignature())),
                    "1"),
            DOUBLE,
            ImmutableList.of(
                    constant(27L, BIGINT)));

    private static final RowExpression SQUARE_ROOT_EXP = call(
                "sqrt",
                new SqlFunctionHandle(
                        new SqlFunctionId(
                                QualifiedObjectName.valueOf(format("%s.sqrt", nativePrefix)),
                                ImmutableList.of(BIGINT.getTypeSignature())),
                        "1"),
                DOUBLE,
                ImmutableList.of(
                        constant(64L, BIGINT)));

    private FunctionAndTypeManager functionAndTypeManager;
    private RowExpressionOptimizer optimizer;

    @BeforeClass
    public void setUp()
    {
        functionAndTypeManager = createTestFunctionAndTypeManager();
        optimizer = new RowExpressionOptimizer(MetadataManager.createTestMetadataManager());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        optimizer = null;
    }

    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        RowExpression expression = constant(1L, BIGINT);
        for (int i = 0; i < 100; i++) {
            FunctionHandle functionHandle = functionAndTypeManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT));
            expression = new CallExpression(ADD.name(), functionHandle, BIGINT, ImmutableList.of(expression, constant(1L, BIGINT)));
        }
        optimize(expression);
    }

    @Test
    public void testIfConstantOptimization()
    {
        assertEquals(optimize(ifExpression(constant(true, BOOLEAN), 1L, 2L)), constant(1L, BIGINT));
        assertEquals(optimize(ifExpression(constant(false, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));
        assertEquals(optimize(ifExpression(constant(null, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));

        FunctionHandle bigintEquals = functionAndTypeManager.resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT));
        RowExpression condition = new CallExpression(EQUAL.name(), bigintEquals, BOOLEAN, ImmutableList.of(constant(3L, BIGINT), constant(3L, BIGINT)));
        assertEquals(optimize(ifExpression(condition, 1L, 2L)), constant(1L, BIGINT));
    }

    @Test
    public void testCastWithJsonParseOptimization()
    {
        FunctionHandle jsonParseFunctionHandle = functionAndTypeManager.lookupFunction("json_parse", fromTypes(VARCHAR));

        // constant
        FunctionHandle jsonCastFunctionHandle = functionAndTypeManager.lookupCast(CAST, JSON, functionAndTypeManager.getType(parseTypeSignature("array(integer)")));
        RowExpression jsonCastExpression = new CallExpression(CAST.name(), jsonCastFunctionHandle, new ArrayType(INTEGER), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, constant(utf8Slice("[1, 2]"), VARCHAR))));
        RowExpression resultExpression = optimize(jsonCastExpression);
        assertInstanceOf(resultExpression, ConstantExpression.class);
        Object resultValue = ((ConstantExpression) resultExpression).getValue();
        assertInstanceOf(resultValue, IntArrayBlock.class);
        assertEquals(toValues(INTEGER, (IntArrayBlock) resultValue), ImmutableList.of(1, 2));

        // varchar to array
        jsonCastFunctionHandle = functionAndTypeManager.lookupCast(CAST, JSON, functionAndTypeManager.getType(parseTypeSignature("array(varchar)")));
        jsonCastExpression = call(CAST.name(), jsonCastFunctionHandle, new ArrayType(VARCHAR), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(JSON_TO_ARRAY_CAST.name(), functionAndTypeManager.lookupCast(JSON_TO_ARRAY_CAST, VARCHAR, functionAndTypeManager.getType(parseTypeSignature("array(varchar)"))), new ArrayType(VARCHAR), field(1, VARCHAR)));

        // varchar to map
        jsonCastFunctionHandle = functionAndTypeManager.lookupCast(CAST, JSON, functionAndTypeManager.getType(parseTypeSignature("map(integer,varchar)")));
        jsonCastExpression = call(CAST.name(), jsonCastFunctionHandle, mapType(INTEGER, VARCHAR), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(JSON_TO_MAP_CAST.name(), functionAndTypeManager.lookupCast(JSON_TO_MAP_CAST, VARCHAR, functionAndTypeManager.getType(parseTypeSignature("map(integer, varchar)"))), mapType(INTEGER, VARCHAR), field(1, VARCHAR)));

        // varchar to row
        jsonCastFunctionHandle = functionAndTypeManager.lookupCast(CAST, JSON, functionAndTypeManager.getType(parseTypeSignature("row(varchar,bigint)")));
        jsonCastExpression = call(CAST.name(), jsonCastFunctionHandle, RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), ImmutableList.of(call("json_parse", jsonParseFunctionHandle, JSON, field(1, VARCHAR))));
        resultExpression = optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(JSON_TO_ROW_CAST.name(), functionAndTypeManager.lookupCast(JSON_TO_ROW_CAST, VARCHAR, functionAndTypeManager.getType(parseTypeSignature("row(varchar,bigint)"))), RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), field(1, VARCHAR)));
    }

    @Test
    public void testDefaultExpressionOptimizerUsesJavaNamespaceForBuiltInFunctions()
    {
        RowExpressionOptimizer nativeOptimizer = getNativeOptimizer();
        assertEquals(nativeOptimizer.optimize(SQUARE_ROOT_EXP, OPTIMIZED, SESSION), constant(8.0, DOUBLE));
        assertThrows(IllegalArgumentException.class, () -> optimizer.optimize(SQUARE_ROOT_EXP, OPTIMIZED, SESSION));

        assertEquals(nativeOptimizer.optimize(CUBE_ROOT_EXP, OPTIMIZED, SESSION), constant(3.0, DOUBLE));
        assertThrows(IllegalArgumentException.class, () -> optimizer.optimize(CUBE_ROOT_EXP, OPTIMIZED, SESSION));
    }

    @Test
    public void testFunctionNotInPrestoDefaultNamespaceIsNotEvaluated()
    {
        RowExpressionOptimizer nativeOptimizer = getNativeOptimizer();

        // Create a call expression to the custom native function
        RowExpression customFunctionCall = call(
                "cpp_custom_func",
                new SqlFunctionHandle(
                        new SqlFunctionId(
                                QualifiedObjectName.valueOf(format("%s.cpp_custom_func", nativePrefix)),
                                ImmutableList.of(BIGINT.getTypeSignature())),
                        "1"),
                BIGINT,
                ImmutableList.of(constant(42L, BIGINT)));

        // The function should not be evaluated since it doesn't exist in presto.default namespace
        // It should return the original call expression unchanged
        RowExpression optimized = nativeOptimizer.optimize(customFunctionCall, OPTIMIZED, SESSION);
        assertEquals(optimized, customFunctionCall);
        assertInstanceOf(optimized, CallExpression.class);

        // Verify that the function handle remains the same (not replaced)
        CallExpression optimizedCall = (CallExpression) optimized;
        assertEquals(optimizedCall.getFunctionHandle().getCatalogSchemaName().toString(), nativePrefix);

        // Create a call expression to the custom native function with a sqrt call expression arg
        RowExpression customFunctionWithCallExpressionCall = call(
                "cpp_custom_func",
                new SqlFunctionHandle(
                        new SqlFunctionId(
                                QualifiedObjectName.valueOf(format("%s.cpp_custom_func", nativePrefix)),
                                ImmutableList.of(BIGINT.getTypeSignature())),
                        "1"),
                BIGINT,
                ImmutableList.of(SQUARE_ROOT_EXP));

        // The inner CallExpression should be optimized, but the outer shouldn't since the function doesn't exist in presto.default namespace
        optimized = nativeOptimizer.optimize(customFunctionWithCallExpressionCall, OPTIMIZED, SESSION);
        assertEquals(
                optimized,
                call(
                        "cpp_custom_func",
                        new SqlFunctionHandle(
                                new SqlFunctionId(
                                        QualifiedObjectName.valueOf(format("%s.cpp_custom_func", nativePrefix)),
                                        ImmutableList.of(BIGINT.getTypeSignature())),
                                "1"),
                        BIGINT,
                        ImmutableList.of(constant(8.0, DOUBLE))));
        assertInstanceOf(optimized, CallExpression.class);
        // Verify that the function handle remains the same (not replaced)
        optimizedCall = (CallExpression) optimized;
        assertEquals(optimizedCall.getFunctionHandle().getCatalogSchemaName().toString(), nativePrefix);
        assertEquals(optimizedCall.getChildren().get(0), constant(8.0, DOUBLE));
    }

    @Test
    public void testSpecialFormExpressionsWhenDefaultNamespaceIsSwitched()
    {
        RowExpressionOptimizer nativeOptimizer = getNativeOptimizer();

        RowExpression leftCondition = callOperator(
                GREATER_THAN,
                SQUARE_ROOT_EXP,
                constant(5.0, DOUBLE));

        RowExpression rightCondition = callOperator(
                LESS_THAN,
                CUBE_ROOT_EXP,
                constant(10.0, DOUBLE));

        RowExpression andExpr = new SpecialFormExpression(
                SpecialFormExpression.Form.AND,
                BOOLEAN,
                ImmutableList.of(leftCondition, rightCondition));

        RowExpression optimized = nativeOptimizer.optimize(andExpr, OPTIMIZED, SESSION);
        assertEquals(optimized, constant(true, BOOLEAN));

        // Lambda expressions inside Special form expressions
        List<Type> lambdaTypes = ImmutableList.of(DOUBLE, DOUBLE);
        LambdaDefinitionExpression lambda =
                new LambdaDefinitionExpression(
                        Optional.empty(),
                        lambdaTypes,
                        ImmutableList.of("s", "x"),
                        callOperator(ADD, SQUARE_ROOT_EXP, CUBE_ROOT_EXP));

        andExpr = new SpecialFormExpression(
                SpecialFormExpression.Form.AND,
                BOOLEAN,
                ImmutableList.of(lambda, rightCondition));

        // rightCondition is always true, hence the expression should be reduced to "(s, x) -> 11.0".
        optimized = nativeOptimizer.optimize(andExpr, OPTIMIZED, SESSION);

        assertInstanceOf(optimized, LambdaDefinitionExpression.class);
        LambdaDefinitionExpression lambdaExpression = (LambdaDefinitionExpression) optimized;
        assertEquals(lambdaExpression.getArgumentTypes(), lambdaTypes);
        assertEquals(lambdaExpression.getBody(), constant(11.0, DOUBLE));
    }

    @Test
    public void testLambdaExpressionsWhenDefaultNamespaceIsSwitched()
    {
        RowExpressionOptimizer nativeOptimizer = getNativeOptimizer();

        List<Type> lambdaTypes = ImmutableList.of(DOUBLE, DOUBLE);

        LambdaDefinitionExpression lambda =
                new LambdaDefinitionExpression(
                        Optional.empty(),
                        lambdaTypes,
                        ImmutableList.of("s", "x"),
                        callOperator(ADD, SQUARE_ROOT_EXP, CUBE_ROOT_EXP));

        LambdaDefinitionExpression nestedLambda =
                new LambdaDefinitionExpression(
                        Optional.empty(),
                        ImmutableList.of(DOUBLE),
                        ImmutableList.of("y"),
                        lambda);

        RowExpression optimized = nativeOptimizer.optimize(lambda, OPTIMIZED, SESSION);

        assertInstanceOf(optimized, LambdaDefinitionExpression.class);
        LambdaDefinitionExpression lambdaExpression = (LambdaDefinitionExpression) optimized;
        assertEquals(lambdaExpression.getArgumentTypes(), lambdaTypes);
        assertEquals(lambdaExpression.getBody(), constant(11.0, DOUBLE));

        // Nested lambda
        RowExpression optimizedOuterLambda = nativeOptimizer.optimize(nestedLambda, OPTIMIZED, SESSION);

        assertInstanceOf(optimizedOuterLambda, LambdaDefinitionExpression.class);
        LambdaDefinitionExpression outerLambdaExpression = (LambdaDefinitionExpression) optimizedOuterLambda;
        assertEquals(outerLambdaExpression.getArgumentTypes(), ImmutableList.of(DOUBLE));
        assertInstanceOf(outerLambdaExpression.getBody(), LambdaDefinitionExpression.class);

        LambdaDefinitionExpression innerLambdaExpression = (LambdaDefinitionExpression) outerLambdaExpression.getBody();
        assertEquals(innerLambdaExpression.getArgumentTypes(), lambdaTypes);
        assertEquals(innerLambdaExpression.getBody(), constant(11.0, DOUBLE));
    }

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        return new SpecialFormExpression(IF, BIGINT, ImmutableList.of(condition, constant(trueValue, BIGINT), constant(falseValue, BIGINT)));
    }

    private static RowExpressionOptimizer getNativeOptimizer()
    {
        String nativePrefix = "native.default";
        MetadataManager metadata = MetadataManager.createTestMetadataManager(new FunctionsConfig().setDefaultNamespacePrefix(nativePrefix));

        metadata.getFunctionAndTypeManager().addFunctionNamespace(
                "native",
                new InMemoryFunctionNamespaceManager(
                        "native",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(
                                        CPP, FunctionImplementationType.CPP,
                                        JAVA, FunctionImplementationType.JAVA),
                                new NoopSqlFunctionExecutor()),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("cpp")));
        metadata.getFunctionAndTypeManager().createFunction(CPP_FOO, true);
        metadata.getFunctionAndTypeManager().createFunction(CPP_BAR, true);
        // Create a custom function that only exists in native namespace
        metadata.getFunctionAndTypeManager().createFunction(CPP_CUSTOM_FUNCTION, true);
        return new RowExpressionOptimizer(metadata);
    }

    private RowExpression callOperator(OperatorType operator, RowExpression left, RowExpression right)
    {
        FunctionHandle functionHandle = functionAndTypeManager.resolveOperator(operator, fromTypes(left.getType(), right.getType()));
        return Expressions.call(operator.getOperator(), functionHandle, left.getType(), left, right);
    }

    private RowExpression optimize(RowExpression expression)
    {
        return optimizer.optimize(expression, OPTIMIZED, SESSION);
    }
}
