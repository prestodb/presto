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
package com.facebook.presto.sidecar.expressions;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sidecar.ForSidecarInfo;
import com.facebook.presto.sidecar.NativeSidecarPluginQueryRunner;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.expressions.AbstractTestExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestNativeExpressionInterpreter
        extends AbstractTestExpressionInterpreter
{
    private final TestVisitor visitor;
    private final MetadataManager metadata;
    private final TestingRowExpressionTranslator translator;
    private final DistributedQueryRunner queryRunner;
    private final NativeSidecarExpressionInterpreter rowExpressionInterpreter;

    public TestNativeExpressionInterpreter()
            throws Exception
    {
        this.queryRunner = NativeSidecarPluginQueryRunner.getQueryRunner();
        FunctionAndTypeManager functionAndTypeManager = queryRunner.getCoordinator().getFunctionAndTypeManager();
        this.metadata = createTestMetadataManager(functionAndTypeManager);
        this.translator = new TestingRowExpressionTranslator(metadata);
        this.rowExpressionInterpreter = getRowExpressionInterpreter(functionAndTypeManager, queryRunner.getCoordinator().getPluginNodeManager());
        this.visitor = new TestVisitor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
    }

    ///  Velox permits Bigint to Varchar cast but Presto does not.
    @Override
    @Test
    public void testCastBigintToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(12300000000 AS varchar)", "'12300000000'");
        assertEvaluatedEquals("CAST(12300000000 AS varchar)", "'12300000000'");

        evaluate("CAST(12300000000 AS varchar(3))", true);
        evaluate("CAST(-12300000000 AS varchar(3))", true);
    }

    /// Sidecar returns a ConstantExpression for random() when evaluated.
    @Override
    @Test
    public void testNonDeterministicFunctionCall()
    {
        // optimize should do nothing
        assertOptimizedEquals("random()", "random()");

        // evaluate should execute
        Object value = evaluate("random()", false);
        assertTrue(value instanceof ConstantExpression);
        double randomValue = (double) ((ConstantExpression) value).getValue();
        assertTrue(0 <= randomValue && randomValue < 1);
    }

    /// Velox adds an implicit cast to the expression type for failed expression optimizations, so expected expression
    /// containing one or more `fail` subexpressions should not be optimized in Velox. The string representation of
    /// RowExpression is used to validate `fail` expression replaces all failing subexpressions.
    @Override
    @Test
    public void testFailedExpressionOptimization()
    {
        // TODO: Velox COALESCE rewrite should be enhanced to deduplicate fail expressions.
        assertFailedMatches("coalesce(0 / 0 > 1, unbound_boolean, 0 / 0 = 0)",
                "COALESCE\\(presto.default.\\$operator\\$greater_than\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\), 1\\), unbound_boolean, presto.default.\\$operator\\$equal\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\), 0\\)\\)");

        assertFailedMatches("if(false, 1, 0 / 0)", "presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\)");

        assertFailedMatches("CASE unbound_long WHEN 1 THEN 1 WHEN 0 / 0 THEN 2 END",
                "SWITCH\\(WHEN\\(presto.default.\\$operator\\$equal\\(1, unbound_long\\), 1\\), WHEN\\(presto.default.\\$operator\\$equal\\(presto.default.\\$operator\\$cast\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\)\\), unbound_long\\), 2\\), null\\)");

        assertFailedMatches("CASE unbound_boolean WHEN true THEN 1 ELSE 0 / 0 END",
                "SWITCH\\(WHEN\\(presto.default.\\$operator\\$equal\\(true, unbound_boolean\\), 1\\), presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\)\\)");

        assertFailedMatches("CASE bound_long WHEN unbound_long THEN 1 WHEN 0 / 0 THEN 2 ELSE 1 END",
                "SWITCH\\(WHEN\\(presto.default.\\$operator\\$equal\\(unbound_long, 1234\\), 1\\), WHEN\\(presto.default.\\$operator\\$equal\\(presto.default.\\$operator\\$cast\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\)\\), 1234\\), 2\\), 1\\)");

        assertFailedMatches("case when unbound_boolean then 1 when 0 / 0 = 0 then 2 end",
                "SWITCH\\(WHEN\\(unbound_boolean, 1\\), WHEN\\(presto.default.\\$operator\\$equal\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\), 0\\), 2\\), null\\)");

        assertFailedMatches("case when unbound_boolean then 1 else 0 / 0  end",
                "SWITCH\\(WHEN\\(unbound_boolean, 1\\), presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\)\\)");

        assertFailedMatches("case when unbound_boolean then 0 / 0 else 1 end",
                "SWITCH\\(WHEN\\(unbound_boolean, presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\)\\), 1\\)");

        assertFailedMatches("case true " +
                        "when unbound_long = 1 then 1 " +
                        "when 0 / 0 = 0 then 2 " +
                        "else 33 end",
                "SWITCH\\(WHEN\\(presto.default.\\$operator\\$equal\\(unbound_long, 1\\), 1\\), WHEN\\(presto.default.\\$operator\\$equal\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\), 0\\), 2\\), 33\\)");

        assertFailedMatches("case 1 " +
                        "when 0 / 0 then 1 " +
                        "when 0 / 0 then 2 " +
                        "else 1 " +
                        "end",
                "SWITCH\\(WHEN\\(presto.default.\\$operator\\$equal\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\), 1\\), 1\\), WHEN\\(presto.default.\\$operator\\$equal\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\), 1\\), 2\\), 1\\)");

        assertFailedMatches("case 1 " +
                        "when unbound_long then 1 " +
                        "when 0 / 0 then 2 " +
                        "else 1 " +
                        "end",
                "SWITCH\\(WHEN\\(presto.default.\\$operator\\$equal\\(unbound_long, 1\\), 1\\), WHEN\\(presto.default.\\$operator\\$equal\\(presto.default.\\$operator\\$cast\\(presto.default.\\$operator\\$cast\\(native.default.fail\\(.*\\)\\)\\), 1\\), 2\\), 1\\)");
    }

    /// Sidecar will return an ExecutionFailure when an expression throws during evaluation. The caller of expression
    /// optimization endpoint on the sidecar should throw a PrestoException.
    @Override
    @Test
    public void testOptimizeDivideByZero()
    {
        assertEvaluateFails("1 / 0", "division by zero");
    }

    /// Sidecar will return an ExecutionFailure when an expression throws during evaluation. The caller of expression
    /// optimization endpoint on the sidecar should throw a PrestoException.
    @Override
    @Test
    public void testArraySubscriptConstantNegativeIndex()
    {
        assertEvaluateFails("ARRAY [1, 2, 3][-1]", "Array subscript index cannot be negative");
    }

    @Override
    @Test
    public void testArraySubscriptConstantZeroIndex()
    {
        assertEvaluateFails("ARRAY [1, 2, 3][0]", "SQL array indices start at 1. Got 0.");
    }

    /// TODO: Optimization of `'a' LIKE unbound_string ESCAPE null` to `null` is not supported in Velox.
    @Override
    @Test
    public void testLikeNullOptimization()
    {
        assertOptimizedEquals("null LIKE '%'", "null");
        assertOptimizedEquals("'a' LIKE null", "null");
        assertOptimizedEquals("'a' LIKE '%' ESCAPE null", "null");
        // assertOptimizedEquals("'a' LIKE unbound_string ESCAPE null", "null");
    }

    /// TODO: Certain tests are pending on IN-rewrite in Velox: https://github.com/facebookincubator/velox/pull/15663.
    @Override
    @Test
    public void testIn()
    {
        assertOptimizedEquals("3 in (2, 4, 3, 5)", "true");
        assertOptimizedEquals("3 in (2, 4, 9, 5)", "false");
        assertOptimizedEquals("3 in (2, null, 3, 5)", "true");

        assertOptimizedEquals("'foo' in ('bar', 'baz', 'foo', 'blah')", "true");
        assertOptimizedEquals("'foo' in ('bar', 'baz', 'buz', 'blah')", "false");
        assertOptimizedEquals("'foo' in ('bar', null, 'foo', 'blah')", "true");

        assertOptimizedEquals("null in (2, null, 3, 5)", "null");
        assertOptimizedEquals("3 in (2, null)", "null");

        assertOptimizedEquals("bound_integer in (2, 1234, 3, 5)", "true");
        assertOptimizedEquals("bound_integer in (2, 4, 3, 5)", "false");
        assertOptimizedEquals("1234 in (2, bound_integer, 3, 5)", "true");
        assertOptimizedEquals("99 in (2, bound_integer, 3, 5)", "false");
        assertOptimizedEquals("bound_integer in (2, bound_integer, 3, 5)", "true");

        assertOptimizedEquals("bound_long in (2, 1234, 3, 5)", "true");
        assertOptimizedEquals("bound_long in (2, 4, 3, 5)", "false");
        assertOptimizedEquals("1234 in (2, bound_long, 3, 5)", "true");
        assertOptimizedEquals("99 in (2, bound_long, 3, 5)", "false");
        assertOptimizedEquals("bound_long in (2, bound_long, 3, 5)", "true");

        assertOptimizedEquals("bound_string in ('bar', 'hello', 'foo', 'blah')", "true");
        assertOptimizedEquals("bound_string in ('bar', 'baz', 'foo', 'blah')", "false");
        assertOptimizedEquals("'hello' in ('bar', bound_string, 'foo', 'blah')", "true");
        assertOptimizedEquals("'baz' in ('bar', bound_string, 'foo', 'blah')", "false");

        // TODO: Pending on IN rewrite in Velox: https://github.com/facebookincubator/velox/pull/15663.
//        assertOptimizedEquals("bound_long in (2, 1234, unbound_long, 5)", "true");
//        assertOptimizedEquals("bound_string in ('bar', 'hello', unbound_string, 'blah')", "true");
//        assertOptimizedEquals("bound_long in (2, 4, unbound_long, unbound_long2, 9)", "1234 in (unbound_long, unbound_long2)");
//        assertOptimizedEquals("unbound_long in (2, 4, bound_long, unbound_long2, 5)", "unbound_long in (2, 4, 1234, unbound_long2, 5)");

        assertOptimizedEquals("1.15 in (1.1, 1.2, 1.3, 1.15)", "true");
        assertOptimizedEquals("9876543210.98745612035 in (9876543210.9874561203, 9876543210.9874561204, 9876543210.98745612035)", "true");
        assertOptimizedEquals("bound_decimal_short in (123.455, 123.46, 123.45)", "true");
        assertOptimizedEquals("bound_decimal_long in (12345678901234567890.123, 9876543210.9874561204, 9876543210.98745612035)", "true");
        assertOptimizedEquals("bound_decimal_long in (9876543210.9874561204, null, 9876543210.98745612035)", "null");
    }

    /// Velox only supports legacy map subscript and returns NULL for missing keys instead of an exception.
    @Override
    @Test(enabled = false)
    public void testMapSubscriptMissingKey() {}

    /// TODO: Fix result mismatch between Presto and Velox for VARBINARY literals.
    /// java.lang.AssertionError:
    /// Expected :Slice{base=[B@771ede0d, address=16, length=2}
    /// Actual   :Slice{base=[B@1fd73dcb, address=47, length=2}
    @Override
    @Test(enabled = false)
    public void testVarbinaryLiteral() {}

    // TODO: current timestamp returns the session timestamp and this should be evaluated on the sidecar plugin.
    @Override
    @Test(enabled = false)
    public void testCurrentTimestamp() {}

    /// TODO: current_user should be evaluated in the sidecar plugin and not in the sidecar.
    @Override
    @Test(enabled = false)
    public void testCurrentUser() {}

    /// This test is disabled because these optimizations for LIKE function are not yet supported in Velox.
    /// TODO: LIKE function with field references should be optimized with a rewrite in Velox.
    @Override
    @Test(enabled = false)
    public void testLikeOptimization() {}

    /// LIKE function with unbound_string will not be optimized/constant-folded in Velox.
    /// TODO: LIKE function with field references should be optimized with a rewrite in Velox.
    @Override
    @Test(enabled = false)
    public void testInvalidLike() {}

    /// TODO: NULL_IF special form is unsupported in Presto C++.
    @Override
    @Test(enabled = false)
    public void testNullIf() {}

    /// TODO: Json based UDFs are not supported by the native expression optimizer.
    @Override
    @Test(enabled = false)
    public void testCppFunctionCall() {}

    /// TODO: Json based UDFs are not supported by the native expression optimizer.
    @Override
    @Test(enabled = false)
    public void testCppAggregateFunctionCall() {}

    /// TODO: Dereference expressions are not supported by Velox expression optimizer.
    @Override
    @Test(enabled = false)
    public void testRowSubscript() {}

    @Override
    public Object evaluate(@Language("SQL") String expression, boolean deterministic)
    {
        return evaluate(expression);
    }

    private RowExpression evaluate(@Language("SQL") String expression)
    {
        return optimize(expression, ExpressionOptimizer.Level.EVALUATED);
    }

    @Override
    public RowExpression optimize(String expression)
    {
        return optimize(expression, ExpressionOptimizer.Level.OPTIMIZED);
    }

    private RowExpression optimize(@Language("SQL") String expression, ExpressionOptimizer.Level level)
    {
        assertRoundTrip(expression);
        RowExpression parsedExpression = sqlToRowExpression(expression);
        return optimizeRowExpression(parsedExpression, level);
    }

    @Override
    public void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        RowExpression optimizedActual = optimize(actual, ExpressionOptimizer.Level.OPTIMIZED);
        RowExpression optimizedExpected = optimize(expected, ExpressionOptimizer.Level.OPTIMIZED);
        assertRowExpressionEvaluationEquals(optimizedActual, optimizedExpected);
    }

    @Override
    public void assertOptimizedMatches(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertOptimizedEquals(actual, expected);
    }

    private void assertEvaluateFails(@Language("SQL") String expression, @Language("SQL") String errorMessage)
    {
        RowExpression rowExpression = sqlToRowExpression(expression);
        rowExpression = rowExpression.accept(visitor, null);

        RowExpressionOptimizationResult response = optimize(rowExpression, ExpressionOptimizer.Level.EVALUATED);
        assertNotNull(response.getExpressionFailureInfo().getMessage());
        assertTrue(response.getExpressionFailureInfo().getMessage().contains(errorMessage), format("Sidecar response: %s did not contain expected error message: %s.", response, errorMessage));
    }

    /// Checks that the string representation of the failed optimized expression matches expected.
    @Override
    public void assertFailedMatches(@Language("SQL") String actual, @Language("RegExp") String expected)
    {
        RowExpression optimized = optimize(actual);
        assertTrue(optimized.toString().matches(expected));
    }

    @Override
    public void assertDoNotOptimize(@Language("SQL") String expression, ExpressionOptimizer.Level optimizationLevel)
    {
        assertRoundTrip(expression);
        RowExpression rowExpression = sqlToRowExpression(expression);
        RowExpression rowExpressionResult = optimizeRowExpression(rowExpression, ExpressionOptimizer.Level.OPTIMIZED);
        assertRowExpressionEvaluationEquals(rowExpressionResult, rowExpression);
    }

    private RowExpression sqlToRowExpression(String expression)
    {
        Expression parsedExpression = FunctionAssertions.createExpression(expression, metadata, SYMBOL_TYPES);
        return translator.translate(parsedExpression, SYMBOL_TYPES);
    }

    @Override
    public void assertEvaluatedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertRowExpressionEvaluationEquals(evaluate(actual), evaluate(expected));
    }

    private RowExpression optimizeRowExpression(RowExpression expression, ExpressionOptimizer.Level level)
    {
        expression = expression.accept(visitor, null);
        RowExpressionOptimizationResult response = optimize(expression, level);

        assertNotNull(response.getExpressionFailureInfo().getMessage());
        assertTrue(response.getExpressionFailureInfo().getMessage().isEmpty());
        return response.getOptimizedExpression();
    }

    private RowExpressionOptimizationResult optimize(RowExpression expression, ExpressionOptimizer.Level level)
    {
        List<RowExpressionOptimizationResult> results = rowExpressionInterpreter.optimize(TEST_SESSION.toConnectorSession(), level, List.of(expression));
        // Since we are only sending in a rowExpression at a time, the result is going to be of fixed size 1.
        assertEquals(results.size(), 1);
        return results.get(0);
    }

    private NativeSidecarExpressionInterpreter getRowExpressionInterpreter(FunctionAndTypeManager functionAndTypeManager, NodeManager nodeManager)
    {
        Module module = binder -> {
            binder.bind(NodeManager.class).toInstance(nodeManager);
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule(functionAndTypeManager.getHandleResolver()));
            binder.bind(ConnectorManager.class).toProvider(() -> null).in(Scopes.SINGLETON);
            binder.install(new ThriftCodecModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);

            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonCodecBinder(binder).bindListJsonCodec(RowExpression.class);
            jsonCodecBinder(binder).bindListJsonCodec(RowExpressionOptimizationResult.class);

            httpClientBinder(binder).bindHttpClient("sidecar", ForSidecarInfo.class);

            binder.bind(NativeSidecarExpressionInterpreter.class).in(Scopes.SINGLETON);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        return injector.getInstance(NativeSidecarExpressionInterpreter.class);
    }

    private static class TestVisitor
            implements RowExpressionVisitor<RowExpression, Object>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression node, Object context)
        {
            return node;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression node, Object context)
        {
            return node;
        }

        /**
         * Convert a variable reference to a RowExpression.
         * If the symbol has a constant value, return a ConstantExpression of the appropriate type.
         * Otherwise return a VariableReferenceExpression as before.
         */
        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression node, Object context)
        {
            Symbol symbol = new Symbol(node.getName());
            Object value = symbolConstant(symbol);
            if (value == null) {
                return new VariableReferenceExpression(Optional.empty(), symbol.getName(), SYMBOL_TYPES.get(symbol.toSymbolReference()));
            }
            Type type = SYMBOL_TYPES.get(symbol.toSymbolReference());
            return new ConstantExpression(value, type);
        }

        @Override
        public RowExpression visitCall(CallExpression call, Object context)
        {
            CallExpression callExpression;
            List<RowExpression> newArguments = new ArrayList<>();
            for (RowExpression argument : call.getArguments()) {
                RowExpression newArgument = argument.accept(this, context);
                newArguments.add(newArgument);
            }
            callExpression = new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    newArguments);
            return callExpression;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Object context)
        {
            return lambda;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Object context)
        {
            SpecialFormExpression result;
            List<RowExpression> newArguments = new ArrayList<>();
            for (RowExpression argument : specialForm.getArguments()) {
                RowExpression newArgument = argument.accept(this, context);
                newArguments.add(newArgument);
            }
            result = new SpecialFormExpression(
                    specialForm.getSourceLocation(),
                    specialForm.getForm(),
                    specialForm.getType(),
                    newArguments);
            return result;
        }
    }
}
