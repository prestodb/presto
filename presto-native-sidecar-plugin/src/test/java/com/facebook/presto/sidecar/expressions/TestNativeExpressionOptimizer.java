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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.BlockSerdeUtil;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.expressions.AbstractTestExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.findRandomPortForSidecar;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.getNativeSidecarProcess;
import static com.facebook.presto.sidecar.expressions.NativeExpressionUtils.getExpressionOptimizer;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.EVALUATED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestNativeExpressionOptimizer
        extends AbstractTestExpressionInterpreter
{
    private ExpressionOptimizer expressionOptimizer;
    private Process sidecar;
    private static final BlockEncodingSerde BLOCK_ENCODING_SERDE = new BlockEncodingManager();

    public TestNativeExpressionOptimizer()
    {
        METADATA.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of(APPLY_FUNCTION));
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        int port = findRandomPortForSidecar();
        URI sidecarUri = URI.create(format("http://127.0.0.1:%s", port));
        sidecar = getNativeSidecarProcess(sidecarUri, port);
        expressionOptimizer = getExpressionOptimizer(METADATA, new HandleResolver(), sidecarUri);
    }

    @AfterClass
    public void tearDown()
    {
        sidecar.destroyForcibly();
    }

    /// TODO: Velox permits Bigint to Varchar cast but Presto does not.
    @Override
    @Test
    public void testCastBigintToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(12300000000 AS varchar(11))", "'12300000000'");
        assertEvaluatedEquals("CAST(12300000000 AS varchar(50))", "'12300000000'");

        evaluate("CAST(12300000000 AS varchar(3))");
        evaluate("CAST(-12300000000 AS varchar(3))");
    }

    @Override
    @Test
    public void testLambda()
    {
        assertDoNotOptimize("transform(unbound_array, x -> x + x)", OPTIMIZED);
        assertOptimizedEquals("transform(ARRAY[1, 5], x -> x + x)", "transform(ARRAY[1, 5], x -> x + x)");
        assertOptimizedEquals("transform(sequence(1, 5), x -> x + x)", "transform(sequence(1, 5), x -> x + x)");
    }

    /// TODO: LIKE function with variables will not be constant folded in Velox.
    /// SERIALIZABLE optimizer level is not supported by native expression optimizer.
    @Override
    @Test
    public void testLike()
    {
        assertOptimizedEquals("'a' LIKE 'a'", "true");
        assertOptimizedEquals("'' LIKE 'a'", "false");
        assertOptimizedEquals("'abc' LIKE 'a'", "false");

        assertOptimizedEquals("'a' LIKE '_'", "true");
        assertOptimizedEquals("'' LIKE '_'", "false");
        assertOptimizedEquals("'abc' LIKE '_'", "false");

        assertOptimizedEquals("'a' LIKE '%'", "true");
        assertOptimizedEquals("'' LIKE '%'", "true");
        assertOptimizedEquals("'abc' LIKE '%'", "true");

        assertOptimizedEquals("'abc' LIKE '___'", "true");
        assertOptimizedEquals("'ab' LIKE '___'", "false");
        assertOptimizedEquals("'abcd' LIKE '___'", "false");

        assertOptimizedEquals("'abc' LIKE 'abc'", "true");
        assertOptimizedEquals("'xyz' LIKE 'abc'", "false");
        assertOptimizedEquals("'abc0' LIKE 'abc'", "false");
        assertOptimizedEquals("'0abc' LIKE 'abc'", "false");

        assertOptimizedEquals("'abc' LIKE 'abc%'", "true");
        assertOptimizedEquals("'abc0' LIKE 'abc%'", "true");
        assertOptimizedEquals("'0abc' LIKE 'abc%'", "false");

        assertOptimizedEquals("'abc' LIKE '%abc'", "true");
        assertOptimizedEquals("'0abc' LIKE '%abc'", "true");
        assertOptimizedEquals("'abc0' LIKE '%abc'", "false");

        assertOptimizedEquals("'abc' LIKE '%abc%'", "true");
        assertOptimizedEquals("'0abc' LIKE '%abc%'", "true");
        assertOptimizedEquals("'abc0' LIKE '%abc%'", "true");
        assertOptimizedEquals("'0abc0' LIKE '%abc%'", "true");
        assertOptimizedEquals("'xyzw' LIKE '%abc%'", "false");

        assertOptimizedEquals("'abc' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0abc' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'abc0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0abc0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'ab01c' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0ab01c' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'ab01c0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0ab01c0' LIKE '%ab%c%'", "true");

        assertOptimizedEquals("'xyzw' LIKE '%ab%c%'", "false");

        // ensure regex chars are escaped
        assertOptimizedEquals("'' LIKE ''", "true");
        assertOptimizedEquals("'.*' LIKE '.*'", "true");
        assertOptimizedEquals("'[' LIKE '['", "true");
        assertOptimizedEquals("']' LIKE ']'", "true");
        assertOptimizedEquals("'{' LIKE '{'", "true");
        assertOptimizedEquals("'}' LIKE '}'", "true");
        assertOptimizedEquals("'?' LIKE '?'", "true");
        assertOptimizedEquals("'+' LIKE '+'", "true");
        assertOptimizedEquals("'(' LIKE '('", "true");
        assertOptimizedEquals("')' LIKE ')'", "true");
        assertOptimizedEquals("'|' LIKE '|'", "true");
        assertOptimizedEquals("'^' LIKE '^'", "true");
        assertOptimizedEquals("'$' LIKE '$'", "true");

        assertOptimizedEquals("null LIKE '%'", "null");
        assertOptimizedEquals("'a' LIKE null", "null");
        assertOptimizedEquals("'a' LIKE '%' ESCAPE null", "null");
//        assertOptimizedEquals("'a' LIKE unbound_string ESCAPE null", "null");

        assertOptimizedEquals("'%' LIKE 'z%' ESCAPE 'z'", "true");

//        assertRowExpressionEquals(SERIALIZABLE, "'%' LIKE 'z%' ESCAPE 'z'", "true");
//        assertRowExpressionEquals(SERIALIZABLE, "'%' LIKE 'z%'", "false");
    }

    @Override
    @Test
    public void testLiterals()
    {
        optimize("date '2013-04-03' + unbound_interval");
        optimize("time '03:04:05.321' + unbound_interval");
        optimize("time '03:04:05.321 UTC' + unbound_interval");
        optimize("timestamp '2013-04-03 03:04:05.321' + unbound_interval");
        optimize("timestamp '2013-04-03 03:04:05.321 UTC' + unbound_interval");

        optimize("interval '3' day * unbound_long");
        optimize("interval '3' year * unbound_long");
    }

    /// Presto C++ only supports legacy map subscript and returns NULL for missing keys instead of PrestoException.
    @Override
    @Test
    public void testMapSubscriptMissingKey() {}

    /// Optimizer level SERIALIZABLE is not supported in Presto C++.
    @Test(enabled = false)
    public void testMassiveArray() {}

    // TODO: current timestamp returns the session timestamp, which is not supported by this test
    @Override
    @Test(enabled = false)
    public void testCurrentTimestamp() {}

    /// TODO: current_user should be evaluated in the sidecar plugin and not in the sidecar.
    @Override
    @Test(enabled = false)
    public void testCurrentUser() {}

    /// This test is disabled because these optimizations for LIKE function are not yet supported in Velox.
    /// TODO: LIKE function with variables will not be constant folded in Velox.
    @Override
    @Test(enabled = false)
    public void testLikeOptimization() {}

    /// TODO: These tests are disabled as they throw a Velox exception when constant folded, protocol changes along
    /// with Velox to Presto exception conversion is needed to support this.
    /// Another limitation is that LIKE function with unbound_string will not be constant folded in Velox.
    @Override
    @Test(enabled = false)
    public void testInvalidLike() {}

    /// TODO: NULL_IF special form is unsupported in Presto native.
    @Override
    @Test(enabled = false)
    public void testNullIf() {}

    /// TODO: This test is disabled as the expressions throw a Velox exception when constant folded, protocol changes
    /// are needed to convert the error code and construct the FailFunction CallExpression.
    @Override
    @Test(enabled = false)
    public void testFailedExpressionOptimization() {}

    /// TODO: Json based UDFs are not supported by the native expression optimizer.
    @Override
    @Test(enabled = false)
    public void testCppFunctionCall() {}

    /// TODO: Json based UDFs are not supported by the native expression optimizer.
    @Override
    @Test(enabled = false)
    public void testCppAggregateFunctionCall() {}

    /// Non-deterministic functions are evaluated by default in sidecar.
    @Override
    @Test(enabled = false)
    public void testNonDeterministicFunctionCall() {}

    @Override
    public Object evaluate(@Language("SQL") String expression, boolean deterministic)
    {
        return evaluate(expression);
    }

    private RowExpression evaluate(@Language("SQL") String expression)
    {
        assertRoundTrip(expression);
        RowExpression rowExpression = sqlToRowExpression(expression);
        return optimizeRowExpression(rowExpression, EVALUATED);
    }

    @Override
    public RowExpression optimize(@Language("SQL") String expression)
    {
        assertRoundTrip(expression);
        RowExpression parsedExpression = sqlToRowExpression(expression);
        return optimizeRowExpression(parsedExpression, OPTIMIZED);
    }

    @Override
    public void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        RowExpression optimizedActual = optimize(actual);
        RowExpression optimizedExpected = optimize(expected);
        assertRowExpressionEvaluationEquals(optimizedActual, optimizedExpected);
    }

    @Override
    public void assertOptimizedMatches(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertOptimizedEquals(actual, expected);
    }

    @Override
    public void assertDoNotOptimize(@Language("SQL") String expression, ExpressionOptimizer.Level optimizationLevel)
    {
        assertRoundTrip(expression);
        RowExpression rowExpression = sqlToRowExpression(expression);
        RowExpression rowExpressionResult = optimizeRowExpression(rowExpression, optimizationLevel);
        assertRowExpressionEvaluationEquals(rowExpressionResult, rowExpression);
    }

    private RowExpression sqlToRowExpression(String expression)
    {
        Expression parsedExpression = FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);
        return TRANSLATOR.translate(parsedExpression, SYMBOL_TYPES);
    }

    /**
     * Assert the evaluation result of two row expressions equivalent
     * no matter they are constants or remaining row expressions.
     */
    private void assertRowExpressionEvaluationEquals(RowExpression left, RowExpression right)
    {
        if (left instanceof ConstantExpression) {
            if (isRemovableCast(right)) {
                assertRowExpressionEvaluationEquals(left, ((CallExpression) right).getArguments().get(0));
                return;
            }
            assertTrue(right instanceof ConstantExpression);
            assertConstantsEqual(((ConstantExpression) left), ((ConstantExpression) left));
        }
        else if (left instanceof InputReferenceExpression || left instanceof VariableReferenceExpression) {
            assertEquals(left, right);
        }
        else if (left instanceof CallExpression && ((CallExpression) left).getFunctionHandle().getName().contains("fail")) {
            assertTrue(right instanceof CallExpression && ((CallExpression) right).getFunctionHandle().getName().contains("fail"));
            assertEquals(((CallExpression) left).getArguments().size(), ((CallExpression) right).getArguments().size());
            for (int i = 0; i < ((CallExpression) left).getArguments().size(); i++) {
                assertRowExpressionEvaluationEquals(((CallExpression) left).getArguments().get(i), ((CallExpression) right).getArguments().get(i));
            }
        }
        else if (left instanceof CallExpression) {
            assertTrue(right instanceof CallExpression);
            assertEquals(((CallExpression) left).getFunctionHandle(), ((CallExpression) right).getFunctionHandle());
            assertEquals(((CallExpression) left).getArguments().size(), ((CallExpression) right).getArguments().size());
            for (int i = 0; i < ((CallExpression) left).getArguments().size(); i++) {
                assertRowExpressionEvaluationEquals(((CallExpression) left).getArguments().get(i), ((CallExpression) right).getArguments().get(i));
            }
        }
        else if (left instanceof SpecialFormExpression) {
            assertTrue(right instanceof SpecialFormExpression);
            assertEquals(((SpecialFormExpression) left).getForm(), ((SpecialFormExpression) right).getForm());
            assertEquals(((SpecialFormExpression) left).getArguments().size(), ((SpecialFormExpression) right).getArguments().size());
            for (int i = 0; i < ((SpecialFormExpression) left).getArguments().size(); i++) {
                assertRowExpressionEvaluationEquals(((SpecialFormExpression) left).getArguments().get(i), ((SpecialFormExpression) right).getArguments().get(i));
            }
        }
        else {
            assertTrue(left instanceof LambdaDefinitionExpression);
            assertTrue(right instanceof LambdaDefinitionExpression);
            assertEquals(((LambdaDefinitionExpression) left).getArguments(), ((LambdaDefinitionExpression) right).getArguments());
            assertEquals(((LambdaDefinitionExpression) left).getArgumentTypes(), ((LambdaDefinitionExpression) right).getArgumentTypes());
            assertRowExpressionEvaluationEquals(((LambdaDefinitionExpression) left).getBody(), ((LambdaDefinitionExpression) right).getBody());
        }
    }

    private void assertConstantsEqual(ConstantExpression left, ConstantExpression right)
    {
        if (left.getValue() instanceof Block) {
            assertTrue(right.getValue() instanceof Block);
            assertBlockEquals((Block) left.getValue(), (Block) right.getValue());
        }
        else {
            assertEquals(left.getValue(), right.getValue());
        }
    }

    private static void assertBlockEquals(Block left, Block right)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput(1000);
        BlockSerdeUtil.writeBlock(BLOCK_ENCODING_SERDE, sliceOutput, right);
        SliceOutput sliceOutput1 = new DynamicSliceOutput(1000);
        BlockSerdeUtil.writeBlock(BLOCK_ENCODING_SERDE, sliceOutput1, left);
        assertEquals(sliceOutput1.slice(), sliceOutput.slice());
    }

    private boolean isRemovableCast(RowExpression value)
    {
        if (value instanceof CallExpression &&
                new FunctionResolution(METADATA.getFunctionAndTypeManager().getFunctionAndTypeResolver()).isCastFunction(((CallExpression) value).getFunctionHandle())) {
            Type targetType = value.getType();
            Type sourceType = ((CallExpression) value).getArguments().get(0).getType();
            return METADATA.getFunctionAndTypeManager().canCoerce(sourceType, targetType);
        }
        return false;
    }

    @Override
    public void assertEvaluatedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertRowExpressionEvaluationEquals(evaluate(actual), evaluate(expected));
    }

    @Override
    public Object optimize(RowExpression expression, ExpressionOptimizer.Level level)
    {
        return optimizeRowExpression(expression, level);
    }

    private RowExpression optimizeRowExpression(RowExpression expression, ExpressionOptimizer.Level level)
    {
        return expressionOptimizer.optimize(
                expression,
                level,
                TEST_SESSION.toConnectorSession(),
                variable -> {
                    Symbol symbol = new Symbol(variable.getName());
                    Object value = symbolConstant(symbol);
                    if (value == null) {
                        return new VariableReferenceExpression(Optional.empty(), symbol.getName(), SYMBOL_TYPES.get(symbol.toSymbolReference()));
                    }
                    return value;
                });
    }
}
