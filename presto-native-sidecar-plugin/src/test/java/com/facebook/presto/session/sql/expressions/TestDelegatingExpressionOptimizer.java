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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.relational.DelegatingRowExpressionOptimizer;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.tests.expressions.TestExpressions;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.findRandomPortForWorker;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeSidecarProcess;
import static com.facebook.presto.session.sql.expressions.TestNativeExpressionOptimization.getExpressionOptimizer;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.EVALUATED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.type.LikeFunctions.castVarcharToLikePattern;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.testng.Assert.assertEquals;

public class TestDelegatingExpressionOptimizer
        extends TestExpressions
{
    public static final FunctionResolution RESOLUTION = new FunctionResolution(METADATA.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    private ExpressionOptimizer expressionOptimizer;
    private Process sidecar;

    @BeforeClass
    public void setup()
            throws Exception
    {
        super.setup();
        int port = findRandomPortForWorker();
        URI sidecarUri = URI.create("http://127.0.0.1:" + port);
        sidecar = getNativeSidecarProcess(URI.create("http://test.invalid/"), port);

        ExpressionOptimizer optimizer = getExpressionOptimizer(METADATA, HANDLE_RESOLVER, sidecarUri);
        expressionOptimizer = new DelegatingRowExpressionOptimizer(METADATA, () -> optimizer);
    }

    @AfterClass
    public void tearDown()
    {
        sidecar.destroyForcibly();
    }

    // TODO: Pending on native function namespace manager.
    @Test(enabled = false)
    public void assertLikeOptimizations()
    {
        assertOptimizedMatches("unbound_string LIKE bound_pattern", "unbound_string LIKE CAST('%el%' AS varchar)");
    }

    // TODO: this test is invalid as it manually constructs an expression which can't be serialized
    @Test(enabled = false)
    @Override
    public void testLikeInvalidUtf8()
    {
    }

    // TODO: Pending on native function namespace manager.
    @Test(enabled = false)
    @Override
    public void testLike()
    {
    }

    // TODO: Pending on native function namespace manager.
    @Test(enabled = false)
    @Override
    public void testLikeOptimization()
    {
    }

    // TODO: Pending on native function namespace manager.
    @Test(enabled = false)
    @Override
    public void testInvalidLike()
    {
    }

    @Test(enabled = false)
    @Override
    public void testLambda()
    {
        assertDoNotOptimize("transform(unbound_array, x -> x + x)", OPTIMIZED);
        assertOptimizedEquals("transform(ARRAY[1, 5], x -> x + x)", "transform(ARRAY[1, 5], x -> x + x)");
        assertOptimizedEquals("transform(sequence(1, 5), x -> x + x)", "transform(sequence(1, 5), x -> x + x)");
        assertRowExpressionEquals(
                OPTIMIZED,
                "transform(sequence(1, unbound_long), x -> cast(json_parse('[1, 2]') AS ARRAY<INTEGER>)[1] + x)",
                "transform(sequence(1, unbound_long), x -> 1 + x)");
        assertRowExpressionEquals(
                OPTIMIZED,
                "transform(sequence(1, unbound_long), x -> cast(json_parse('[1, 2]') AS ARRAY<INTEGER>)[1] + 1)",
                "transform(sequence(1, unbound_long), x -> 2)");
        // TODO: lambdas are currently unsupported by this test
//        assertEquals(evaluate("reduce(ARRAY[1, 5], 0, (x, y) -> x + y, x -> x)", true), 6L);
    }

    // TODO: current timestamp returns the session timestamp, which is not supported by this test
    @Test(enabled = false)
    @Override
    public void testCurrentTimestamp()
    {
    }

    // TODO: this function is not supported by this test because its contents are not serializable
    @Test(enabled = false)
    @Override
    public void testMassiveArray()
    {
    }

    // TODO: apply function is not supported in Presto native.
    @Test(enabled = false)
    @Override
    public void testBind()
    {
    }

    @Test
    @Override
    public void testLiterals()
    {
        optimize("date '2013-04-03' + unbound_interval");
        // TODO: TIME type is unsupported in Presto native.
//        optimize("time '03:04:05.321' + unbound_interval");
//        optimize("time '03:04:05.321 UTC' + unbound_interval");
        optimize("timestamp '2013-04-03 03:04:05.321' + unbound_interval");
        optimize("timestamp '2013-04-03 03:04:05.321 UTC' + unbound_interval");

        optimize("interval '3' day * unbound_long");
        // TODO: Pending on velox PR: https://github.com/facebookincubator/velox/pull/11612.
//        optimize("interval '3' year * unbound_integer");
    }

    // TODO: NULL_IF special form is unsupported in Presto native.
    @Test(enabled = false)
    @Override
    public void testNullIf()
    {
    }

    @Test
    @Override
    public void testCastBigintToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(12300000000 AS varchar(11))", "'12300000000'");
        assertEvaluatedEquals("CAST(12300000000 AS varchar(50))", "'12300000000'");

        // TODO: Velox permits this cast, but Presto does not
//        try {
//            evaluate("CAST(12300000000 AS varchar(3))", true);
//            fail("Expected to throw an INVALID_CAST_ARGUMENT exception");
//        }
//        catch (PrestoException e) {
//            try {
//                assertEquals(e.getErrorCode(), INVALID_CAST_ARGUMENT.toErrorCode());
//                assertEquals(e.getMessage(), "Value 12300000000 cannot be represented as varchar(3)");
//            }
//            catch (Throwable failure) {
//                failure.addSuppressed(e);
//                throw failure;
//            }
//        }
//
//        try {
//            evaluate("CAST(-12300000000 AS varchar(3))", true);
//        }
//        catch (PrestoException e) {
//            try {
//                assertEquals(e.getErrorCode(), INVALID_CAST_ARGUMENT.toErrorCode());
//                assertEquals(e.getMessage(), "Value -12300000000 cannot be represented as varchar(3)");
//            }
//            catch (Throwable failure) {
//                failure.addSuppressed(e);
//                throw failure;
//            }
//        }
    }

    // TODO: Non-legacy map subscript is not supported in Presto native.
    @Test(enabled = false)
    @Override
    public void testMapSubscriptMissingKey()
    {
    }

    // TODO: This test is not applicable in the sidecar so disabled for now.
    //  To be enabled after changes in sidecar query runner to support Json
    //  based UDF registration.
    @Test(enabled = false)
    @Override
    public void testCppAggregateFunctionCall()
    {
    }

    // TODO: This test is not applicable in the sidecar so disabled for now.
    //  To be enabled after changes in sidecar query runner to support Json
    //  based UDF registration.
    @Test(enabled = false)
    @Override
    public void testCppFunctionCall()
    {
    }

    @Override
    protected void assertLike(byte[] value, String pattern, boolean expected)
    {
        CallExpression predicate = call(
                "LIKE",
                RESOLUTION.likeVarcharFunction(),
                BOOLEAN,
                ImmutableList.of(
                        new ConstantExpression(wrappedBuffer(value), VARCHAR),
                        new ConstantExpression(castVarcharToLikePattern(utf8Slice(pattern)), LIKE_PATTERN)));
        assertEquals(optimizeRowExpression(predicate, EVALUATED), expected);
    }
    @Override
    protected Object evaluate(String expression, boolean deterministic)
    {
        assertRoundTrip(expression);
        RowExpression rowExpression = sqlToRowExpression(expression);
        return optimizeRowExpression(rowExpression, EVALUATED);
    }

    @Override
    protected Object optimize(String expression)
    {
        assertRoundTrip(expression);
        RowExpression parsedExpression = sqlToRowExpression(expression);
        return optimizeRowExpression(parsedExpression, OPTIMIZED);
    }

    @Override
    protected Object optimizeRowExpression(RowExpression expression, Level level)
    {
        Object optimized = expressionOptimizer.optimize(
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
        return unwrap(optimized);
    }

    public Object unwrap(Object result)
    {
        if (result instanceof ConstantExpression) {
            return ((ConstantExpression) result).getValue();
        }
        else {
            return result;
        }
    }

    @Override
    protected void assertOptimizedEquals(String actual, String expected)
    {
        Object optimizedActual = optimize(actual);
        Object optimizedExpected = optimize(expected);
        assertRowExpressionEvaluationEquals(optimizedActual, optimizedExpected);
    }

    @Override
    protected void assertOptimizedMatches(String actual, String expected)
    {
        Object actualOptimized = optimize(actual);
        Object expectedOptimized = optimize(expected);
        assertRowExpressionEvaluationEquals(
                actualOptimized,
                expectedOptimized);
    }

    @Override
    protected void assertDoNotOptimize(String expression, Level optimizationLevel)
    {
        assertRoundTrip(expression);
        RowExpression rowExpression = sqlToRowExpression(expression);
        Object rowExpressionResult = optimizeRowExpression(rowExpression, optimizationLevel);
        assertRowExpressionEvaluationEquals(rowExpressionResult, rowExpression);
    }
}
