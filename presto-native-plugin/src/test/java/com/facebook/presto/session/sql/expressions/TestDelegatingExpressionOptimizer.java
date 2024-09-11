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

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
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
import java.util.OptionalInt;
import java.util.function.BiFunction;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.findRandomPortForWorker;
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
        int port = findRandomPortForWorker();
        URI sidecarUri = URI.create("http://127.0.0.1:" + port);
        Optional<BiFunction<Integer, URI, Process>> launcher = PrestoNativeQueryRunnerUtils.getExternalWorkerLauncher(
                "hive",
                "/Users/tdcmeehan/git/presto/presto-native-execution/_build/release/presto_cpp/main/presto_server",
                OptionalInt.of(port),
                0,
                Optional.empty(),
                false);
        sidecar = launcher.get().apply(0, URI.create("http://test.invalid/"));

        expressionOptimizer = new DelegatingRowExpressionOptimizer(METADATA, () -> getExpressionOptimizer(METADATA, HANDLE_RESOLVER, sidecarUri));
    }

    @AfterClass
    public void tearDown()
    {
        sidecar.destroyForcibly();
    }

    @Test
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

    // TODO: lambdas are currently unsupported by this test
    @Test(enabled = false)
    @Override
    public void testLambda()
    {
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

    // TODO: non-deterministic function calls are not supported by this test and need to be tested separately
    @Test(enabled = false)
    @Override
    public void testNonDeterministicFunctionCall()
    { }

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
