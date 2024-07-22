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
package com.facebook.presto.tests.expressions;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.relational.DelegatingRowExpressionOptimizer;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
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

    @BeforeClass
    public void setup()
    {
        METADATA.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of(APPLY_FUNCTION));
        setupJsonFunctionNamespaceManager(METADATA.getFunctionAndTypeManager());

        expressionOptimizer = new DelegatingRowExpressionOptimizer(METADATA, TestNativeExpressions.getExpressionOptimizer(METADATA, HANDLE_RESOLVER));
    }

    @Test
    public void assertLikeOptimizations()
    {
        assertOptimizedMatches("unbound_string LIKE bound_pattern", "unbound_string LIKE CAST('%el%' AS varchar)");
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
    protected Object optimize(@Language("SQL") String expression)
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
    protected void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        Object optimizedActual = optimize(actual);
        Object optimizedExpected = optimize(expected);
        assertEquals(optimizedActual, optimizedExpected);
    }

    @Override
    protected void assertOptimizedMatches(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        Object actualOptimized = optimize(actual);
        Object expectedOptimized = optimize(expected);
        assertRowExpressionEvaluationEquals(
                actualOptimized,
                expectedOptimized);
    }

    @Override
    protected void assertDoNotOptimize(@Language("SQL") String expression, Level optimizationLevel)
    {
        assertRoundTrip(expression);
        RowExpression rowExpression = sqlToRowExpression(expression);
        Object rowExpressionResult = optimizeRowExpression(rowExpression, optimizationLevel);
        assertRowExpressionEvaluationEquals(rowExpressionResult, rowExpression);
    }
}
