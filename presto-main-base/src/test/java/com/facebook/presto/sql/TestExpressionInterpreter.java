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
package com.facebook.presto.sql;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.expressions.AbstractTestExpressionInterpreter;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionInterpreter;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.rowExpressionInterpreter;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestExpressionInterpreter
        extends AbstractTestExpressionInterpreter
{
    @BeforeClass
    public void setup()
    {
        METADATA.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of(APPLY_FUNCTION));
        setupJsonFunctionNamespaceManager(METADATA.getFunctionAndTypeManager());
    }

    // Run this method exactly once.
    private void setupJsonFunctionNamespaceManager(FunctionAndTypeManager functionAndTypeManager)
    {
        functionAndTypeManager.addFunctionNamespaceFactory(new JsonFileBasedFunctionNamespaceManagerFactory());
        functionAndTypeManager.loadFunctionNamespaceManager(
                JsonFileBasedFunctionNamespaceManagerFactory.NAME,
                "json",
                ImmutableMap.of("supported-function-languages", "CPP", "function-implementation-type", "CPP"),
                new TestingNodeManager());
    }

    @Test
    public void testBind()
    {
        assertOptimizedEquals("apply(90, \"$internal$bind\"(9, (x, y) -> x + y))", "apply(90, \"$internal$bind\"(9, (x, y) -> x + y))");
        evaluate("apply(90, \"$internal$bind\"(9, (x, y) -> x + y))", true);
        evaluate("apply(900, \"$internal$bind\"(90, 9, (x, y, z) -> x + y + z))", true);
    }

    @Test
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
        assertEquals(evaluate("reduce(ARRAY[1, 5], 0, (x, y) -> x + y, x -> x)", true), 6L);
    }

    @Test
    public void testMassiveArray()
    {
        assertRowExpressionEquals(
                OPTIMIZED,
                "SEQUENCE(1, 999)",
                format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(1, 1000).mapToObj(i -> "(BIGINT '" + i + "')").iterator())));
        assertDoNotOptimize("SEQUENCE(1, 1000)", SERIALIZABLE);
        optimize(format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "(bound_long + " + i + ")").iterator())));
        optimize(format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "(bound_integer + " + i + ")").iterator())));
        optimize(format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "'" + i + "'").iterator())));
        optimize(format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "ARRAY['" + i + "']").iterator())));
    }

    @Test(timeOut = 60000)
    public void testLikeInvalidUtf8()
    {
        assertLike(new byte[] {'a', 'b', 'c'}, "%b%", true);
        assertLike(new byte[] {'a', 'b', 'c', (byte) 0xFF, 'x', 'y'}, "%b%", true);
    }

    @Test
    public void testLikeSerializable()
    {
        assertRowExpressionEquals(SERIALIZABLE, "'%' LIKE 'z%' ESCAPE 'z'", "true");
        assertRowExpressionEquals(SERIALIZABLE, "'%' LIKE 'z%'", "false");
    }

    private void assertLike(byte[] value, String pattern, boolean expected)
    {
        Expression predicate = new LikePredicate(
                rawStringLiteral(Slices.wrappedBuffer(value)),
                new StringLiteral(pattern),
                Optional.empty());
        assertEquals(evaluate(predicate, true), expected);
    }

    private static StringLiteral rawStringLiteral(final Slice slice)
    {
        return new StringLiteral(slice.toStringUtf8())
        {
            @Override
            public Slice getSlice()
            {
                return slice;
            }
        };
    }

    @Override
    public void assertDoNotOptimize(@Language("SQL") String expression, ExpressionOptimizer.Level optimizationLevel)
    {
        assertRoundTrip(expression);
        Expression translatedExpression = expression(expression);
        RowExpression rowExpression = toRowExpression(translatedExpression);

        Object expressionResult = optimize(translatedExpression);
        if (expressionResult instanceof Expression) {
            expressionResult = toRowExpression((Expression) expressionResult);
        }
        Object rowExpressionResult = optimize(rowExpression, optimizationLevel);
        assertRowExpressionEvaluationEquals(expressionResult, rowExpressionResult);
        assertRowExpressionEvaluationEquals(rowExpressionResult, rowExpression);
    }

    private void assertRowExpressionEquals(ExpressionOptimizer.Level level, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        Object actualResult = optimize(toRowExpression(expression(actual)), level);
        Object expectedResult = optimize(toRowExpression(expression(expected)), level);
        if (actualResult instanceof Block && expectedResult instanceof Block) {
            assertEquals(blockToSlice((Block) actualResult), blockToSlice((Block) expectedResult));
            return;
        }
        assertEquals(actualResult, expectedResult);
    }

    @Override
    public void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertEquals(optimize(actual), optimize(expected));
    }

    @Override
    public void assertOptimizedMatches(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        // replaces FunctionCalls to FailureFunction by fail()
        Object actualOptimized = optimize(actual);
        if (actualOptimized instanceof Expression) {
            actualOptimized = ExpressionTreeRewriter.rewriteWith(new FailedFunctionRewriter(), (Expression) actualOptimized);
        }
        assertEquals(
                actualOptimized,
                rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expected)));
    }

    @Override
    public Object optimize(@Language("SQL") String expression)
    {
        assertRoundTrip(expression);

        Expression parsedExpression = expression(expression);
        Object expressionResult = optimize(parsedExpression);

        RowExpression rowExpression = toRowExpression(parsedExpression);
        Object rowExpressionResult = optimize(rowExpression, OPTIMIZED);
        assertExpressionAndRowExpressionEquals(expressionResult, rowExpressionResult);
        return expressionResult;
    }

    @Override
    public void assertEvaluatedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertEquals(evaluate(actual, true), evaluate(expected, true));
    }

    private Object optimize(RowExpression expression, ExpressionOptimizer.Level level)
    {
        return new RowExpressionInterpreter(expression, METADATA, TEST_SESSION.toConnectorSession(), level).optimize(variable -> {
            Symbol symbol = new Symbol(variable.getName());
            Object value = symbolConstant(symbol);
            if (value == null) {
                return new VariableReferenceExpression(Optional.empty(), symbol.getName(), SYMBOL_TYPES.get(symbol.toSymbolReference()));
            }
            return value;
        });
    }

    private Object optimize(Expression expression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(TEST_SESSION, METADATA, SQL_PARSER, SYMBOL_TYPES, expression, emptyMap(), WarningCollector.NOOP);
        ExpressionInterpreter interpreter = expressionOptimizer(expression, METADATA, TEST_SESSION, expressionTypes);
        return interpreter.optimize(variable -> {
            Symbol symbol = new Symbol(variable.getName());
            Object value = symbolConstant(symbol);
            if (value == null) {
                return symbol.toSymbolReference();
            }
            return value;
        });
    }

    @Override
    public Object evaluate(@Language("SQL") String expression, boolean deterministic)
    {
        assertRoundTrip(expression);

        Expression parsedExpression = FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);

        return evaluate(parsedExpression, deterministic);
    }

    private Object evaluate(Expression expression, boolean deterministic)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(TEST_SESSION, METADATA, SQL_PARSER, SYMBOL_TYPES, expression, emptyMap(), WarningCollector.NOOP);
        Object expressionResult = expressionInterpreter(expression, METADATA, TEST_SESSION, expressionTypes).evaluate();
        Object rowExpressionResult = rowExpressionInterpreter(TRANSLATOR.translateAndOptimize(expression), METADATA.getFunctionAndTypeManager(), TEST_SESSION.toConnectorSession()).evaluate();

        if (deterministic) {
            assertExpressionAndRowExpressionEquals(expressionResult, rowExpressionResult);
        }
        return expressionResult;
    }

    private static class FailedFunctionRewriter
            extends ExpressionRewriter<Object>
    {
        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Object context, ExpressionTreeRewriter<Object> treeRewriter)
        {
            if (node.getName().equals(QualifiedName.of("fail"))) {
                return new FunctionCall(QualifiedName.of("fail"), ImmutableList.of(node.getArguments().get(0), new StringLiteral("ignored failure message")));
            }
            return node;
        }
    }
}
