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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
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
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionInterpreter;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.rowExpressionInterpreter;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestExpressionInterpreter
        extends TestExpressions
{
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);

    @Test
    public void assertLikeOptimizations()
    {
        assertOptimizedEquals("unbound_string LIKE bound_pattern", "unbound_string LIKE bound_pattern");
    }

    @Override
    protected void assertLike(byte[] value, String pattern, boolean expected)
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
    protected void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertEquals(optimize(actual), optimize(expected));
    }

    @Override
    protected void assertOptimizedMatches(@Language("SQL") String actual, @Language("SQL") String expected)
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
    protected Object optimize(@Language("SQL") String expression)
    {
        assertRoundTrip(expression);

        Expression parsedExpression = expression(expression);
        Object expressionResult = optimize(parsedExpression);

        RowExpression rowExpression = toRowExpression(parsedExpression);
        Object rowExpressionResult = optimizeRowExpression(rowExpression, OPTIMIZED);
        assertExpressionAndRowExpressionEquals(expressionResult, rowExpressionResult);
        return expressionResult;
    }

    @Override
    protected Object optimizeRowExpression(RowExpression expression, ExpressionOptimizer.Level level)
    {
        RowExpressionInterpreter rowExpressionInterpreter = new RowExpressionInterpreter(expression, METADATA, TEST_SESSION.toConnectorSession(), level);
        return rowExpressionInterpreter.optimize(variable -> {
            Symbol symbol = new Symbol(variable.getName());
            Object value = symbolConstant(symbol);
            if (value == null) {
                return new VariableReferenceExpression(Optional.empty(), symbol.getName(), SYMBOL_TYPES.get(symbol.toSymbolReference()));
            }
            return value;
        });
    }

    private static Expression expression(String expression)
    {
        return FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);
    }

    private static RowExpression toRowExpression(Expression expression)
    {
        return TRANSLATOR.translate(expression, SYMBOL_TYPES);
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
    protected void assertDoNotOptimize(@Language("SQL") String expression, Level optimizationLevel)
    {
        assertRoundTrip(expression);
        Expression translatedExpression = expression(expression);
        RowExpression rowExpression = toRowExpression(translatedExpression);

        Object expressionResult = optimize(translatedExpression);
        if (expressionResult instanceof Expression) {
            expressionResult = toRowExpression((Expression) expressionResult);
        }
        Object rowExpressionResult = optimizeRowExpression(rowExpression, optimizationLevel);
        assertRowExpressionEvaluationEquals(expressionResult, rowExpressionResult);
        assertRowExpressionEvaluationEquals(rowExpressionResult, rowExpression);
    }

    private void assertExpressionAndRowExpressionEquals(Object expressionResult, Object rowExpressionResult)
    {
        if (rowExpressionResult instanceof RowExpression) {
            // Cannot be completely evaluated into a constant; compare expressions
            assertTrue(expressionResult instanceof Expression);

            // It is tricky to check the equivalence of an expression and a row expression.
            // We rely on the optimized translator to fill the gap.
            RowExpression translated = TRANSLATOR.translateAndOptimize((Expression) expressionResult, SYMBOL_TYPES);
            assertRowExpressionEvaluationEquals(translated, rowExpressionResult);
        }
        else {
            // We have constants; directly compare
            assertRowExpressionEvaluationEquals(expressionResult, rowExpressionResult);
        }
    }
    @Override
    protected Object evaluate(String expression, boolean deterministic)
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
