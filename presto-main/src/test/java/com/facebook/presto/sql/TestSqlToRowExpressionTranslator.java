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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.util.Collections.emptyList;

public class TestSqlToRowExpressionTranslator
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private final LiteralEncoder literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());

    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        Expression expression = new LongLiteral("1");
        ImmutableMap.Builder<NodeRef<Expression>, Type> types = ImmutableMap.builder();
        types.put(NodeRef.of(expression), BIGINT);
        for (int i = 0; i < 100; i++) {
            expression = new CoalesceExpression(expression, new LongLiteral("2"));
            types.put(NodeRef.of(expression), BIGINT);
        }
        translateAndOptimize(expression, types.build());
    }

    @Test
    public void testOptimizeDecimalLiteral()
    {
        // Short decimal
        assertEquals(translateAndOptimize(expression("CAST(NULL AS DECIMAL(7,2))")), constant(null, createDecimalType(7, 2)));
        assertEquals(translateAndOptimize(expression("DECIMAL '42'")), constant(42L, createDecimalType(2, 0)));
        assertEquals(translateAndOptimize(expression("CAST(42 AS DECIMAL(7,2))")), constant(4200L, createDecimalType(7, 2)));
        assertEquals(translateAndOptimize(simplifyExpression(expression("CAST(42 AS DECIMAL(7,2))"))), constant(4200L, createDecimalType(7, 2)));

        // Long decimal
        assertEquals(translateAndOptimize(expression("CAST(NULL AS DECIMAL(35,2))")), constant(null, createDecimalType(35, 2)));
        assertEquals(
                translateAndOptimize(expression("DECIMAL '123456789012345678901234567890'")),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890")), createDecimalType(30, 0)));
        assertEquals(
                translateAndOptimize(expression("CAST(DECIMAL '123456789012345678901234567890' AS DECIMAL(35,2))")),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
        assertEquals(
                translateAndOptimize(simplifyExpression(expression("CAST(DECIMAL '123456789012345678901234567890' AS DECIMAL(35,2))"))),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
    }

    private RowExpression translateAndOptimize(Expression expression)
    {
        return translateAndOptimize(expression, getExpressionTypes(expression));
    }

    private RowExpression translateAndOptimize(Expression expression, Map<NodeRef<Expression>, Type> types)
    {
        return SqlToRowExpressionTranslator.translate(expression, SCALAR, types, metadata.getFunctionRegistry(), metadata.getTypeManager(), TEST_SESSION, true);
    }

    private Expression simplifyExpression(Expression expression)
    {
        // Testing simplified expressions is important, since simplification may create CASTs or function calls that cannot be simplified by the ExpressionOptimizer

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(expression);
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(expression, metadata, TEST_SESSION, expressionTypes);
        Object value = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
        return literalEncoder.toExpression(value, expressionTypes.get(NodeRef.of(expression)));
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                TEST_SESSION,
                TypeProvider.empty(),
                emptyList(),
                node -> new IllegalStateException("Expected node: %s" + node),
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }
}
