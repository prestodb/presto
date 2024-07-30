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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.EVALUATED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestDelegatingRowExpressionOptimizer
{
    private DelegatingRowExpressionOptimizer optimizer;
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);

    @BeforeClass
    public void setUp()
    {
        optimizer = new DelegatingRowExpressionOptimizer(METADATA, InnerOptimizer::new, 3);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        optimizer = null;
    }

    @Test
    public void testBasicExpressions()
    {
        assertEquals(optimize(expression("BIGINT'1' + 1")), expression("BIGINT'2'"));
        assertEquals(optimize(expression("IF(TRUE, 1, 2)")), expression("1"));
    }

    @Test
    public void testVariableReference()
    {
        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), "x", BIGINT);
        assertEquals(optimize(variable), variable);
        ImmutableMap<String, Type> typeMap = ImmutableMap.of("x", BIGINT);
        assertEquals(optimize(expression("x + 1", typeMap)), expression("x + 1", typeMap));
        assertEquals(optimize(expression("x + 1", typeMap), ImmutableMap.of(variable, 1L)), expression("BIGINT'2'"));
    }

    @Test
    public void testComplexExpressions()
    {
        assertEquals(optimize(expression("IF(TRUE, 1, 2) + 3")), expression("4"));
        assertEquals(optimize(expression("IF(TRUE, 1, 2) + 3 + 4")), expression("8"));
        assertEquals(optimize(expression("IF(TRUE, 1, 2) + 3 + 4 + 5")), expression("8 + 5"));

        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), "x", INTEGER);
        ImmutableMap<String, Type> typeMap = ImmutableMap.of("x", INTEGER);
        assertEquals(optimize(expression("IF(TRUE, 1, 2) + x", typeMap), ImmutableMap.of(variable, 3L)), expression("4"));
        assertEquals(optimize(expression("IF(TRUE, 1, 2) + x + 4", typeMap), ImmutableMap.of(variable, 3L)), expression("8"));
        assertEquals(optimize(expression("IF(TRUE, 1, 2) + x + 4 + 5", typeMap), ImmutableMap.of(variable, 3L)), expression("8 + 5"));
    }

    @Test
    public void testDifferentOptimizationLevels()
    {
        assertEquals(optimize(expression("rand(10) + 1 > 0"), EVALUATED), expression("true"));
        assertEquals(optimize(expression("rand(10) + 1 > 0"), OPTIMIZED), expression("rand(10) + 1 > 0"));
        assertEquals(optimize(expression("rand(10) + 1 > 0"), SERIALIZABLE), expression("rand(10) + 1 > 0"));
    }

    private Object optimize(RowExpression expression, Map<VariableReferenceExpression, Object> variableMap)
    {
        return optimize(expression, variableMap, OPTIMIZED);
    }

    private Object optimize(RowExpression expression, Map<VariableReferenceExpression, Object> variableMap, Level level)
    {
        return optimizer.optimize(expression, level, SESSION, variableMap::get);
    }

    private RowExpression optimize(RowExpression expression)
    {
        return optimize(expression, OPTIMIZED);
    }

    private RowExpression optimize(RowExpression expression, Level level)
    {
        return optimizer.optimize(expression, level, SESSION);
    }

    private static RowExpression expression(String expressionSql)
    {
        return expression(expressionSql, ImmutableMap.of());
    }

    private static RowExpression expression(String expressionSql, Map<String, Type> typeMap)
    {
        return TRANSLATOR.translate(expressionSql, typeMap);
    }

    private static class InnerOptimizer
            implements ExpressionOptimizer
    {
        @Override
        public RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session)
        {
            OneLevelDeepExpressionRewriter rewriter = new OneLevelDeepExpressionRewriter(level, variable -> variable);
            return rowExpression.accept(rewriter, null);
        }

        @Override
        public Object optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
        {
            OneLevelDeepExpressionRewriter rewriter = new OneLevelDeepExpressionRewriter(level, variableResolver);
            return expression.accept(rewriter, null);
        }
    }

    // This visitor will only rewrite the first expression it comes across.  It is intended to be used to test
    // the DelegatingRowExpressionOptimizer, which will call the inner optimizer multiple times.
    private static class OneLevelDeepExpressionRewriter
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final RowExpressionOptimizer innerOptimizer = new RowExpressionOptimizer(METADATA);
        private final Level level;
        private final Function<VariableReferenceExpression, Object> variableResolver;

        private boolean rewritten;

        public OneLevelDeepExpressionRewriter(Level level, Function<VariableReferenceExpression, Object> variableResolver)
        {
            this.level = level;
            this.variableResolver = requireNonNull(variableResolver, "variableResolver is null");
        }

        @Override
        public RowExpression visitExpression(RowExpression node, Void context)
        {
            return node;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            if (variableResolver == null) {
                return reference;
            }
            Object value = variableResolver.apply(reference);
            if (value == null) {
                return reference;
            }
            return toRowExpression(value, reference.getType());
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            List<RowExpression> arguments = call.getArguments().stream()
                    .map(argument -> argument.accept(this, context))
                    .collect(toImmutableList());
            if (!rewritten) {
                RowExpression rewritten = toRowExpression(innerOptimizer.optimize(call, level, SESSION, variableResolver), call.getType());
                if (!rewritten.equals(call)) {
                    this.rewritten = true;
                    return rewritten;
                }
            }
            return call(call.getDisplayName(), call.getFunctionHandle(), call.getType(), arguments);
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            List<RowExpression> arguments = specialForm.getArguments().stream()
                    .map(argument -> argument.accept(this, context))
                    .collect(toImmutableList());
            if (!rewritten) {
                RowExpression rewritten = toRowExpression(innerOptimizer.optimize(specialForm, OPTIMIZED, SESSION, variableResolver), specialForm.getType());
                if (!rewritten.equals(specialForm)) {
                    this.rewritten = true;
                    return rewritten;
                }
            }
            return new SpecialFormExpression(specialForm.getForm(), specialForm.getType(), arguments);
        }
    }
}
