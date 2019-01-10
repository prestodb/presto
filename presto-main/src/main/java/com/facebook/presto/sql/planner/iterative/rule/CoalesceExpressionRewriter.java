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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CoalesceExpressionRewriter
{
    public static Expression simplifyCoalesceExpression(Session session, ExpressionEquivalence expressionEquivalence, Expression expression, TypeProvider typeProvider)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(session, expressionEquivalence, typeProvider), expression);
    }

    private CoalesceExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final ExpressionEquivalence expressionEquivalence;
        private final Session session;
        private final TypeProvider typeProvider;

        private Visitor(Session session, ExpressionEquivalence expressionEquivalence, TypeProvider typeProvider)
        {
            this.session = requireNonNull(session, "Session is null");
            this.expressionEquivalence = requireNonNull(expressionEquivalence, "ExpressionEquivalence is null");
            this.typeProvider = requireNonNull(typeProvider, "TypeProvider is null");
        }

        @Override
        public Expression rewriteCoalesceExpression(CoalesceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            ImmutableList.Builder<Expression> operandsBuilder = ImmutableList.builder();
            Map<RowExpression, Expression> rowExpressionMap = new HashMap<>();
            for (Expression operand : node.getOperands()) {
                if (DeterminismEvaluator.isDeterministic(operand)) {
                    RowExpression rowExpression = expressionEquivalence.toCanonicalizedRowExpression(session, operand, typeProvider);
                    rowExpressionMap.putIfAbsent(rowExpression, operand);
                    operandsBuilder.add(rowExpressionMap.get(rowExpression));
                }
                else {
                    operandsBuilder.add(operand);
                }
            }
            return new CoalesceExpression(operandsBuilder.build());
        }
    }
}
