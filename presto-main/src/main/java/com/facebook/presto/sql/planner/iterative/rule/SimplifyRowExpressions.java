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

import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.google.common.annotations.VisibleForTesting;

import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SimplifyRowExpressions
        extends RowExpressionRewriteRuleSet
{
    public SimplifyRowExpressions(Metadata metadata)
    {
        super(new Rewriter(metadata));
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final RowExpressionOptimizer optimizer;
        private final LogicalExpressionRewriter logicalExpressionRewriter;

        public Rewriter(Metadata metadata)
        {
            requireNonNull(metadata, "metadata is null");
            this.optimizer = new RowExpressionOptimizer(metadata);
            this.logicalExpressionRewriter = new LogicalExpressionRewriter(metadata.getFunctionAndTypeManager());
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return rewrite(expression, context.getSession().toConnectorSession());
        }

        private RowExpression rewrite(RowExpression expression, ConnectorSession session)
        {
            RowExpression optimizedRowExpression = optimizer.optimize(expression, SERIALIZABLE, session);
            if (optimizedRowExpression instanceof ConstantExpression || !BooleanType.BOOLEAN.equals(optimizedRowExpression.getType())) {
                return optimizedRowExpression;
            }
            return RowExpressionTreeRewriter.rewriteWith(logicalExpressionRewriter, optimizedRowExpression, true);
        }
    }

    @VisibleForTesting
    public static RowExpression rewrite(RowExpression expression, Metadata metadata, ConnectorSession session)
    {
        return new Rewriter(metadata).rewrite(expression, session);
    }

    private static class LogicalExpressionRewriter
            extends RowExpressionRewriter<Boolean>
    {
        private final FunctionResolution functionResolution;
        private final LogicalRowExpressions logicalRowExpressions;

        public LogicalExpressionRewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(functionAndTypeManager, "functionManager is null");
            this.functionResolution = new FunctionResolution(functionAndTypeManager);
            this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(functionAndTypeManager), new FunctionResolution(functionAndTypeManager), functionAndTypeManager);
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Boolean isRoot, RowExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (functionResolution.isNotFunction(node.getFunctionHandle())) {
                checkState(BooleanType.BOOLEAN.equals(node.getType()), "NOT must be boolean function");
                return rewriteBooleanExpression(node, isRoot);
            }
            if (isRoot) {
                return treeRewriter.rewrite(node, false);
            }
            return null;
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Boolean isRoot, RowExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (isConjunctiveDisjunctive(node.getForm())) {
                checkState(BooleanType.BOOLEAN.equals(node.getType()), "AND/OR must be boolean function");
                return rewriteBooleanExpression(node, isRoot);
            }
            if (isRoot) {
                return treeRewriter.rewrite(node, false);
            }
            return null;
        }

        private boolean isConjunctiveDisjunctive(Form form)
        {
            return form == AND || form == OR;
        }

        private RowExpression rewriteBooleanExpression(RowExpression expression, boolean isRoot)
        {
            if (isRoot) {
                return logicalRowExpressions.convertToConjunctiveNormalForm(expression);
            }
            return logicalRowExpressions.minimalNormalForm(expression);
        }
    }
}
