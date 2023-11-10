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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizerResult;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;

import static com.facebook.presto.SystemSessionProperties.GENERATE_DOMAIN_FILTERS;
import static java.util.Objects.requireNonNull;

public class GenerateDomainFilters
        implements PlanOptimizer
{
    private final RowExpressionDomainTranslator rowExpressionDomainTranslator;
    private final LogicalRowExpressions logicalRowExpressions;
    private final SqlParser sqlParser;
    private final Metadata metadata;

    public GenerateDomainFilters(Metadata metadata, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
        this.rowExpressionDomainTranslator = new RowExpressionDomainTranslator(metadata);
        this.sqlParser = sqlParser;
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()),
                new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()),
                metadata.getFunctionAndTypeManager());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return session.getSystemProperty(GENERATE_DOMAIN_FILTERS, Boolean.class);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!isEnabled(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        Rewriter rewriter = new Rewriter(logicalRowExpressions, rowExpressionDomainTranslator, sqlParser, metadata, session);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, true);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final LogicalRowExpressions logicalRowExpressions;
        private final RowExpressionDomainTranslator rowExpressionDomainTranslator;
        private final ExpressionEquivalence expressionEquivalence;
        private final Metadata metadata;
        private final Session session;

        public Rewriter(LogicalRowExpressions logicalRowExpressions,
                RowExpressionDomainTranslator rowExpressionDomainTranslator,
                SqlParser sqlParser,
                Metadata metadata,
                Session session)
        {
            this.logicalRowExpressions = logicalRowExpressions;
            this.rowExpressionDomainTranslator = rowExpressionDomainTranslator;
            this.expressionEquivalence = new ExpressionEquivalence(metadata, sqlParser);
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            RowExpression predicate = node.getPredicate();
            TupleDomain<VariableReferenceExpression> inferredTupleDomain = rowExpressionDomainTranslator.fromPredicate(session.toConnectorSession(), predicate).getTupleDomain();

            if (inferredTupleDomain.isAll()) {
                return node;
            }

            RowExpression withTupleDomainPredicates = logicalRowExpressions.combineConjuncts(predicate,
                    rowExpressionDomainTranslator.toPredicate(inferredTupleDomain));

            return areExpressionsEquivalent(predicate, withTupleDomainPredicates) ? node : new FilterNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getSource(),
                    withTupleDomainPredicates);
        }

        private boolean areExpressionsEquivalent(RowExpression leftExpression, RowExpression rightExpression)
        {
            return expressionEquivalence.areExpressionsEquivalent(simplifyExpression(leftExpression), simplifyExpression(rightExpression));
        }

        private RowExpression simplifyExpression(RowExpression expression)
        {
            return new RowExpressionOptimizer(metadata).optimize(expression, ExpressionOptimizer.Level.SERIALIZABLE, session.toConnectorSession());
        }
    }
}
