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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;

import java.util.List;

public class PlanVerifiersFactory
        implements Provider<List<PlanVerifier>>
{
    private final List<PlanVerifier> verifiers;

    public PlanVerifiersFactory()
    {
        this.verifiers = ImmutableList.of(new NoSubqueryExpressionLeft());
    }

    @Override
    public List<PlanVerifier> get()
    {
        return verifiers;
    }

    public void verify(Plan plan)
    {
        for (PlanVerifier verifier : verifiers) {
            verifier.verify(plan.getRoot());
        }
    }

    private class NoSubqueryExpressionLeft
            implements PlanVerifier
    {
        @Override
        public void verify(PlanNode plan)
        {
            ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
            plan.accept(new ExpressionExtractor(), expressionsBuilder);
            for (Expression expression : expressionsBuilder.build()) {
                new DefaultTraversalVisitor<Void, Void>()
                {
                    @Override
                    protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
                    {
                        throw new SemanticException(SemanticErrorCode.NOT_SUPPORTED, node, "Given subquery is not yet supported");
                    }
                }.process(expression, null);
            }
        }
    }

    private final class ExpressionExtractor
            extends SimplePlanVisitor<ImmutableList.Builder<Expression>>
    {
        @Override
        public Void visitFilter(FilterNode node, ImmutableList.Builder<Expression> context)
        {
            context.add(node.getPredicate());
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitProject(ProjectNode node, ImmutableList.Builder<Expression> context)
        {
            context.addAll(node.getExpressions());
            return super.visitProject(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, ImmutableList.Builder<Expression> context)
        {
            context.add(node.getOriginalConstraint());
            return super.visitTableScan(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, ImmutableList.Builder<Expression> context)
        {
            for (List<Expression> row : node.getRows()) {
                context.addAll(row);
            }
            return super.visitValues(node, context);
        }
    }
}
