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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.rule.CanonicalizeExpressionRewriter;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static java.util.Objects.requireNonNull;

@Deprecated
public class CanonicalizeExpressions
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            if (node.getFilter().isPresent()) {
                Expression canonicalizedExpression = canonicalizeExpression(node.getFilter().get());
                if (!canonicalizedExpression.equals(node.getFilter().get())) {
                    return new JoinNode(
                            node.getId(),
                            node.getType(),
                            context.rewrite(node.getLeft()),
                            context.rewrite(node.getRight()),
                            node.getCriteria(),
                            node.getOutputSymbols(),
                            Optional.of(canonicalizedExpression),
                            node.getLeftHashSymbol(),
                            node.getRightHashSymbol(),
                            node.getDistributionType());
                }
            }

            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Assignments assignments = node.getAssignments().rewrite(CanonicalizeExpressionRewriter::canonicalizeExpression);
            return new ProjectNode(node.getId(), source, assignments);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Expression canonicalized = canonicalizeExpression(node.getPredicate());
            if (canonicalized.equals(BooleanLiteral.TRUE_LITERAL)) {
                return source;
            }
            return new FilterNode(node.getId(), source, canonicalized);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            Expression originalConstraint = null;
            if (node.getOriginalConstraint() != null) {
                originalConstraint = canonicalizeExpression(node.getOriginalConstraint());
            }
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    originalConstraint);
        }
    }
}
