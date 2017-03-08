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
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CanonicalizeExpressions
        implements PlanOptimizer
{
    public static Expression canonicalizeExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new CanonicalizeExpressionRewriter(), expression);
    }

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
                            node.getLeft(),
                            node.getRight(),
                            node.getCriteria(),
                            node.getOutputSymbols(),
                            Optional.of(canonicalizedExpression),
                            node.getLeftHashSymbol(),
                            node.getRightHashSymbol(),
                            node.getDistributionType());
                }
            }

            return node;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Assignments assignments = node.getAssignments().rewrite(CanonicalizeExpressions::canonicalizeExpression);
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

    private static class CanonicalizeExpressionRewriter
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteIsNotNullPredicate(IsNotNullPredicate node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression value = treeRewriter.rewrite(node.getValue(), context);
            return new NotExpression(new IsNullPredicate(value));
        }

        @Override
        public Expression rewriteIfExpression(IfExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression condition = treeRewriter.rewrite(node.getCondition(), context);
            Expression trueValue = treeRewriter.rewrite(node.getTrueValue(), context);

            Optional<Expression> falseValue = node.getFalseValue()
                    .map((value) -> treeRewriter.rewrite(value, context));

            return new SearchedCaseExpression(ImmutableList.of(new WhenClause(condition, trueValue)), falseValue);
        }

        @Override
        public Expression rewriteCurrentTime(CurrentTime node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getPrecision() != null) {
                throw new UnsupportedOperationException("not yet implemented: non-default precision");
            }

            switch (node.getType()) {
                case DATE:
                    return new FunctionCall(QualifiedName.of("current_date"), ImmutableList.of());
                case TIME:
                    return new FunctionCall(QualifiedName.of("current_time"), ImmutableList.of());
                case LOCALTIME:
                    return new FunctionCall(QualifiedName.of("localtime"), ImmutableList.of());
                case TIMESTAMP:
                    return new FunctionCall(QualifiedName.of("current_timestamp"), ImmutableList.of());
                case LOCALTIMESTAMP:
                    return new FunctionCall(QualifiedName.of("localtimestamp"), ImmutableList.of());
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getType());
            }
        }

        @Override
        public Expression rewriteExtract(Extract node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression value = treeRewriter.rewrite(node.getExpression(), context);

            switch (node.getField()) {
                case YEAR:
                    return new FunctionCall(QualifiedName.of("year"), ImmutableList.of(value));
                case QUARTER:
                    return new FunctionCall(QualifiedName.of("quarter"), ImmutableList.of(value));
                case MONTH:
                    return new FunctionCall(QualifiedName.of("month"), ImmutableList.of(value));
                case WEEK:
                    return new FunctionCall(QualifiedName.of("week"), ImmutableList.of(value));
                case DAY:
                case DAY_OF_MONTH:
                    return new FunctionCall(QualifiedName.of("day"), ImmutableList.of(value));
                case DAY_OF_WEEK:
                case DOW:
                    return new FunctionCall(QualifiedName.of("day_of_week"), ImmutableList.of(value));
                case DAY_OF_YEAR:
                case DOY:
                    return new FunctionCall(QualifiedName.of("day_of_year"), ImmutableList.of(value));
                case YEAR_OF_WEEK:
                case YOW:
                    return new FunctionCall(QualifiedName.of("year_of_week"), ImmutableList.of(value));
                case HOUR:
                    return new FunctionCall(QualifiedName.of("hour"), ImmutableList.of(value));
                case MINUTE:
                    return new FunctionCall(QualifiedName.of("minute"), ImmutableList.of(value));
                case SECOND:
                    return new FunctionCall(QualifiedName.of("second"), ImmutableList.of(value));
                case TIMEZONE_MINUTE:
                    return new FunctionCall(QualifiedName.of("timezone_minute"), ImmutableList.of(value));
                case TIMEZONE_HOUR:
                    return new FunctionCall(QualifiedName.of("timezone_hour"), ImmutableList.of(value));
            }

            throw new UnsupportedOperationException("not yet implemented: " + node.getField());
        }
    }
}
