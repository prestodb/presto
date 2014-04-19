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

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CanonicalizeExpressions
        extends PlanOptimizer
{
    public static Expression canonicalizeExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new CanonicalizeExpressionRewriter(), expression);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        @Override
        public PlanNode rewriteProject(ProjectNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);
            Map<Symbol, Expression> assignments = ImmutableMap.copyOf(Maps.transformValues(node.getOutputMap(), canonicalizeExpressionFunction()));
            return new ProjectNode(node.getId(), source, assignments);
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);
            Expression canonicalized = canonicalizeExpression(node.getPredicate());
            if (canonicalized.equals(BooleanLiteral.TRUE_LITERAL)) {
                return source;
            }
            return new FilterNode(node.getId(), source, canonicalized);
        }

        @Override
        public PlanNode rewriteTableScan(TableScanNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            Expression originalConstraint = null;
            if (node.getOriginalConstraint() != null) {
                originalConstraint = canonicalizeExpression(node.getOriginalConstraint());
            }
            return new TableScanNode(node.getId(), node.getTable(), node.getOutputSymbols(), node.getAssignments(), originalConstraint, node.getGeneratedPartitions());
        }

        private Function<Expression, Expression> canonicalizeExpressionFunction()
        {
            return new Function<Expression, Expression>()
            {
                @Override
                public Expression apply(Expression input)
                {
                    return canonicalizeExpression(input);
                }
            };
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

            Expression falseValue = null;
            if (node.getFalseValue().isPresent()) {
                falseValue = treeRewriter.rewrite(node.getFalseValue().get(), context);
            }

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
                    return new FunctionCall(new QualifiedName("current_date"), ImmutableList.<Expression>of());
                case TIME:
                    return new FunctionCall(new QualifiedName("current_time"), ImmutableList.<Expression>of());
                case LOCALTIME:
                    return new FunctionCall(new QualifiedName("localtime"), ImmutableList.<Expression>of());
                case TIMESTAMP:
                    return new FunctionCall(new QualifiedName("now"), ImmutableList.<Expression>of());
                case LOCALTIMESTAMP:
                    return new FunctionCall(new QualifiedName("localtimestamp"), ImmutableList.<Expression>of());
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
                    return new FunctionCall(new QualifiedName("year"), ImmutableList.of(value));
                case QUARTER:
                    return new FunctionCall(new QualifiedName("quarter"), ImmutableList.of(value));
                case MONTH:
                    return new FunctionCall(new QualifiedName("month"), ImmutableList.of(value));
                case WEEK:
                    return new FunctionCall(new QualifiedName("week"), ImmutableList.of(value));
                case DAY:
                case DAY_OF_MONTH:
                    return new FunctionCall(new QualifiedName("day"), ImmutableList.of(value));
                case DAY_OF_WEEK:
                case DOW:
                    return new FunctionCall(new QualifiedName("day_of_week"), ImmutableList.of(value));
                case DAY_OF_YEAR:
                case DOY:
                    return new FunctionCall(new QualifiedName("day_of_year"), ImmutableList.of(value));
                case HOUR:
                    return new FunctionCall(new QualifiedName("hour"), ImmutableList.of(value));
                case MINUTE:
                    return new FunctionCall(new QualifiedName("minute"), ImmutableList.of(value));
                case SECOND:
                    return new FunctionCall(new QualifiedName("second"), ImmutableList.of(value));
                case TIMEZONE_MINUTE:
                    return new FunctionCall(new QualifiedName("timezone_minute"), ImmutableList.of(value));
                case TIMEZONE_HOUR:
                    return new FunctionCall(new QualifiedName("timezone_hour"), ImmutableList.of(value));
            }

            throw new UnsupportedOperationException("not yet implemented: " + node.getField());
        }
    }
}
