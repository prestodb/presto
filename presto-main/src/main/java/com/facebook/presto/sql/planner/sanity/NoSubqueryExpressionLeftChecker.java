package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.lang.String.format;

public final class NoSubqueryExpressionLeftChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan)
    {
        for (Expression expression : ExpressionExtractor.extractExpressions(plan)) {
            new DefaultTraversalVisitor<Void, Void>()
            {
                @Override
                protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
                {
                    throw new IllegalStateException(format("Unexpected subquery expression in logical plan: %s", node));
                }
            }.process(expression, null);
        }
    }

    private static class ExpressionExtractor
            extends SimplePlanVisitor<ImmutableList.Builder<Expression>>
    {
        public static List<Expression> extractExpressions(PlanNode plan)
        {
            ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
            plan.accept(new ExpressionExtractor(), expressionsBuilder);
            return expressionsBuilder.build();
        }

        @Override
        public Void visitFilter(FilterNode node, ImmutableList.Builder<Expression> context)
        {
            context.add(node.getPredicate());
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitProject(ProjectNode node, ImmutableList.Builder<Expression> context)
        {
            context.addAll(node.getAssignments().values());
            return super.visitProject(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, ImmutableList.Builder<Expression> context)
        {
            if (node.getOriginalConstraint() != null) {
                context.add(node.getOriginalConstraint());
            }
            return super.visitTableScan(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, ImmutableList.Builder<Expression> context)
        {
            node.getRows().forEach(context::addAll);
            return super.visitValues(node, context);
        }
    }
}
