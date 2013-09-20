package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.*;
import com.facebook.presto.sql.tree.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImplementSampleAsFilter
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(), plan, null);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        @Override
        public PlanNode rewriteSample(SampleNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), context);

            ComparisonExpression expression = new ComparisonExpression(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, new FunctionCall(QualifiedName.of("rand"), ImmutableList.<Expression>of()), new DoubleLiteral(Double.toString (node.getSampleRatio())));
            return new FilterNode(node.getId(), rewrittenSource, expression);
        }
    }
}
