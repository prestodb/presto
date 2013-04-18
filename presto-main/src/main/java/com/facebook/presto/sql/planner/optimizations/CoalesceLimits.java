package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Merges successive LIMIT operators into a single LIMIT that's the minimum of the entire chain
 */
public class CoalesceLimits
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");

        return PlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
        extends PlanNodeRewriter<Void>
    {
        @Override
        public PlanNode rewriteLimit(LimitNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            if (source instanceof LimitNode) {
                LimitNode limitNode = (LimitNode) source;
                return new LimitNode(node.getId(), limitNode.getSource(), Math.min(node.getCount(), limitNode.getCount()));
            }
            else if (source != node.getSource()) {
                return new LimitNode(node.getId(), source, node.getCount());
            }

            return node;
        }
    }
}
