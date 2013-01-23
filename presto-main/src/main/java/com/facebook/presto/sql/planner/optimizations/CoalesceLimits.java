package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;

import java.util.Map;

/**
 * Merges successive LIMIT operators into a single LIMIT that's the minimum of the entire chain
 */
public class CoalesceLimits
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Map<Symbol, Type> types)
    {
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
                return new LimitNode(limitNode.getSource(), Math.min(node.getCount(), limitNode.getCount()));
            }
            else if (source != node.getSource()) {
                return new LimitNode(source, node.getCount());
            }

            return node;
        }
    }
}
