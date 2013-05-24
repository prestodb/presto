package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SetFlatteningOptimizer
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");

        return PlanRewriter.rewriteWith(new Rewriter(), plan, false);
    }

    // TODO: remove expectation that UNION DISTINCT => distinct aggregation directly above union node
    private static class Rewriter
            extends PlanNodeRewriter<Boolean>
    {
        @Override
        public PlanNode rewriteNode(PlanNode node, Boolean upstreamDistinct, PlanRewriter<Boolean> planRewriter)
        {
            return super.rewriteNode(node, false, planRewriter);
        }

        @Override
        public PlanNode rewriteUnion(UnionNode node, Boolean upstreamDistinct, PlanRewriter<Boolean> planRewriter)
        {
            ImmutableList.Builder<PlanNode> flattenedSources = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                PlanNode rewrittenSource = planRewriter.rewrite(source, upstreamDistinct);

                if (rewrittenSource instanceof UnionNode) {
                    // Absorb source's subplans if it is also a UnionNode
                    flattenedSources.addAll(rewrittenSource.getSources());
                }
                else {
                    flattenedSources.add(rewrittenSource);
                }
            }
            return new UnionNode(node.getId(), flattenedSources.build(), node.getOutputSymbols());
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, Boolean upstreamDistinct, PlanRewriter<Boolean> planRewriter)
        {
            boolean distinct = isDistinctOperator(node);

            PlanNode rewrittenNode = planRewriter.rewrite(node.getSource(), distinct);

            if (upstreamDistinct && distinct) {
                return rewrittenNode;
            }

            return new AggregationNode(node.getId(), rewrittenNode, node.getGroupBy(), node.getAggregations(), node.getFunctions());
        }

        private static boolean isDistinctOperator(AggregationNode node)
        {
            return node.getAggregations().isEmpty();
        }
    }
}
