package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SetFlatteningOptimizer
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(idAllocator, "idAllocator is null");

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
            ImmutableListMultimap.Builder<Symbol, Symbol> flattenedSymbolMap = ImmutableListMultimap.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                PlanNode rewrittenSource = planRewriter.rewrite(subplan, upstreamDistinct);

                if (rewrittenSource instanceof UnionNode) {
                    // Absorb source's subplans if it is also a UnionNode
                    UnionNode rewrittenUnion = (UnionNode) rewrittenSource;
                    flattenedSources.addAll(rewrittenUnion.getSources());
                    for (Map.Entry<Symbol, Collection<Symbol>> entry : node.getSymbolMapping().asMap().entrySet()) {
                        Symbol inputSymbol = Iterables.get(entry.getValue(), i);
                        flattenedSymbolMap.putAll(entry.getKey(), rewrittenUnion.getSymbolMapping().get(inputSymbol));
                    }
                }
                else {
                    flattenedSources.add(rewrittenSource);
                    for (Map.Entry<Symbol, Collection<Symbol>> entry : node.getSymbolMapping().asMap().entrySet()) {
                        flattenedSymbolMap.put(entry.getKey(), Iterables.get(entry.getValue(), i));
                    }
                }
            }
            return new UnionNode(node.getId(), flattenedSources.build(), flattenedSymbolMap.build());
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, Boolean upstreamDistinct, PlanRewriter<Boolean> planRewriter)
        {
            boolean distinct = isDistinctOperator(node);

            PlanNode rewrittenNode = planRewriter.rewrite(node.getSource(), distinct);

            if (upstreamDistinct && distinct) {
                // Assumes underlying node has same output symbols as this distinct node
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
