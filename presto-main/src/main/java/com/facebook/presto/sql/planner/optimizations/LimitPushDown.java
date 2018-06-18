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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LimitPushDown
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class LimitContext
    {
        private final long count;
        private final boolean partial;

        public LimitContext(long count, boolean partial)
        {
            this.count = count;
            this.partial = partial;
        }

        public long getCount()
        {
            return count;
        }

        public boolean isPartial()
        {
            return partial;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("count", count)
                    .add("partial", partial)
                    .toString();
        }
    }

    private static class Rewriter
            extends SimplePlanRewriter<LimitContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<LimitContext> context)
        {
            PlanNode rewrittenNode = context.defaultRewrite(node);

            LimitContext limit = context.get();
            if (limit != null) {
                // Drop in a LimitNode b/c we cannot push our limit down any further
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, limit.getCount(), limit.isPartial());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<LimitContext> context)
        {
            long count = node.getCount();
            if (context.get() != null) {
                count = Math.min(count, context.get().getCount());
            }

            // return empty ValuesNode in case of limit 0
            if (count == 0) {
                return new ValuesNode(idAllocator.getNextId(),
                        node.getOutputSymbols(),
                        ImmutableList.of());
            }

            // default visitPlan logic will insert the limit node
            return context.rewrite(node.getSource(), new LimitContext(count, false));
        }

        @Override
        @Deprecated
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<LimitContext> context)
        {
            LimitContext limit = context.get();

            if (limit != null &&
                    node.getAggregations().isEmpty() &&
                    node.getOutputSymbols().size() == node.getGroupingKeys().size() &&
                    node.getOutputSymbols().containsAll(node.getGroupingKeys())) {
                PlanNode rewrittenSource = context.rewrite(node.getSource());
                return new DistinctLimitNode(idAllocator.getNextId(), rewrittenSource, limit.getCount(), false, rewrittenSource.getOutputSymbols(), Optional.empty());
            }
            PlanNode rewrittenNode = context.defaultRewrite(node);
            if (limit != null) {
                // Drop in a LimitNode b/c limits cannot be pushed through aggregations
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, limit.getCount(), limit.isPartial());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<LimitContext> context)
        {
            // the fallback logic (in visitPlan) for node types we don't know about introduces a limit node,
            // so we need this here to push the limit through this trivial node type
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<LimitContext> context)
        {
            // the fallback logic (in visitPlan) for node types we don't know about introduces a limit node,
            // so we need this here to push the limit through this trivial node type
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<LimitContext> context)
        {
            LimitContext limit = context.get();

            PlanNode rewrittenSource = context.rewrite(node.getSource());
            if (rewrittenSource == node.getSource() && limit == null) {
                return node;
            }

            long count = node.getCount();
            if (limit != null) {
                count = Math.min(count, limit.getCount());
            }
            return new TopNNode(node.getId(), rewrittenSource, count, node.getOrderingScheme(), node.getStep());
        }

        @Override
        @Deprecated
        public PlanNode visitSort(SortNode node, RewriteContext<LimitContext> context)
        {
            LimitContext limit = context.get();

            PlanNode rewrittenSource = context.rewrite(node.getSource());
            if (limit != null) {
                return new TopNNode(node.getId(), rewrittenSource, limit.getCount(), node.getOrderingScheme(), TopNNode.Step.SINGLE);
            }
            else if (rewrittenSource != node.getSource()) {
                return new SortNode(node.getId(), rewrittenSource, node.getOrderingScheme());
            }
            return node;
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<LimitContext> context)
        {
            LimitContext limit = context.get();

            LimitContext childLimit = null;
            if (limit != null) {
                childLimit = new LimitContext(limit.getCount(), true);
            }

            List<PlanNode> sources = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                sources.add(context.rewrite(node.getSources().get(i), childLimit));
            }

            PlanNode output = new UnionNode(node.getId(), sources, node.getSymbolMapping(), node.getOutputSymbols());
            if (limit != null) {
                output = new LimitNode(idAllocator.getNextId(), output, limit.getCount(), limit.isPartial());
            }
            return output;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<LimitContext> context)
        {
            PlanNode source = context.rewrite(node.getSource(), context.get());
            if (source != node.getSource()) {
                return new SemiJoinNode(
                        node.getId(),
                        source,
                        node.getFilteringSource(),
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType());
            }
            return node;
        }
    }
}
