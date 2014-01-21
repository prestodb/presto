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

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.base.Optional;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class LimitPushDown
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

        return PlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class Rewriter
            extends PlanNodeRewriter<LimitNode>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode rewriteNode(PlanNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            PlanNode rewrittenNode = planRewriter.defaultRewrite(node, null);
            if (limit != null) {
                // Drop in a LimitNode b/c we cannot push our limit down any further
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, limit.getCount(), limit.getSampleWeight());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode rewriteLimit(LimitNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            if (limit != null && limit.getCount() < node.getCount()) {
                return planRewriter.rewrite(node.getSource(), limit);
            }
            else {
                return planRewriter.rewrite(node.getSource(), node);
            }
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            if (limit != null &&
                    node.getAggregations().isEmpty() &&
                    node.getOutputSymbols().size() == node.getGroupBy().size() &&
                    node.getOutputSymbols().containsAll(node.getGroupBy())) {
                PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), null);
                return new DistinctLimitNode(idAllocator.getNextId(), rewrittenSource, limit.getCount());
            }
            PlanNode rewrittenNode = planRewriter.defaultRewrite(node, null);
            if (limit != null) {
                // Drop in a LimitNode b/c limits cannot be pushed through aggregations
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, limit.getCount(), limit.getSampleWeight());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode rewriteMarkDistinct(MarkDistinctNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            return planRewriter.defaultRewrite(node, limit);
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            return planRewriter.defaultRewrite(node, limit);
        }

        @Override
        public PlanNode rewriteTopN(TopNNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), null);
            if (rewrittenSource == node.getSource() && limit == null) {
                return node;
            }

            long count = node.getCount();
            Optional<Symbol> sampleWeight = node.getSampleWeight();
            if (limit != null) {
                count = Math.min(count, limit.getCount());
                if (limit.getSampleWeight().isPresent()) {
                    checkState(!sampleWeight.isPresent() || sampleWeight.equals(limit.getSampleWeight()), "limit and topN sample weight symbols don't match");
                    sampleWeight = limit.getSampleWeight();
                }
            }
            return new TopNNode(node.getId(), rewrittenSource, count, node.getOrderBy(), node.getOrderings(), node.isPartial(), sampleWeight);
        }

        @Override
        public PlanNode rewriteSort(SortNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), null);
            if (limit != null) {
                return new TopNNode(node.getId(), rewrittenSource, limit.getCount(), node.getOrderBy(), node.getOrderings(), false, limit.getSampleWeight());
            }
            else if (rewrittenSource != node.getSource()) {
                return new SortNode(node.getId(), rewrittenSource, node.getOrderBy(), node.getOrderings());
            }
            return node;
        }

        @Override
        public PlanNode rewriteUnion(UnionNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            PlanNode output = planRewriter.defaultRewrite(node, limit);
            if (limit != null) {
                output = new LimitNode(idAllocator.getNextId(), output, limit.getCount(), limit.getSampleWeight());
            }
            return output;
        }

        @Override
        public PlanNode rewriteSemiJoin(SemiJoinNode node, LimitNode limit, PlanRewriter<LimitNode> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), limit);
            if (source != node.getSource()) {
                return new SemiJoinNode(node.getId(), source, node.getFilteringSource(), node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput());
            }
            return node;
        }
    }
}
