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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
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

    private static class LimitContext
    {
        private final long count;
        private final Optional<Symbol> sampleWeight;

        public LimitContext(long count, Optional<Symbol> sampleWeight)
        {
            this.count = count;
            this.sampleWeight = checkNotNull(sampleWeight, "sampleWeight is null");
        }

        public long getCount()
        {
            return count;
        }

        public Optional<Symbol> getSampleWeight()
        {
            return sampleWeight;
        }
    }

    private static class Rewriter
            extends PlanNodeRewriter<LimitContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode rewriteNode(PlanNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            PlanNode rewrittenNode = planRewriter.defaultRewrite(node, null);
            if (context != null) {
                // Drop in a LimitNode b/c we cannot push our limit down any further
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, context.getCount(), context.getSampleWeight());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode rewriteLimit(LimitNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            if (context != null && context.getCount() < node.getCount()) {
                if (context.getSampleWeight().isPresent()) {
                    checkState(node.getSampleWeight().isPresent() && node.getSampleWeight().equals(context.getSampleWeight()), "sample weight symbols don't match");
                }
                return planRewriter.rewrite(node.getSource(), context);
            }
            else {
                return planRewriter.rewrite(node.getSource(), new LimitContext(node.getCount(), node.getSampleWeight()));
            }
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            if (context != null &&
                    node.getAggregations().isEmpty() &&
                    node.getOutputSymbols().size() == node.getGroupBy().size() &&
                    node.getOutputSymbols().containsAll(node.getGroupBy())) {
                checkArgument(!context.getSampleWeight().isPresent(), "sample weight symbol referenced after a DISTINCT node");
                checkArgument(!node.getSampleWeight().isPresent(), "DISTINCT aggregation has sample weight symbol");
                PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), null);
                return new DistinctLimitNode(idAllocator.getNextId(), rewrittenSource, context.getCount());
            }
            PlanNode rewrittenNode = planRewriter.defaultRewrite(node, null);
            if (context != null) {
                checkState(!context.getSampleWeight().isPresent(), "Aggregations do not output a sample weight, but limit consumes one");
                // Drop in a LimitNode b/c limits cannot be pushed through aggregations
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, context.getCount(), Optional.<Symbol>absent());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode rewriteMarkDistinct(MarkDistinctNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            return planRewriter.defaultRewrite(node, context);
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            Symbol sampleWeightSymbol = context != null ? context.getSampleWeight().orNull() : null;
            if (sampleWeightSymbol == null) {
                return planRewriter.defaultRewrite(node, context);
            }

            Expression expression = node.getOutputMap().get(sampleWeightSymbol);
            if (expression instanceof QualifiedNameReference) {
                Symbol unaliasedSampleWeightSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                context = new LimitContext(context.getCount(), Optional.of(unaliasedSampleWeightSymbol));
                PlanNode source = planRewriter.rewrite(node.getSource(), context);
                return new ProjectNode(node.getId(), source, node.getOutputMap());
            }
            else {
                // TODO: We might want to add another limit here that ignores sample weight, and push it down. We would have to assume that sample weight is never zero.
                PlanNode rewrittenNode = planRewriter.defaultRewrite(node, null);
                // Drop in a LimitNode b/c we cannot push our limit down any further
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, context.getCount(), context.getSampleWeight());
                return rewrittenNode;
            }
        }

        @Override
        public PlanNode rewriteTopN(TopNNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), null);
            if (rewrittenSource == node.getSource() && context == null) {
                return node;
            }

            long count = node.getCount();
            Optional<Symbol> sampleWeight = node.getSampleWeight();
            if (context != null) {
                count = Math.min(count, context.getCount());
                if (context.getSampleWeight().isPresent()) {
                    checkState(sampleWeight.isPresent() && sampleWeight.equals(context.getSampleWeight()), "limit and topN sample weight symbols don't match");
                }
            }
            return new TopNNode(node.getId(), rewrittenSource, count, node.getOrderBy(), node.getOrderings(), node.isPartial(), sampleWeight);
        }

        @Override
        public PlanNode rewriteSort(SortNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), null);
            if (context != null) {
                return new TopNNode(node.getId(), rewrittenSource, context.getCount(), node.getOrderBy(), node.getOrderings(), false, context.getSampleWeight());
            }
            else if (rewrittenSource != node.getSource()) {
                return new SortNode(node.getId(), rewrittenSource, node.getOrderBy(), node.getOrderings());
            }
            return node;
        }

        @Override
        public PlanNode rewriteUnion(UnionNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            List<Symbol> sampleWeights = null;
            if (context != null && context.getSampleWeight().isPresent()) {
                sampleWeights = node.getSymbolMapping().get(context.getSampleWeight().get());
            }

            List<PlanNode> sources = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                LimitContext rewrittenContext = context;
                if (sampleWeights != null) {
                    rewrittenContext = new LimitContext(context.getCount(), Optional.of(sampleWeights.get(i)));
                }
                sources.add(planRewriter.rewrite(node.getSources().get(i), rewrittenContext));
            }

            PlanNode output = new UnionNode(node.getId(), sources, node.getSymbolMapping());
            if (context != null) {
                output = new LimitNode(idAllocator.getNextId(), output, context.getCount(), context.getSampleWeight());
            }
            return output;
        }

        @Override
        public PlanNode rewriteSemiJoin(SemiJoinNode node, LimitContext context, PlanRewriter<LimitContext> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);
            if (source != node.getSource()) {
                return new SemiJoinNode(node.getId(), source, node.getFilteringSource(), node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput());
            }
            return node;
        }
    }
}
