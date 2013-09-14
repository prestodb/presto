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
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.base.Preconditions;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

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

        return PlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, Long.MAX_VALUE);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Long>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = Preconditions.checkNotNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode rewriteNode(PlanNode node, Long limit, PlanRewriter<Long> planRewriter)
        {
            PlanNode rewrittenNode = planRewriter.defaultRewrite(node, Long.MAX_VALUE);
            if (limit != Long.MAX_VALUE) {
                // Drop in a LimitNode b/c we cannot push our limit down any further
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, limit);
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode rewriteLimit(LimitNode node, Long limit, PlanRewriter<Long> planRewriter)
        {
            return planRewriter.rewrite(node.getSource(), Math.min(node.getCount(), limit));
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Long limit, PlanRewriter<Long> planRewriter)
        {
            return planRewriter.defaultRewrite(node, limit);
        }

        @Override
        public PlanNode rewriteTopN(TopNNode node, Long limit, PlanRewriter<Long> planRewriter)
        {
            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), Long.MAX_VALUE);
            if (limit != Long.MAX_VALUE || rewrittenSource != node.getSource()) {
                return new TopNNode(node.getId(), rewrittenSource, Math.min(node.getCount(), limit), node.getOrderBy(), node.getOrderings(), node.isPartial());
            }
            return node;
        }

        @Override
        public PlanNode rewriteSort(SortNode node, Long limit, PlanRewriter<Long> planRewriter)
        {
            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), Long.MAX_VALUE);
            if (limit != Long.MAX_VALUE) {
                return new TopNNode(node.getId(), rewrittenSource, limit, node.getOrderBy(), node.getOrderings(), false);
            }
            else if (rewrittenSource != node.getSource()) {
                return new SortNode(node.getId(), rewrittenSource, node.getOrderBy(), node.getOrderings());
            }
            return node;
        }

        @Override
        public PlanNode rewriteUnion(UnionNode node, Long limit, PlanRewriter<Long> planRewriter)
        {
            PlanNode output = planRewriter.defaultRewrite(node, limit);
            if (limit != Long.MAX_VALUE) {
                output = new LimitNode(idAllocator.getNextId(), output, limit);
            }
            return output;
        }

        @Override
        public PlanNode rewriteSemiJoin(SemiJoinNode node, Long limit, PlanRewriter<Long> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), limit);
            if (source != node.getSource()) {
                return new SemiJoinNode(node.getId(), source, node.getFilteringSource(), node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput());
            }
            return node;
        }
    }
}
