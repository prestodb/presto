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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.plan.PlanNode;

import java.util.List;

import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class SimplePlanRewriter<C>
        extends InternalPlanVisitor<PlanNode, SimplePlanRewriter.RewriteContext<C>>
{
    public static <C> PlanNode rewriteWith(SimplePlanRewriter<C> rewriter, PlanNode node)
    {
        return rewriteWith(rewriter, node, null);
    }

    public static <C> PlanNode rewriteWith(SimplePlanRewriter<C> rewriter, PlanNode node, C context)
    {
        // If we rewrite a plan node, topmost node should remain statistically equivalent.
        PlanNode result = node.accept(rewriter, new RewriteContext<>(rewriter, context));
        if (node.getStatsEquivalentPlanNode().isPresent() && !result.getStatsEquivalentPlanNode().isPresent()) {
            result = result.assignStatsEquivalentPlanNode(node.getStatsEquivalentPlanNode());
        }
        return result;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, RewriteContext<C> context)
    {
        return context.defaultRewrite(node, context.get());
    }

    public static class RewriteContext<C>
    {
        private final C userContext;
        private final SimplePlanRewriter<C> nodeRewriter;

        private RewriteContext(SimplePlanRewriter<C> nodeRewriter, C userContext)
        {
            this.nodeRewriter = nodeRewriter;
            this.userContext = userContext;
        }

        public C get()
        {
            return userContext;
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children
         */
        public PlanNode defaultRewrite(PlanNode node)
        {
            return defaultRewrite(node, null);
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children
         */
        public PlanNode defaultRewrite(PlanNode node, C context)
        {
            List<PlanNode> children = node.getSources().stream()
                    .map(child -> rewrite(child, context))
                    .collect(toImmutableList());

            return replaceChildren(node, children);
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node
         */
        public PlanNode rewrite(PlanNode node, C userContext)
        {
            // If we rewrite a plan node, topmost node should remain statistically equivalent.
            PlanNode result = node.accept(nodeRewriter, new RewriteContext<>(nodeRewriter, userContext));
            if (node.getStatsEquivalentPlanNode().isPresent() && !result.getStatsEquivalentPlanNode().isPresent()) {
                result = result.assignStatsEquivalentPlanNode(node.getStatsEquivalentPlanNode());
            }
            verify(result != null, "nodeRewriter returned null for %s", node.getClass().getName());

            return result;
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node
         */
        public PlanNode rewrite(PlanNode node)
        {
            return rewrite(node, null);
        }
    }
}
