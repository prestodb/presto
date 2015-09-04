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

import com.facebook.presto.util.ImmutableCollectors;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class PlanRewriter<C>
        extends PlanVisitor<PlanRewriter.RewriteContext<C>, PlanNode>
{
    public static <C> PlanNode rewriteWith(PlanRewriter<C> rewriter, PlanNode node)
    {
        return node.accept(rewriter, new RewriteContext<>(rewriter, null));
    }

    public static <C> PlanNode rewriteWith(PlanRewriter<C> rewriter, PlanNode node, C context)
    {
        return node.accept(rewriter, new RewriteContext<>(rewriter, context));
    }

    @Override
    protected PlanNode visitPlan(PlanNode node, RewriteContext<C> context)
    {
        return context.defaultRewrite(node, context.get());
    }

    public static class RewriteContext<C>
    {
        private final C userContext;
        private final PlanRewriter<C> nodeRewriter;

        private RewriteContext(PlanRewriter<C> nodeRewriter, C userContext)
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
                    .collect(ImmutableCollectors.toImmutableList());

            return replaceChildren(node, children);
        }

        /**
         * Return an identical copy of the given node with its children replaced
         */
        public PlanNode replaceChildren(PlanNode node, List<PlanNode> newChildren)
        {
            for (int i = 0; i < node.getSources().size(); i++) {
                if (newChildren.get(i) != node.getSources().get(i)) {
                    return ChildReplacer.replaceChildren(node, newChildren);
                }
            }

            // children haven't change, so make this a no-op
            return node;
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node
         */
        public PlanNode rewrite(PlanNode node, C userContext)
        {
            PlanNode result = node.accept(nodeRewriter, new RewriteContext<>(nodeRewriter, userContext));
            requireNonNull(result, format("nodeRewriter returned null for %s", node.getClass().getName()));

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
