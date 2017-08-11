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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class PlanRewriter<C, P>
        extends PlanVisitor<PlanRewriter.Result<P>, PlanRewriter.RewriteContext<C, P>>
{
    public static <C, P> Result<P> rewriteWith(PlanRewriter<C, P> rewriter, PlanNode node)
    {
        return node.accept(rewriter, new RewriteContext<>(rewriter, null));
    }

    public static <C, P> Result<P> rewriteWith(PlanRewriter<C, P> rewriter, PlanNode node, C context)
    {
        return node.accept(rewriter, new RewriteContext<>(rewriter, context));
    }

    @Override
    protected Result<P> visitPlan(PlanNode node, RewriteContext<C, P> context)
    {
        return context.defaultRewrite(node, context.get());
    }

    public static class Result<P>
    {
        private final PlanNode planNode;
        private final P payload;

        public Result(PlanNode planNode, @Nullable P payload)
        {
            this.planNode = requireNonNull(planNode, "planNode is null");
            this.payload = payload;
        }

        public PlanNode getPlanNode()
        {
            return planNode;
        }

        public P getPayload()
        {
            return payload;
        }
    }

    public static class RewriteContext<C, P>
    {
        private final C userContext;
        private final PlanRewriter<C, P> nodeRewriter;

        private RewriteContext(PlanRewriter<C, P> nodeRewriter, @Nullable C userContext)
        {
            this.nodeRewriter = requireNonNull(nodeRewriter, "nodeRewriter is null");
            this.userContext = userContext;
        }

        public C get()
        {
            return userContext;
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children. The final payload will
         * be null.
         */
        public Result<P> defaultRewrite(PlanNode node)
        {
            return defaultRewrite(node, null, payloads -> null);
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children. The final payload will
         * be null.
         */
        public Result<P> defaultRewrite(PlanNode node, C context)
        {
            return defaultRewrite(node, context, payloads -> null);
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children. The payloadCombiner is used
         * to produce the final payload given the respective payloads of the children.
         */
        public Result<P> defaultRewrite(PlanNode node, Function<List<P>, P> payloadCombiner)
        {
            return defaultRewrite(node, null, payloadCombiner);
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children. The payloadCombiner is used
         * to produce the final payload given the respective payloads of the children.
         */
        public Result<P> defaultRewrite(PlanNode node, C context, Function<List<P>, P> payloadCombiner)
        {
            List<PlanNode> children = new ArrayList<>(node.getSources().size());
            List<P> payloads = new ArrayList<>(node.getSources().size());
            for (PlanNode source : node.getSources()) {
                Result<P> result = rewrite(source, context);
                children.add(result.getPlanNode());
                payloads.add(result.getPayload());
            }
            return new Result<>(ChildReplacer.replaceChildren(node, children), payloadCombiner.apply(payloads));
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node.
         */
        public Result<P> rewrite(PlanNode node, C userContext)
        {
            Result<P> result = node.accept(nodeRewriter, new RewriteContext<>(nodeRewriter, userContext));
            requireNonNull(result, format("nodeRewriter returned null for %s", node.getClass().getName()));

            return result;
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node.
         */
        public Result<P> rewrite(PlanNode node)
        {
            return rewrite(node, null);
        }
    }
}
