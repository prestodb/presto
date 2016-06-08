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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class MergeIdenticalWindows
        implements PlanOptimizer
{
    public PlanNode optimize(PlanNode plan,
            Session session,
            Map<Symbol, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, new Context());
    }

    private static class Context
    {
        private Map<WindowNode.Specification, ImmutableMap.Builder<Symbol, FunctionCall>> functions =
                new HashMap<>();
        private Map<WindowNode.Specification, ImmutableMap.Builder<Symbol, Signature>> signatures =
                new HashMap<>();

        private boolean collectFunctions(WindowNode window)
        {
            WindowNode.Specification key = window.getSpecification();

            // Either both maps must contain the specification, or neither.
            checkState(functions.containsKey(key) == signatures.containsKey(key),
                    "Merge context is in an inconsistent state.");

            ImmutableMap.Builder<Symbol, FunctionCall> functionsBuilder =
                    functions.getOrDefault(key, new ImmutableMap.Builder<>());
            functionsBuilder.putAll(window.getWindowFunctions());
            functions.put(key, functionsBuilder);

            ImmutableMap.Builder<Symbol, Signature> signaturesBuilder =
                    signatures.getOrDefault(key, new ImmutableMap.Builder<>());
            signaturesBuilder.putAll(window.getSignatures());
            return signatures.put(key, signaturesBuilder) == null;
        }

        private class FunctionPair
        {
            final Map<Symbol, FunctionCall> windowFunctions;
            final Map<Symbol, Signature> signatures;

            private FunctionPair(
                    Map<Symbol, FunctionCall> windowFunctions,
                    Map<Symbol, Signature> signatures)
            {
                this.windowFunctions = windowFunctions;
                this.signatures = signatures;
            }
        }

        private FunctionPair getCollectedFunctions(WindowNode window)
        {
            WindowNode.Specification key = window.getSpecification();

            ImmutableMap.Builder<Symbol, FunctionCall> functionBuilder = functions.remove(key);
            ImmutableMap.Builder<Symbol, Signature> signatureBuilder = signatures.remove(key);

            // The only valid state here is that both maps contain the specification.
            checkState(functionBuilder != null && signatureBuilder != null,
                    "Merge context in inconsistent state or trying to merge the same window specification twice");

            return new FunctionPair(functionBuilder.build(), signatureBuilder.build());
        }
    }

    private static class Rewriter
            extends SimplePlanRewriter<MergeIdenticalWindows.Context>
    {
        @Override
        public PlanNode visitWindow(
                WindowNode node,
                RewriteContext<MergeIdenticalWindows.Context> context)
        {
            Context mergeContext = context.get();
            boolean retainNode = mergeContext.collectFunctions(node);

            // Don't merge window functions across nodes that aren't WindowNodes
            if (node.getClass().isInstance(node.getSource())) {
                node = (WindowNode) context.defaultRewrite(node, mergeContext);
            }
            else {
                node = (WindowNode) context.defaultRewrite(node, new Context());
            }

            // WindowFunctions contained in this node will be merged in some node further up the tree.
            if (!retainNode) {
                return node.getSource();
            }

            Context.FunctionPair collectedFunctions = mergeContext.getCollectedFunctions(node);

            return new WindowNode(
                    node.getId(),
                    node.getSource(),
                    node.getSpecification(),
                    collectedFunctions.windowFunctions,
                    collectedFunctions.signatures,
                    node.getHashSymbol(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix());
        }
    }
}
