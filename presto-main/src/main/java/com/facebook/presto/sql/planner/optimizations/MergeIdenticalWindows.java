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

/**
 * Merge together the functions in WindowNodes that have identical WindowNode.Specifications.
 * For example:
 *
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: A, Functions: [sum(something)])
 *       `--WindowNode(Specification: B, Functions: [sum(something)])
 *          `--WindowNode(Specification: A, Functions: [avg(something)])
 *             `--...
 *
 * Will be transformed into
 *
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: A, Functions: [sum(something), avg(something)])
 *       `--WindowNode(Specification: B, Functions: [sum(something)])
 *          `--...
 *
 * This will NOT merge the functions in WindowNodes that have identical WindowNode.Specifications,
 * but have a node between them that is not a WindowNode.
 * In the following example, the functions in the WindowNodes with specification `A' will not be
 * merged into a single WindowNode.
 *
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: A, Functions: [sum(something)])
 *       `--WindowNode(Specification: B, Functions: [sum(something)])
 *          `-- ProjectNode(...)
 *             `--WindowNode(Specification: A, Functions: [avg(something)])
 *                `--...
 */
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

    private static class Rewriter
            extends SimplePlanRewriter<MergeIdenticalWindows.Context>
    {
        @Override
        public PlanNode visitWindow(
                WindowNode node,
                RewriteContext<MergeIdenticalWindows.Context> context)
        {
            checkState(!node.getHashSymbol().isPresent(),
                    "MergeIdenticalWindows should be run before HashGenerationOptimizer");
            checkState(node.getPrePartitionedInputs().isEmpty() && node.getPreSortedOrderPrefix() == 0,
                    "MergeIdenticalWindows should be run before AddExchanges");

            Context mergeContext = context.get();
            boolean retainNode = mergeContext.collectFunctions(node);

            // Don't merge window functions across nodes that aren't WindowNodes
            if (node.getSource() instanceof WindowNode) {
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
                    collectedFunctions.getWindowFunctions().build(),
                    collectedFunctions.getSignatures().build(),
                    node.getHashSymbol(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix());
        }
    }

    private static final class Context
    {
        private Map<WindowNode.Specification, FunctionPair> pairMap = new HashMap<>();

        private boolean collectFunctions(WindowNode window)
        {
            WindowNode.Specification key = window.getSpecification();
            FunctionPair pair = pairMap.get(key);

            if (pair == null) {
                pair = new FunctionPair();
            }

            pair.getWindowFunctions().putAll(window.getWindowFunctions());
            pair.getSignatures().putAll(window.getSignatures());
            return pairMap.put(key, pair) == null;
        }

        private FunctionPair getCollectedFunctions(WindowNode window)
        {
            WindowNode.Specification key = window.getSpecification();
            checkState(
                    pairMap.containsKey(key),
                    "No pair for window specification. Tried to merge the same window twice?");
            return pairMap.get(key);
        }

        private static class FunctionPair
        {
            private final ImmutableMap.Builder<Symbol, FunctionCall> windowFunctions = new ImmutableMap.Builder<>();
            private final ImmutableMap.Builder<Symbol, Signature> signatures = new ImmutableMap.Builder<>();

            public ImmutableMap.Builder<Symbol, FunctionCall> getWindowFunctions()
            {
                return windowFunctions;
            }

            public ImmutableMap.Builder<Symbol, Signature> getSignatures()
            {
                return signatures;
            }
        }
    }
}
