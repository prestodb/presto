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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.plan.WindowNode.Specification;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

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
        // LinkedListMultimap preserves order of window nodes
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, LinkedListMultimap.create());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Multimap<Specification, WindowNode>>
    {
        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Multimap<Specification, WindowNode>> context)
        {
            PlanNode newNode = context.defaultRewrite(node, LinkedListMultimap.create());
            // Use reverse to preserve window specification order
            for (Specification specification : Lists.reverse(new ArrayList<>(context.get().keySet()))) {
                Collection<WindowNode> windows = context.get().get(specification);
                newNode = collapseWindows(newNode, specification, windows);
            }
            return newNode;
        }

        @Override
        public PlanNode visitWindow(
                WindowNode node,
                RewriteContext<Multimap<Specification, WindowNode>> context)
        {
            checkState(!node.getHashSymbol().isPresent(), "MergeIdenticalWindows should be run before HashGenerationOptimizer");
            checkState(node.getPrePartitionedInputs().isEmpty() && node.getPreSortedOrderPrefix() == 0, "MergeIdenticalWindows should be run before AddExchanges");

            context.get().put(node.getSpecification(), node);

            return context.rewrite(node.getSource(), context.get());
        }

        private static WindowNode collapseWindows(PlanNode source, Specification specification, Collection<WindowNode> windows)
        {
            WindowNode canonical = windows.iterator().next();
            return new WindowNode(
                    canonical.getId(),
                    source,
                    specification,
                    windows.stream()
                            .map(WindowNode::getWindowFunctions)
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                    windows.stream()
                            .map(WindowNode::getSignatures)
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                    canonical.getHashSymbol(),
                    canonical.getPrePartitionedInputs(),
                    canonical.getPreSortedOrderPrefix());
        }
    }
}
