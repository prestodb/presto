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
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;

/**
 * Merge together the functions in WindowNodes that have identical WindowNode.Specifications.
 * For example:
 * <p>
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: A, Functions: [sum(something)])
 *       `--WindowNode(Specification: B, Functions: [sum(something)])
 *          `--WindowNode(Specification: A, Functions: [avg(something)])
 *             `--...
 *
 * Will be transformed into
 * <p>
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: B, Functions: [sum(something)])
 *       `--WindowNode(Specification: A, Functions: [avg(something), sum(something)])
 *          `--...
 *
 * This will NOT merge the functions in WindowNodes that have identical WindowNode.Specifications,
 * but have a node between them that is not a WindowNode.
 * In the following example, the functions in the WindowNodes with specification `A' will not be
 * merged into a single WindowNode.
 * <p>
 * OutputNode
 * `--...
 *    `--WindowNode(Specification: A, Functions: [sum(something)])
 *       `--WindowNode(Specification: B, Functions: [sum(something)])
 *          `-- ProjectNode(...)
 *             `--WindowNode(Specification: A, Functions: [avg(something)])
 *                `--...
 */
public class MergeWindows
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan,
            Session session,
            Map<Symbol, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        // ImmutableListMultimap preserves order of window nodes
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, ImmutableListMultimap.of());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Multimap<WindowNode.Specification, WindowNode>>
    {
        @Override
        protected PlanNode visitPlan(
                PlanNode node,
                RewriteContext<Multimap<WindowNode.Specification, WindowNode>> context)
        {
            PlanNode newNode = context.defaultRewrite(node, ImmutableListMultimap.of());
            return collapseWindowsWithinSpecification(context.get(), newNode);
        }

        @Override
        public PlanNode visitWindow(
                WindowNode windowNode,
                RewriteContext<Multimap<WindowNode.Specification, WindowNode>> context)
        {
            checkState(!windowNode.getHashSymbol().isPresent(), "MergeWindows should be run before HashGenerationOptimizer");
            checkState(windowNode.getPrePartitionedInputs().isEmpty() && windowNode.getPreSortedOrderPrefix() == 0, "MergeWindows should be run before AddExchanges");
            checkState(windowNode.getWindowFunctions().values().stream().distinct().count() == 1, "Frames expected to be identical");

            for (WindowNode.Specification specification : context.get().keySet()) {
                Collection<WindowNode> nodes = context.get().get(specification);
                if (nodes.stream().anyMatch(node -> dependsOn(node, windowNode))) {
                    return collapseWindowsWithinSpecification(context.get(),
                            context.rewrite(
                                    windowNode.getSource(),
                                    ImmutableListMultimap.of(windowNode.getSpecification(), windowNode)));
                }
            }

            return context.rewrite(
                    windowNode.getSource(),
                    ImmutableListMultimap.<WindowNode.Specification, WindowNode>builder()
                            .put(windowNode.getSpecification(), windowNode) // Add the current window first so that it gets precedence in iteration order
                            .putAll(context.get())
                            .build());
        }

        private static PlanNode collapseWindowsWithinSpecification(Multimap<WindowNode.Specification, WindowNode> windowsMap, PlanNode sourceNode)
        {
            for (WindowNode.Specification specification : windowsMap.keySet()) {
                Collection<WindowNode> windows = windowsMap.get(specification);
                sourceNode = collapseWindows(sourceNode, specification, windows);
            }
            return sourceNode;
        }

        private static WindowNode collapseWindows(PlanNode source, WindowNode.Specification specification, Collection<WindowNode> windows)
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
                    canonical.getHashSymbol(),
                    canonical.getPrePartitionedInputs(),
                    canonical.getPreSortedOrderPrefix());
        }

        private static boolean dependsOn(WindowNode parent, WindowNode child)
        {
            Set<Symbol> childOutputs = child.getCreatedSymbols();

            Stream<Symbol> arguments = parent.getWindowFunctions().values().stream()
                    .map(WindowNode.Function::getFunctionCall)
                    .flatMap(functionCall -> functionCall.getArguments().stream())
                    .map(DependencyExtractor::extractUnique)
                    .flatMap(Collection::stream);

            return parent.getPartitionBy().stream().anyMatch(childOutputs::contains)
                    || parent.getOrderBy().stream().anyMatch(childOutputs::contains)
                    || arguments.anyMatch(childOutputs::contains);
        }
    }
}
