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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

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
public class MergeIdenticalWindows
        implements PlanOptimizer
{
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
            extends SimplePlanRewriter<Multimap<SpecificationAndFrame, WindowNode>>
    {
        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Multimap<SpecificationAndFrame, WindowNode>> context)
        {
            PlanNode newNode = context.defaultRewrite(node, ImmutableListMultimap.of());
            for (SpecificationAndFrame specification : context.get().keySet()) {
                Collection<WindowNode> windows = context.get().get(specification);
                newNode = collapseWindows(newNode, specification, windows);
            }
            return newNode;
        }

        @Override
        public PlanNode visitWindow(
                WindowNode node,
                RewriteContext<Multimap<SpecificationAndFrame, WindowNode>> context)
        {
            checkState(!node.getHashSymbol().isPresent(), "MergeIdenticalWindows should be run before HashGenerationOptimizer");
            checkState(node.getPrePartitionedInputs().isEmpty() && node.getPreSortedOrderPrefix() == 0, "MergeIdenticalWindows should be run before AddExchanges");
            checkState(node.getWindowFunctions().values().stream().distinct().count() == 1, "Frames expected to be identical");

            SpecificationAndFrame specificationAndFrame = new SpecificationAndFrame(node.getSpecification(), node.getFrames().iterator().next());

            return context.rewrite(
                    node.getSource(),
                    ImmutableListMultimap.<SpecificationAndFrame, WindowNode>builder()
                            .put(specificationAndFrame, node) // Add the current window first so that it gets precedence in iteration order
                            .putAll(context.get())
                            .build());
        }

        private static WindowNode collapseWindows(PlanNode source, SpecificationAndFrame specification, Collection<WindowNode> windows)
        {
            WindowNode canonical = windows.iterator().next();
            return new WindowNode(
                    canonical.getId(),
                    source,
                    specification.getSpecification(),
                    windows.stream()
                            .map(WindowNode::getWindowFunctions)
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                    canonical.getHashSymbol(),
                    canonical.getPrePartitionedInputs(),
                    canonical.getPreSortedOrderPrefix());
        }
    }

    private static final class SpecificationAndFrame
    {
        private final WindowNode.Specification specification;
        private final WindowNode.Frame frame;

        public SpecificationAndFrame(WindowNode.Specification specification, WindowNode.Frame frame)
        {
            this.specification = requireNonNull(specification, "specification is null");
            this.frame = requireNonNull(frame, "frame is null");
        }

        public WindowNode.Specification getSpecification()
        {
            return specification;
        }

        public WindowNode.Frame getFrame()
        {
            return frame;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(specification, frame);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SpecificationAndFrame other = (SpecificationAndFrame) obj;
            return Objects.equals(this.specification, other.specification) &&
                    Objects.equals(this.frame, other.frame);
        }
    }
}
