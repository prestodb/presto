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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.WindowNode;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkState;

/**
 * Reorder windows such as those with the same <i>partition by</i> clause are adjacent.
 * For example:
 * <pre>
 * OutputNode
 * --...
 *    --WindowNode(PartitionBy A OrderBy B)
 *       --WindowNode(PartitionBy C OrderBy D)
 *          --WindowNode(PartitionBy A OrderBy E)
 *             --...
 * </pre>
 *
 * Will be transformed into:
 * <pre>
 * OutputNode
 * --...
 *    --WindowNode(PartitionBy C OrderBy D)
 *       --WindowNode(PartitionBy A OrderBy B)
 *          --WindowNode(PartitionBy A OrderBy E)
 *             --...
 * </pre>
 *
 * What is more, if one <i>PartitionBy</i> is a prefix of another, the shorter is preferred, e.g.:
 * <pre>
 * OutputNode
 * --...
 *    --WindowNode(PartitionBy A OrderBy B)
 *       --WindowNode(PartitionBy C OrderBy D)
 *          --WindowNode(PartitionBy A,E OrderBy F)
 *             --...
 * </pre>
 *
 * Will be transformed into:
 * <pre>
 * OutputNode
 * --...
 *    --WindowNode(PartitionBy C OrderBy D)
 *       --WindowNode(PartitionBy A,E OrderBy F)
 *          --WindowNode(PartitionBy A OrderBy B)
 *             --...
 * </pre>
 *
 * This will NOT reorder WindowNodes across non-Window nodes.
 * In the following example, the WindowNode with PartitionBy = A
 * won't be carried over beyond ProjectNode to precede WindowNode with PartitionBy = A,B:
 * <pre>
 * OutputNode
 * --...
 *    --WindowNode(PartitionBy A OrderBy F)
 *       --ProjectNode(...)
 *          --WindowNode(PartitionBy A,B OrderBy C)
 *             --...
 * </pre>
 *
 * The optimizer may reorder windows even if no window partitioning is a prefix of the other.
 * E.g.:
 * <pre>
 * OutputNode
 * --...
 *    --WindowNode(PartitionBy A OrderBy B)
 *       --WindowNode(PartitionBy C OrderBy D)
 *          --...
 * </pre>
 *
 * May be transformed into:
 * <pre>
 * OutputNode
 * --...
 *    --WindowNode(PartitionBy C OrderBy D)
 *       --WindowNode(PartitionBy A OrderBy B)
 *          --...
 * </pre>
 *
 * Even though there are no identical <i>partition by</i> clauses in such a plan.
 */
public class ReorderWindows
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!SystemSessionProperties.isReorderWindowsEnabled(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, new PriorityQueue<>(new PartitionByComparator()));
    }

    private static class Rewriter
            extends SimplePlanRewriter<PriorityQueue<WindowNode>>
    {
        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<PriorityQueue<WindowNode>> context)
        {
            PriorityQueue<WindowNode> windowNodes = context.get();

            PlanNode sourceNode = context.defaultRewrite(node, new PriorityQueue<>(new PartitionByComparator()));
            while (windowNodes.peek() != null) {
                WindowNode windowNode = windowNodes.poll();
                sourceNode = new WindowNode(
                        windowNode.getId(),
                        sourceNode,
                        windowNode.getSpecification(),
                        windowNode.getWindowFunctions(),
                        windowNode.getHashSymbol(),
                        windowNode.getPrePartitionedInputs(),
                        windowNode.getPreSortedOrderPrefix());
            }
            return sourceNode;
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<PriorityQueue<WindowNode>> context)
        {
            checkState(!node.getHashSymbol().isPresent(), "ReorderWindows should be run before HashGenerationOptimizer");
            checkState(node.getPrePartitionedInputs().isEmpty() && node.getPreSortedOrderPrefix() == 0, "ReorderWindows should be run before AddExchanges");

            context.get().add(node);
            return context.rewrite(node.getSource(), context.get());
        }
    }

    private static class PartitionByComparator
            implements Comparator<WindowNode>
    {
        @Override
        public int compare(WindowNode o1, WindowNode o2)
        {
            Iterator<Symbol> iterator1 = o1.getPartitionBy().iterator();
            Iterator<Symbol> iterator2 = o2.getPartitionBy().iterator();

            while (iterator1.hasNext() && iterator2.hasNext()) {
                Symbol symbol1 = iterator1.next();
                Symbol symbol2 = iterator2.next();

                int comparison = symbol1.compareTo(symbol2);
                if (comparison != 0) {
                    return comparison;
                }
            }

            if (iterator1.hasNext()) {
                return 1;
            }

            if (iterator2.hasNext()) {
                return -1;
            }

            return 0;
        }
    }
}
