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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.TABLE_WRITE_ORDERING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.mergingExchange;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;

public final class WriteOrderingOptimizer
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types,
                             PlanVariableAllocator variableAllocator,
                             PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!session.getSystemProperty(TABLE_WRITE_ORDERING, Boolean.class)) {
            return plan;
        }
        return rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext context)
        {
            PlanNode current = node;
            // recursively searching down the tree to skip the exchange nodes.
            while (true) {
                List<PlanNode> sources = current.getSources();
                if (sources.size() == 1 && sources.get(0) instanceof ExchangeNode) {
                    current = sources.get(0);
                }
                else {
                    break;
                }
            }
            // found the first non-exchange node
            List<PlanNode> sources = current.getSources();
            PlanNode newNode = node;
            // Try to recreate a node if and only if previous node is a sort node
            if (sources.size() == 1) {
                PlanNode previous = sources.get(0);
                if (previous instanceof SortNode) {
                    SortNode previousSortNode = (SortNode) previous;
                    if (!previousSortNode.isPartial()) {
                        // if previous is not partial sort we can directly link to the previous node
                        current = previous;
                    }
                    else {
                        // if previous is partial sort, we need to add a RemoteStreamingMerge
                        current = mergingExchange(
                                idAllocator.getNextId(),
                                REMOTE_STREAMING,
                                current,
                                previousSortNode.getOrderingScheme());
                    }
                    // Now that 'current' is the true source of the LocalExchange, we can try connect 'node' to 'current'
                    newNode = new TableWriterNode(
                            node.getSourceLocation(),
                            node.getId(),
                            current,
                            node.getTarget(),
                            node.getRowCountVariable(),
                            node.getFragmentVariable(),
                            node.getTableCommitContextVariable(),
                            node.getColumns(),
                            node.getColumnNames(),
                            node.getNotNullColumnVariables(),
                            node.getTablePartitioningScheme(),
                            node.getPreferredShufflePartitioningScheme(),
                            node.getStatisticsAggregation());
                }
            }
            return newNode;
        }
    }
}
