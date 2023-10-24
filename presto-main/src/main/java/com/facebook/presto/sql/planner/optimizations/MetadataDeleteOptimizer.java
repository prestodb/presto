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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Converts delete followed immediately by table scan to a special metadata-only delete node
 * <p>
 * Turn
 * <pre>
 *     TableCommit - Delete - TableScanNode (no node allowed in between except Exchanges)
 * </pre>
 * into
 * <pre>
 *     MetadataDelete
 * </pre>
 */
public class MetadataDeleteOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    public MetadataDeleteOptimizer(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        Optimizer optimizer = new Optimizer(session, metadata, idAllocator);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(optimizer, plan, null);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, optimizer.isPlanChanged());
    }

    private static class Optimizer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final Metadata metadata;
        private boolean planChanged;

        private Optimizer(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.metadata = metadata;
            this.idAllocator = idAllocator;
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Void> context)
        {
            Optional<DeleteNode> delete = findNode(node.getSource(), DeleteNode.class);
            if (!delete.isPresent()) {
                return context.defaultRewrite(node);
            }
            Optional<TableScanNode> tableScan = findNode(delete.get().getSource(), TableScanNode.class);
            if (!tableScan.isPresent()) {
                return context.defaultRewrite(node);
            }
            TableScanNode tableScanNode = tableScan.get();
            if (!metadata.supportsMetadataDelete(session, tableScanNode.getTable())) {
                return context.defaultRewrite(node);
            }

            planChanged = true;
            return new MetadataDeleteNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    tableScanNode.getTable(),
                    Iterables.getOnlyElement(node.getOutputVariables()));
        }

        private static <T> Optional<T> findNode(PlanNode source, Class<T> clazz)
        {
            while (true) {
                // allow any chain of linear exchanges
                if (source instanceof ExchangeNode) {
                    List<PlanNode> sources = source.getSources();
                    if (sources.size() != 1) {
                        return Optional.empty();
                    }
                    source = sources.get(0);
                }
                else if (clazz.isInstance(source)) {
                    return Optional.of(clazz.cast(source));
                }
                else {
                    return Optional.empty();
                }
            }
        }
    }
}
