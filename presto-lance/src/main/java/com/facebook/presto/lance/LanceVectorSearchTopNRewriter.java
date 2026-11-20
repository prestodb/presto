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
package com.facebook.presto.lance;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;

/**
 * Completes the {@code system.vector_search} table function plan: the engine rewrites the
 * invocation into a bare TableScanNode (TransformTableFunctionProcessorToTableScan) with no
 * TopN above it, so per-split local top-k results would otherwise be concatenated, not merged.
 * This LOGICAL-phase optimizer inserts {@code TopN(SINGLE, k, _distance ASC)} directly above
 * any Lance scan whose {@link LanceVectorSearch} still has {@code requiresGlobalTopK} set, and
 * clears the flag on the scan's handle. AddExchanges then plans a gathering exchange under the
 * SINGLE TopN, giving the exact global top-k merge. Currently all vector search scans run as
 * a single split (lance-core (verified through 11.0.0) does not support fragment-scoped nearest), so the TopN is
 * the engine-side merge point kept for when per-segment fan-out returns.
 *
 * <p>If this optimizer does not run (connector optimization disabled), the flag stays set and
 * {@link LanceSplitManager} falls back to one split over all fragments, which is still exact.
 */
public class LanceVectorSearchTopNRewriter
        implements ConnectorPlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator), maxSubplan);
    }

    private static final class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            TableHandle handle = node.getTable();
            if (!(handle.getConnectorHandle() instanceof LanceTableHandle)) {
                return node;
            }
            LanceTableHandle lanceHandle = (LanceTableHandle) handle.getConnectorHandle();
            if (!lanceHandle.hasVectorSearch() || !lanceHandle.getVectorSearch().get().isRequiresGlobalTopK()) {
                return node;
            }

            VariableReferenceExpression distanceVariable = null;
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (((LanceColumnHandle) entry.getValue()).isDistance()) {
                    distanceVariable = entry.getKey();
                    break;
                }
            }
            if (distanceVariable == null) {
                // Cannot order without the distance column; leave the flag set so the split
                // manager keeps the exact single-split fallback.
                return node;
            }

            LanceVectorSearch search = lanceHandle.getVectorSearch().get();
            LanceVectorSearch mergedSearch = new LanceVectorSearch(
                    search.getColumn(), search.getQueryVector(), search.getK(), search.getMetric(),
                    search.getNprobes(), search.getEf(), search.getRefineFactor(), false);
            LanceTableHandle newLanceHandle = lanceHandle.withVectorSearch(mergedSearch);

            // Rebuild the engine TableHandle + scan exactly the way LanceVectorSearchPushdown does.
            // CRITICAL: the layout handle embeds its own copy of the LanceTableHandle, and the split
            // manager reads THAT copy -- rebuild it too, or the cleared flag never reaches getSplits
            // and every query silently takes the single-split fallback.
            Optional<ConnectorTableLayoutHandle> newLayout = handle.getLayout()
                    .map(LanceTableLayoutHandle.class::cast)
                    .map(layout -> (ConnectorTableLayoutHandle) new LanceTableLayoutHandle(newLanceHandle, layout.getTupleDomain()));
            TableHandle newHandle = new TableHandle(
                    handle.getConnectorId(), newLanceHandle, handle.getTransaction(), newLayout);
            TableScanNode newScan = new TableScanNode(
                    node.getSourceLocation(), node.getId(), newHandle, node.getOutputVariables(),
                    node.getAssignments(), node.getCurrentConstraint(), node.getEnforcedConstraint(), node.getCteMaterializationInfo());

            return new TopNNode(
                    node.getSourceLocation(), idAllocator.getNextId(), newScan,
                    search.getK(),
                    new OrderingScheme(ImmutableList.of(new Ordering(distanceVariable, SortOrder.ASC_NULLS_LAST))),
                    TopNNode.Step.SINGLE);
        }
    }
}
