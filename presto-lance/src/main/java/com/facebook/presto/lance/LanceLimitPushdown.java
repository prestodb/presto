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

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;

import java.util.Optional;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;

/**
 * Pushes a LIMIT that sits directly above a Lance table scan into the table handle.
 * The split manager and page source use the pushed limit to scan fewer fragments and
 * stop each fragment scan early. The "directly above the scan" requirement keeps this
 * correct in the presence of a filter: when a WHERE clause is present the plan is
 * LIMIT -&gt; FILTER -&gt; SCAN, so the limit is not pushed.
 */
public class LanceLimitPushdown
        implements ConnectorPlanOptimizer
{
    @Override
    public PlanNode optimize(
            PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(), maxSubplan);
    }

    private static final class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return visitPlan(node, context);
            }

            TableScanNode tableScan = (TableScanNode) node.getSource();
            TableHandle handle = tableScan.getTable();
            if (!(handle.getConnectorHandle() instanceof LanceTableHandle)) {
                return visitPlan(node, context);
            }

            LanceTableHandle lanceTable = (LanceTableHandle) handle.getConnectorHandle();
            long count = node.getCount();
            if (lanceTable.getLimit().isPresent() && lanceTable.getLimit().getAsLong() <= count) {
                // An equal-or-tighter limit is already pushed; leave the plan unchanged
                return node;
            }

            LanceTableHandle newLanceTable = lanceTable.withLimit(count);
            Optional<ConnectorTableLayoutHandle> newLayout = handle.getLayout()
                    .map(LanceTableLayoutHandle.class::cast)
                    .map(layout -> new LanceTableLayoutHandle(newLanceTable, layout.getTupleDomain()));
            TableHandle newHandle = new TableHandle(
                    handle.getConnectorId(),
                    newLanceTable,
                    handle.getTransaction(),
                    newLayout);
            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    newHandle,
                    tableScan.getOutputVariables(),
                    tableScan.getAssignments(),
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());

            // Keep the LimitNode so the engine still enforces exact LIMIT semantics; pushdown is best-effort
            return new LimitNode(node.getSourceLocation(), node.getId(), newTableScan, count, node.getStep());
        }
    }
}
