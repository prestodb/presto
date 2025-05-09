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
package com.facebook.presto.plugin.clp;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

public class ClpPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(ClpPlanOptimizer.class);
    private final FunctionMetadataManager functionManager;
    private final StandardFunctionResolution functionResolution;

    public ClpPlanOptimizer(FunctionMetadataManager functionManager,
                            StandardFunctionResolution functionResolution)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan,
                             ConnectorSession session,
                             VariableAllocator variableAllocator,
                             PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator), maxSubplan);
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            TableScanNode tableScanNode = (TableScanNode) node.getSource();
            Map<VariableReferenceExpression, ColumnHandle> assignments = tableScanNode.getAssignments();
            TableHandle tableHandle = tableScanNode.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();
            ClpExpression clpExpression = node.getPredicate()
                    .accept(new ClpFilterToKqlConverter(functionResolution, functionManager, assignments),
                            null);
            Optional<String> kqlQuery = clpExpression.getDefinition();
            Optional<RowExpression> remainingPredicate = clpExpression.getRemainingExpression();
            if (!kqlQuery.isPresent()) {
                return node;
            }
            log.debug("KQL query: %s", kqlQuery.get());
            ClpTableLayoutHandle clpTableLayoutHandle = new ClpTableLayoutHandle(clpTableHandle, kqlQuery);
            TableScanNode newTableScanNode = new TableScanNode(
                    tableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    new TableHandle(
                            tableHandle.getConnectorId(),
                            clpTableHandle,
                            tableHandle.getTransaction(),
                            Optional.of(clpTableLayoutHandle)),
                    tableScanNode.getOutputVariables(),
                    tableScanNode.getAssignments(),
                    tableScanNode.getTableConstraints(),
                    tableScanNode.getCurrentConstraint(),
                    tableScanNode.getEnforcedConstraint(),
                    tableScanNode.getCteMaterializationInfo());
            if (!remainingPredicate.isPresent()) {
                return newTableScanNode;
            }

            return new FilterNode(node.getSourceLocation(),
                    idAllocator.getNextId(),
                    newTableScanNode,
                    remainingPredicate.get());
        }
    }
}
