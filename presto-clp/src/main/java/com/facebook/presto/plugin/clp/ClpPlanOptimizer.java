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
import com.facebook.presto.plugin.clp.split.filter.ClpSplitFilterProvider;
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
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ClpPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(ClpPlanOptimizer.class);
    private final FunctionMetadataManager functionManager;
    private final StandardFunctionResolution functionResolution;
    private final ClpSplitFilterProvider splitFilterProvider;

    public ClpPlanOptimizer(FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution, ClpSplitFilterProvider splitFilterProvider)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.splitFilterProvider = requireNonNull(splitFilterProvider, "splitFilterProvider is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        Rewriter rewriter = new Rewriter(idAllocator);
        PlanNode optimizedPlanNode = rewriteWith(rewriter, maxSubplan);

        // Throw exception if any required split filters are missing
        if (!rewriter.tableScopeSet.isEmpty() && !rewriter.hasVisitedFilter) {
            splitFilterProvider.checkContainsRequiredFilters(rewriter.tableScopeSet, "");
        }
        return optimizedPlanNode;
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Set<String> tableScopeSet;
        private boolean hasVisitedFilter;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
            hasVisitedFilter = false;
            tableScopeSet = new HashSet<>();
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            TableHandle tableHandle = node.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();
            tableScopeSet.add(format("%s.%s", CONNECTOR_NAME, clpTableHandle.getSchemaTableName()));
            return super.visitTableScan(node, context);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }
            hasVisitedFilter = true;

            TableScanNode tableScanNode = (TableScanNode) node.getSource();
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>(tableScanNode.getAssignments());
            TableHandle tableHandle = tableScanNode.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();
            String tableScope = CONNECTOR_NAME + "." + clpTableHandle.getSchemaTableName().toString();
            ClpExpression clpExpression = node.getPredicate().accept(
                    new ClpFilterToKqlConverter(
                            functionResolution,
                            functionManager,
                            splitFilterProvider.getColumnNames(tableScope)),
                    assignments);
            Optional<String> kqlQuery = clpExpression.getPushDownExpression();
            Optional<String> metadataSqlQuery = clpExpression.getMetadataSqlQuery();
            Optional<RowExpression> remainingPredicate = clpExpression.getRemainingExpression();

            // Perform required metadata filter checks before handling the KQL query (if kqlQuery
            // isn't present, we'll return early, skipping subsequent checks).
            splitFilterProvider.checkContainsRequiredFilters(ImmutableSet.of(tableScope), metadataSqlQuery.orElse(""));
            if (metadataSqlQuery.isPresent()) {
                metadataSqlQuery = Optional.of(splitFilterProvider.remapSplitFilterPushDownExpression(tableScope, metadataSqlQuery.get()));
                log.debug("Metadata SQL query: %s", metadataSqlQuery);
            }

            if (!kqlQuery.isPresent()) {
                return node;
            }
            log.debug("KQL query: %s", kqlQuery);

            ClpTableLayoutHandle clpTableLayoutHandle = new ClpTableLayoutHandle(clpTableHandle, kqlQuery, metadataSqlQuery);
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
