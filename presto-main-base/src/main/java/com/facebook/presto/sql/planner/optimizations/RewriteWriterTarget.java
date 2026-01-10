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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode.CallDistributedProcedureTarget;
import com.facebook.presto.spi.plan.TableWriterNode.WriterTarget;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter.RewriteContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.toSet;

public class RewriteWriterTarget
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    public RewriteWriterTarget(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = metadata;
        this.accessControl = accessControl;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        Rewriter rewriter = new Rewriter(session, metadata, accessControl);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, Optional.empty());
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private class Rewriter
            extends SimplePlanRewriter<Optional<WriterTarget>>
    {
        private final Session session;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private boolean planChanged;

        public Rewriter(Session session, Metadata metadata, AccessControl accessControl)
        {
            this.session = session;
            this.metadata = metadata;
            this.accessControl = accessControl;
        }

        @Override
        public PlanNode visitCallDistributedProcedure(CallDistributedProcedureNode node, RewriteContext<Optional<WriterTarget>> context)
        {
            CallDistributedProcedureTarget callDistributedProcedureTarget = (CallDistributedProcedureTarget) getContextTarget(context);
            return new CallDistributedProcedureNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getSource(),
                    Optional.of(callDistributedProcedureTarget),
                    node.getRowCountVariable(),
                    node.getFragmentVariable(),
                    node.getTableCommitContextVariable(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getNotNullColumnVariables(),
                    node.getPartitioningScheme());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Optional<WriterTarget>> context)
        {
            PlanNode child = node.getSource();

            Optional<WriterTarget> newTarget = getWriterTarget(child);
            if (!newTarget.isPresent()) {
                return node;
            }

            planChanged = true;
            child = context.rewrite(child, newTarget);

            return new TableFinishNode(
                    node.getSourceLocation(),
                    node.getId(),
                    child,
                    newTarget,
                    node.getRowCountVariable(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor(),
                    Optional.empty());
        }

        public Optional<WriterTarget> getWriterTarget(PlanNode node)
        {
            if (node instanceof CallDistributedProcedureNode) {
                Optional<TableHandle> tableHandle = findTableHandleForCallDistributedProcedure(((CallDistributedProcedureNode) node).getSource());
                Optional<CallDistributedProcedureTarget> callDistributedProcedureTarget = ((CallDistributedProcedureNode) node).getTarget();
                return !tableHandle.isPresent() ?
                        callDistributedProcedureTarget.map(target -> new CallDistributedProcedureTarget(
                                target.getProcedureName(),
                                target.getProcedureArguments(),
                                target.getSourceHandle(),
                                target.getSchemaTableName(),
                                true)) :
                        callDistributedProcedureTarget.map(target -> new CallDistributedProcedureTarget(
                                target.getProcedureName(),
                                target.getProcedureArguments(),
                                tableHandle,
                                target.getSchemaTableName(),
                                false));
            }

            if (node instanceof ExchangeNode || node instanceof UnionNode) {
                Set<Optional<WriterTarget>> writerTargets = node.getSources().stream()
                        .map(this::getWriterTarget)
                        .collect(toSet());
                return getOnlyElement(writerTargets);
            }

            return Optional.empty();
        }

        private Optional<TableHandle> findTableHandleForCallDistributedProcedure(PlanNode startNode)
        {
            List<PlanNode> tableScanNodes = PlanNodeSearcher.searchFrom(startNode)
                    .where(node -> node instanceof TableScanNode)
                    .findAll();

            if (tableScanNodes.size() == 1) {
                TableHandle tableHandle = ((TableScanNode) tableScanNodes.get(0)).getTable();
                checkFullDataAccessControl(tableHandle);
                return Optional.of(tableHandle);
            }

            List<ValuesNode> valuesNodes = PlanNodeSearcher.searchFrom(startNode)
                    .where(node -> node instanceof ValuesNode)
                    .findAll();

            if (valuesNodes.size() == 1) {
                return Optional.empty();
            }

            throw new IllegalArgumentException("Expected to find exactly one update target TableScanNode in plan but found: " + tableScanNodes);
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        private void checkFullDataAccessControl(TableHandle tableHandle)
        {
            TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            QualifiedObjectName baseTable = new QualifiedObjectName(tableMetadata.getConnectorId().getCatalogName(),
                    tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName());
            String errorMessage = "Full data access is restricted by row filters and column masks for table: " + baseTable;

            // Check for row filters on this target table
            List<ViewExpression> rowFilters = accessControl.getRowFilters(
                    session.getRequiredTransactionId(),
                    session.getIdentity(),
                    session.getAccessControlContext(),
                    baseTable);

            if (!rowFilters.isEmpty()) {
                throw new AccessDeniedException(errorMessage);
            }

            // Check for column masks on this target table
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            List<ColumnMetadata> columnsMetadata = columnHandles.values().stream()
                    .map(handle -> metadata.getColumnMetadata(session, tableHandle, handle))
                    .collect(toImmutableList());

            Map<ColumnMetadata, ViewExpression> columnMasks = accessControl.getColumnMasks(
                    session.getRequiredTransactionId(),
                    session.getIdentity(),
                    session.getAccessControlContext(),
                    baseTable,
                    columnsMetadata);

            if (!columnMasks.isEmpty()) {
                throw new AccessDeniedException(errorMessage);
            }
        }
    }

    private static WriterTarget getContextTarget(RewriteContext<Optional<WriterTarget>> context)
    {
        return context.get().orElseThrow(() -> new IllegalStateException("WriterTarget not present"));
    }
}
