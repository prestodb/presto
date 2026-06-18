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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.connector.thrift.ThriftWarningCode.THRIFT_INDEXSOURCE_CONVERTED_TO_VALUESNODE;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Runs during both logical and physical phases of connector-aided plan optimization.
 * In most cases filter pushdown will occur during logical phase. However, in cases
 * when new filter is added between logical and physical phases, e.g. a filter on a join
 * key from one side of a join is added to the other side, the new filter will get
 * merged with the one already pushed down.
 */
public class ThriftFilterPushdown
        implements ConnectorPlanOptimizer
{
    private final ThriftMetadata metadata;
    protected final RowExpressionService rowExpressionService;

    public ThriftFilterPushdown(ThriftMetadata metadata, RowExpressionService rowExpressionService)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(session, idAllocator, metadata), maxSubplan);
    }

    public static class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorMetadata metadata;

        public Rewriter(ConnectorSession session, PlanNodeIdAllocator idAllocator, ConnectorMetadata metadata)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode project, RewriteContext<Void> context)
        {
            // Thrift connector in Prestissimo must have no other PlanNode be present
            // between IndexJoinNode and IndexSourceNode.
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : project.getAssignments().entrySet()) {
                if (!entry.getKey().equals(entry.getValue())) {
                    return visitPlan(project, context);
                }
            }
            // If all assignments are identical, remove the current ProjectNode.
            return visitPlan(project.getSource(), context);
        }

        @Override
        public PlanNode visitFilter(FilterNode filter, RewriteContext<Void> context)
        {
            if (!(filter.getSource() instanceof IndexSourceNode)) {
                return visitPlan(filter, context);
            }

            IndexSourceNode indexSource = (IndexSourceNode) filter.getSource();
            checkArgument(indexSource.getTableHandle().getConnectorHandle() instanceof ThriftTableHandle, "pushdownFilter is never supported on a non-thrift TableHandle");

            RowExpression expression = filter.getPredicate();
            TableHandle handle = indexSource.getTableHandle();
            BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping
                    = indexSource.getAssignments().entrySet().stream()
                    .collect(toImmutableBiMap(Map.Entry::getKey, entry ->
                            new VariableReferenceExpression(
                                    entry.getKey().getSourceLocation(),
                                    getColumnName(session, metadata, handle.getConnectorHandle(), entry.getValue()),
                                    entry.getKey().getType())));

            // replaceExpression() may further optimize the expression;
            RowExpression optimizedExpression = replaceExpression(expression, symbolToColumnMapping);

            // If the resulting expression is always false, then return an empty ValuesNode.
            if (FALSE_CONSTANT.equals(optimizedExpression)) {
                return createEmptyValuesNode(indexSource);
            }

            // If the resulting expression is always true, then remove the FilterNode altogether.
            if (TRUE_CONSTANT.equals(optimizedExpression)) {
                return indexSource;
            }

            ThriftTableLayoutHandle layoutHandle = (ThriftTableLayoutHandle) handle.getLayoutHandle();
            checkArgument(layoutHandle.getRemainingPredicate() == null || TRUE_CONSTANT.equals(layoutHandle.getRemainingPredicate()),
                    "remaining predicate already exists");
            ThriftTableHandle tableHandle = (ThriftTableHandle) handle.getConnectorHandle();

            return createIndexSourceNode(
                    indexSource,
                    handle,
                    tableHandle,
                    new ThriftTableLayoutHandle(
                            layoutHandle.getSchemaName(),
                            layoutHandle.getSchemaName(),
                            layoutHandle.getColumns(),
                            layoutHandle.getConstraint(),
                            optimizedExpression));
        }

        private static String getColumnName(ConnectorSession session, ConnectorMetadata metadata, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
        {
            return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
        }

        private static IndexSourceNode createIndexSourceNode(
                IndexSourceNode indexSource,
                TableHandle tableHandle,
                ConnectorTableHandle connectorTableHandle,
                ConnectorTableLayoutHandle connectorTableLayoutHandle)
        {
            return new IndexSourceNode(
                    indexSource.getSourceLocation(),
                    indexSource.getId(),
                    indexSource.getIndexHandle(),
                    new TableHandle(tableHandle.getConnectorId(),
                            connectorTableHandle,
                            tableHandle.getTransaction(),
                            Optional.of(connectorTableLayoutHandle)),
                    indexSource.getLookupVariables(),
                    indexSource.getOutputVariables(),
                    indexSource.getAssignments(),
                    indexSource.getCurrentConstraint());
        }

        private ValuesNode createEmptyValuesNode(IndexSourceNode indexSource)
        {
            session.getWarningCollector().add(
                    new PrestoWarning(THRIFT_INDEXSOURCE_CONVERTED_TO_VALUESNODE,
                            format("Table '%s' returns 0 rows, and is converted to an empty %s by %s",
                                    indexSource.getTableHandle().getConnectorHandle(),
                                    ValuesNode.class.getSimpleName(),
                                    ThriftFilterPushdown.class.getSimpleName())));
            return new ValuesNode(
                    indexSource.getSourceLocation(),
                    idAllocator.getNextId(),
                    indexSource.getOutputVariables(),
                    ImmutableList.of(),
                    Optional.of(indexSource.getTableHandle().getConnectorHandle().toString()));
        }
    }
}
