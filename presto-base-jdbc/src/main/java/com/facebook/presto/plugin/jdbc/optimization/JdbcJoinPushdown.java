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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableHandleSet;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * JdbcJoinPushdown Optimizer:
 * This Optimizer converts ConnectorHandleSet with various TableHandles to JdbcTableHandle which has list of all the JdbcTableHandles that can be pushed down.
 * This results in Jdbc Join Pushdown.
 *
 * Example:
 * Before Transformation:
 *   --OutputNode
 *   `-- TableScanNode
 *       `-- Set<ConnectorTableHandle> (ConnectorHandleSet)
 *          `-- TableHandle1
 *          `-- TableHandle2
 *          `-- TableHandle3
 *
 * After Transformation:
 *   --OutputNode
 *   `-- TableScanNode
 *       `-- JdbcTableHandle
 *           `-- List<ConnectorTableHandle>
 *               `-- JdbcTableHandle1
 *               `-- JdbcTableHandle2
 *               `-- JdbcTableHandle3
 */
public class JdbcJoinPushdown
        implements ConnectorPlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(session, idAllocator), maxSubplan);
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            TableHandle tableHandle = node.getTable();
            if (tableHandle.getConnectorHandle() instanceof ConnectorTableHandleSet) {
                ConnectorTableHandleSet tableHandles = (ConnectorTableHandleSet) tableHandle.getConnectorHandle();
                List<ConnectorTableHandle> newConnectorTableHandles = ImmutableList.copyOf(tableHandles.getConnectorTableHandles());
                JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) newConnectorTableHandles.get(0);

                JdbcTableHandle newConnectorTableHandle = new JdbcTableHandle(jdbcTableHandle.getConnectorId(), jdbcTableHandle.getSchemaTableName(),
                        jdbcTableHandle.getCatalogName(), jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName(),
                        Optional.of(newConnectorTableHandles), jdbcTableHandle.getTableAlias());

                Optional<ConnectorTableLayoutHandle> optionalTableLayoutHandle = tableHandle.getLayout();
                if (optionalTableLayoutHandle.isPresent()) {
                    JdbcTableLayoutHandle oldTableLayoutHandle = (JdbcTableLayoutHandle) tableHandle.getLayout().get();
                    optionalTableLayoutHandle = Optional.of(new JdbcTableLayoutHandle(newConnectorTableHandle,
                            oldTableLayoutHandle.getTupleDomain(), oldTableLayoutHandle.getAdditionalPredicate(), oldTableLayoutHandle.getLayoutString()));
                }

                TableHandle newTableHandle = new TableHandle(tableHandle.getConnectorId(), newConnectorTableHandle, tableHandle.getTransaction(),
                        optionalTableLayoutHandle, tableHandle.getDynamicFilter());
                return new TableScanNode(node.getSourceLocation(), node.getId(), newTableHandle, node.getOutputVariables(), node.getAssignments(),
                        node.getCurrentConstraint(), node.getEnforcedConstraint());
            }
            return node;
        }
    }
}
