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

import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.JoinTableInfo;
import com.facebook.presto.spi.JoinTableSet;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * JdbcJoinPushdown Optimizer:
 * This Optimizer converts a {@link JoinTableSet} to a {@link JdbcTableHandle} with its {@code joinTables} set to
 * those from {@code innerJoinTableInfos} in {@link JoinTableSet}.
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
            if (tableHandle.getConnectorHandle() instanceof JoinTableSet) {
                JoinTableSet tableHandles = (JoinTableSet) tableHandle.getConnectorHandle();
                ImmutableList<JoinTableInfo> newJoinTableInfos = ImmutableList.copyOf(tableHandles.getInnerJoinTableInfos());

                ImmutableList.Builder<ConnectorTableHandle> newConnectorTableHandlesBuilder = ImmutableList.builder();
                Map<VariableReferenceExpression, ColumnHandle> groupAssignments = new HashMap<>();

                //Generate aliases for each table being joined to avoid any ambiguous column name references.
                // These aliases are used in QueryBuilder#buildSql
                final String aliasPrefix = "T";
                int aliasTableCounter = 0;

                // Create new table handles and update column handles
                for (JoinTableInfo tableMapping : newJoinTableInfos) {
                    JdbcTableHandle handle = (JdbcTableHandle) tableMapping.getTableHandle();
                    JdbcTableHandle newHandle = new JdbcTableHandle(handle.getConnectorId(), handle.getSchemaTableName(), handle.getCatalogName(),
                            handle.getSchemaName(), handle.getTableName(), handle.getJoinTables(), Optional.of(aliasPrefix + (++aliasTableCounter)));
                    newConnectorTableHandlesBuilder.add(newHandle);

                    tableMapping.getAssignments().forEach((key, oldColumnHandle) -> {
                        JdbcColumnHandle newColumnHandle = new JdbcColumnHandle(((JdbcColumnHandle) oldColumnHandle).getConnectorId(), ((JdbcColumnHandle) oldColumnHandle).getColumnName(),
                                ((JdbcColumnHandle) oldColumnHandle).getJdbcTypeHandle(), ((JdbcColumnHandle) oldColumnHandle).getColumnType(), ((JdbcColumnHandle) oldColumnHandle).isNullable(),
                                ((JdbcColumnHandle) oldColumnHandle).getComment(), newHandle.getTableAlias());
                        groupAssignments.put(key, newColumnHandle); // Update the map entry
                    });
                }

                List<ConnectorTableHandle> newConnectorTableHandles = newConnectorTableHandlesBuilder.build();
                JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) newConnectorTableHandles.get(0);
                JdbcTableHandle newConnectorTableHandle = new JdbcTableHandle(jdbcTableHandle.getConnectorId(), jdbcTableHandle.getSchemaTableName(), jdbcTableHandle.getCatalogName(),
                        jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName(), newConnectorTableHandles, jdbcTableHandle.getTableAlias());

                Optional<ConnectorTableLayoutHandle> optionalTableLayoutHandle = tableHandle.getLayout().map(oldTableLayoutHandle -> {
                    JdbcTableLayoutHandle oldLayout = (JdbcTableLayoutHandle) oldTableLayoutHandle;
                    return new JdbcTableLayoutHandle(newConnectorTableHandle, oldLayout.getTupleDomain(), oldLayout.getAdditionalPredicate(), oldLayout.getLayoutString());
                });

                TableHandle newTableHandle = new TableHandle(tableHandle.getConnectorId(), newConnectorTableHandle, tableHandle.getTransaction(), optionalTableLayoutHandle, tableHandle.getDynamicFilter());
                return new TableScanNode(node.getSourceLocation(), node.getId(), newTableHandle, node.getOutputVariables(), groupAssignments, node.getCurrentConstraint(), node.getEnforcedConstraint(), node.getCteMaterializationInfo());
            }
            return node;
        }
    }
}
