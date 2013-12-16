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

import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TableAlias;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.MaterializedViewWriterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableAliasSelector
        extends PlanOptimizer
{
    private final Metadata metadata;
    private final AliasDao aliasDao;
    private final NodeManager nodeManager;
    private final ShardManager shardManager;

    public TableAliasSelector(Metadata metadata,
            AliasDao aliasDao,
            NodeManager nodeManager,
            ShardManager shardManager)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.aliasDao = checkNotNull(aliasDao, "aliasDao is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        // don't optimize plans that actually write local tables. We always want to
        // read the remote table in that case.
        if (containsMaterializedViewWriter(plan)) {
            return plan;
        }

        return PlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static boolean containsMaterializedViewWriter(PlanNode plan)
    {
        if (plan == null) {
            return false;
        }
        if (plan instanceof MaterializedViewWriterNode) {
            return true;
        }
        for (PlanNode sourceNode : plan.getSources()) {
            if (containsMaterializedViewWriter(sourceNode)) {
                return true;
            }
        }

        return false;
    }

    private class Rewriter
            extends PlanNodeRewriter<Void>
    {
        @Override
        public PlanNode rewriteTableScan(TableScanNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            TableHandle tableHandle = node.getTable();

            String connectorId = metadata.getConnectorId(tableHandle);
            SchemaTableName tableName = metadata.getTableMetadata(tableHandle).getTable();

            TableAlias tableAlias = aliasDao.getAlias(connectorId, tableName.getSchemaName(), tableName.getTableName());
            if (tableAlias == null) {
                return node;
            }

            Optional<TableHandle> aliasTableHandle = metadata.getTableHandle(
                    tableAlias.getDestinationConnectorId(),
                    new SchemaTableName(tableAlias.getDestinationSchemaName(), tableAlias.getDestinationTableName()));

            if (!aliasTableHandle.isPresent()) {
                return node;
            }

            if (!allNodesPresent(aliasTableHandle.get())) {
                return node;
            }

            Map<String, ColumnHandle> lookupColumns = metadata.getColumnHandles(aliasTableHandle.get());

            Map<Symbol, ColumnHandle> assignments = node.getAssignments();

            ImmutableMap.Builder<Symbol, ColumnHandle> newAssignmentsBuilder = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> assignmentEntry : assignments.entrySet()) {
                ColumnMetadata originalColumn = metadata.getColumnMetadata(tableHandle, assignmentEntry.getValue());

                ColumnHandle aliasedColumnHandle = lookupColumns.get(originalColumn.getName());
                checkState(aliasedColumnHandle != null, "no matching column for original column %s found!", originalColumn);
                newAssignmentsBuilder.put(assignmentEntry.getKey(), aliasedColumnHandle);
            }

            return new TableScanNode(node.getId(), aliasTableHandle.get(), node.getOutputSymbols(), newAssignmentsBuilder.build(), node.getOriginalConstraint(), node.getGeneratedPartitions());
        }

        private boolean allNodesPresent(TableHandle tableHandle)
        {
            Set<String> nodesActive = ImmutableSet.copyOf(Collections2.transform(nodeManager.getAllNodes().getActiveNodes(), Node.getIdentifierFunction()));
            Set<String> nodesRequired = shardManager.getTableNodes(tableHandle);

            return nodesActive.containsAll(nodesRequired);
        }
    }
}
