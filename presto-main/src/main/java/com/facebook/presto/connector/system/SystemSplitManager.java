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
package com.facebook.presto.connector.system;

import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.SystemTable.Distribution;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.SystemTable.Distribution.ALL_COORDINATORS;
import static com.facebook.presto.spi.SystemTable.Distribution.ALL_NODES;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class SystemSplitManager
        implements ConnectorSplitManager
{
    private final InternalNodeManager nodeManager;
    private final Map<SchemaTableName, SystemTable> tables;

    public SystemSplitManager(InternalNodeManager nodeManager, Set<SystemTable> tables)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.tables = uniqueIndex(tables, table -> table.getTableMetadata().getTable());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        SystemTableLayoutHandle layoutHandle = checkType(layout, SystemTableLayoutHandle.class, "layout");
        SystemTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<ColumnHandle> constraint = layoutHandle.getConstraint();
        SystemTable systemTable = tables.get(tableHandle.getSchemaTableName());

        Distribution tableDistributionMode = systemTable.getDistribution();
        if (tableDistributionMode == SINGLE_COORDINATOR) {
            HostAddress address = nodeManager.getCurrentNode().getHostAndPort();
            ConnectorSplit split = new SystemSplit(tableHandle.getConnectorId(), tableHandle, address, constraint);
            return new FixedSplitSource(ImmutableList.of(split));
        }

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        ImmutableSet.Builder<Node> nodes = ImmutableSet.builder();
        if (tableDistributionMode == ALL_COORDINATORS) {
            nodes.addAll(nodeManager.getCoordinators());
        }
        else if (tableDistributionMode == ALL_NODES) {
            nodes.addAll(nodeManager.getNodes(ACTIVE));
        }
        Set<Node> nodeSet = nodes.build();
        for (Node node : nodeSet) {
            splits.add(new SystemSplit(tableHandle.getConnectorId(), tableHandle, node.getHostAndPort(), constraint));
        }
        return new FixedSplitSource(splits.build());
    }
}
