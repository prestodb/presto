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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.uniqueIndex;

public class SystemSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final Map<SchemaTableName, SystemTable> tables;

    public SystemSplitManager(NodeManager nodeManager, Set<SystemTable> tables)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.tables = uniqueIndex(tables, table -> table.getTableMetadata().getTable());
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain)
    {
        checkNotNull(tupleDomain, "tupleDomain is null");
        SystemTableHandle systemTableHandle = checkType(table, SystemTableHandle.class, "table");

        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new SystemPartition(systemTableHandle));
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(SystemConnector.NAME, ImmutableList.<ConnectorSplit>of());
        }

        ConnectorPartition partition = Iterables.getOnlyElement(partitions);
        SystemPartition systemPartition = checkType(partition, SystemPartition.class, "partition");

        SystemTable systemTable = tables.get(systemPartition.getTableHandle().getSchemaTableName());
        checkArgument(systemTable != null, "Table %s does not exist", systemPartition.getTableHandle().getTableName());

        if (systemTable.isDistributed()) {
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (Node node : nodeManager.getActiveNodes()) {
                splits.add(new SystemSplit(systemPartition.getTableHandle(), node.getHostAndPort()));
            }
            return new FixedSplitSource(SystemConnector.NAME, splits.build());
        }

        HostAddress address = nodeManager.getCurrentNode().getHostAndPort();
        ConnectorSplit split = new SystemSplit(systemPartition.getTableHandle(), address);
        return new FixedSplitSource(SystemConnector.NAME, ImmutableList.of(split));
    }

    public static class SystemPartition
            implements ConnectorPartition
    {
        private final SystemTableHandle tableHandle;

        public SystemPartition(SystemTableHandle tableHandle)
        {
            this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        }

        public SystemTableHandle getTableHandle()
        {
            return tableHandle;
        }

        @Override
        public String getPartitionId()
        {
            return tableHandle.getSchemaTableName().toString();
        }

        @Override
        public TupleDomain<ColumnHandle> getTupleDomain()
        {
            return TupleDomain.all();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("tableHandle", tableHandle)
                    .toString();
        }
    }
}
