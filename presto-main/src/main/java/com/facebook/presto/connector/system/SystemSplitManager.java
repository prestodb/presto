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

import com.facebook.presto.spi.ConnectorColumnHandle;
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
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SystemSplitManager
        implements ConnectorSplitManager
{
    public static final String SYSTEM_DATASOURCE = "system";
    private final NodeManager nodeManager;
    private final ConcurrentMap<SchemaTableName, SystemTable> tables = new ConcurrentHashMap<>();

    @Inject
    public SystemSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    public void addTable(SystemTable systemTable)
    {
        checkNotNull(systemTable, "systemTable is null");
        SchemaTableName tableName = systemTable.getTableMetadata().getTable();
        checkArgument(tables.putIfAbsent(tableName, systemTable) == null, "Table %s is already registered", tableName);
    }

    @Override
    public String getConnectorId()
    {
        // system is not a connector
        return null;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        checkNotNull(table, "table is null");
        checkNotNull(tupleDomain, "tupleDomain is null");

        checkArgument(table instanceof SystemTableHandle, "TableHandle must be an SystemTableHandle");
        SystemTableHandle systemTableHandle = (SystemTableHandle) table;

        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new SystemPartition(systemTableHandle));
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(SYSTEM_DATASOURCE, ImmutableList.<ConnectorSplit>of());
        }

        ConnectorPartition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof SystemPartition, "Partition must be a system partition");
        SystemPartition systemPartition = (SystemPartition) partition;

        SystemTable systemTable = tables.get(systemPartition.getTableHandle().getSchemaTableName());
        checkArgument(systemTable != null, "Table %s does not exist", systemPartition.getTableHandle().getTableName());

        if (systemTable.isDistributed()) {
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (Node node : nodeManager.getActiveNodes()) {
                splits.add(new SystemSplit(systemPartition.tableHandle, node.getHostAndPort()));
            }
            return new FixedSplitSource(SYSTEM_DATASOURCE, splits.build());
        }

        HostAddress address = nodeManager.getCurrentNode().getHostAndPort();
        ConnectorSplit split = new SystemSplit(systemPartition.tableHandle, address);
        return new FixedSplitSource(SYSTEM_DATASOURCE, ImmutableList.of(split));
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
        public TupleDomain<ConnectorColumnHandle> getTupleDomain()
        {
            return TupleDomain.all();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("tableHandle", tableHandle)
                    .toString();
        }
    }
}
