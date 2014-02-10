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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

public class SystemSplitManager
        implements ConnectorSplitManager
{
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
    public boolean canHandle(ConnectorTableHandle handle)
    {
        return handle instanceof SystemTableHandle;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        checkNotNull(table, "table is null");
        checkNotNull(tupleDomain, "tupleDomain is null");

        checkArgument(table instanceof SystemTableHandle, "TableHandle must be an SystemTableHandle");
        SystemTableHandle systemTableHandle = (SystemTableHandle) table;

        Map<ConnectorColumnHandle, Comparable<?>> bindings = tupleDomain.extractFixedValues();

        TupleDomain<ConnectorColumnHandle> unusedTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            unusedTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(bindings.keySet()))));
        }

        ImmutableList<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new SystemPartition(systemTableHandle, bindings));
        return new ConnectorPartitionResult(partitions, unusedTupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(null, ImmutableList.<ConnectorSplit>of());
        }

        ConnectorPartition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof SystemPartition, "Partition must be a system partition");
        SystemPartition systemPartition = (SystemPartition) partition;

        SystemTable systemTable = tables.get(systemPartition.getTableHandle().getSchemaTableName());
        checkArgument(systemTable != null, "Table %s does not exist", systemPartition.getTableHandle().getTableName());

        ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();
        for (Entry<ConnectorColumnHandle, Comparable<?>> entry : systemPartition.getFilters().entrySet()) {
            SystemColumnHandle systemColumnHandle = (SystemColumnHandle) entry.getKey();
            filters.put(systemColumnHandle.getColumnName(), entry.getValue());
        }

        if (systemTable.isDistributed()) {
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (Node node : nodeManager.getActiveNodes()) {
                splits.add(new SystemSplit(systemPartition.tableHandle, filters.build(), node.getHostAndPort()));
            }
            return new FixedSplitSource(null, splits.build());
        }

        HostAddress address = nodeManager.getCurrentNode().getHostAndPort();
        ConnectorSplit split = new SystemSplit(systemPartition.tableHandle, filters.build(), address);
        return new FixedSplitSource(null, ImmutableList.of(split));
    }

    public static class SystemPartition
            implements ConnectorPartition
    {
        private final SystemTableHandle tableHandle;
        private final Map<ConnectorColumnHandle, Comparable<?>> filters;

        public SystemPartition(SystemTableHandle tableHandle, Map<ConnectorColumnHandle, Comparable<?>> filters)
        {
            this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
            this.filters = ImmutableMap.copyOf(checkNotNull(filters, "filters is null"));
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
            return TupleDomain.withFixedValues(filters);
        }

        public Map<ConnectorColumnHandle, Comparable<?>> getFilters()
        {
            return filters;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("tableHandle", tableHandle)
                    .add("filters", filters)
                    .toString();
        }
    }
}
