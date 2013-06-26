package com.facebook.presto.connector.system;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof SystemTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        checkNotNull(table, "table is null");
        checkNotNull(bindings, "bindings is null");

        checkArgument(table instanceof SystemTableHandle, "TableHandle must be an SystemTableHandle");
        SystemTableHandle systemTableHandle = (SystemTableHandle) table;

        return ImmutableList.<Partition>of(new SystemPartition(systemTableHandle, bindings));
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle table, List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        Partition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof SystemPartition, "Partition must be a system partition");
        SystemPartition systemPartition = (SystemPartition) partition;

        SystemTable systemTable = tables.get(systemPartition.getTableHandle().getSchemaTableName());
        checkArgument(systemTable != null, "Table %s does not exist", systemPartition.getTableHandle().getTableName());

        ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();
        for (Entry<ColumnHandle, Object> entry : systemPartition.getFilters().entrySet()) {
            SystemColumnHandle systemColumnHandle = (SystemColumnHandle) entry.getKey();
            filters.put(systemColumnHandle.getColumnName(), entry.getValue());
        }

        if (systemTable.isDistributed()) {
            ImmutableList.Builder<Split> splits = ImmutableList.builder();
            for (Node node : nodeManager.getActiveNodes()) {
                splits.add(new SystemSplit(systemPartition.tableHandle, filters.build(), ImmutableList.of(node.getHostAndPort())));
            }
            return splits.build();
        }
        else {
            // table is not distributed
            Optional<Node> currentNode = nodeManager.getCurrentNode();
            Preconditions.checkState(currentNode.isPresent(), "current node is not in the active set");
            Split split = new SystemSplit(systemPartition.tableHandle, filters.build(), ImmutableList.of(currentNode.get().getHostAndPort()));
            return ImmutableList.of(split);
        }
    }

    public static class SystemPartition
            implements Partition
    {
        private final SystemTableHandle tableHandle;
        private final Map<ColumnHandle, Object> filters;

        public SystemPartition(SystemTableHandle tableHandle, Map<ColumnHandle, Object> filters)
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
        public Map<ColumnHandle, Object> getKeys()
        {
            return ImmutableMap.of();
        }

        public Map<ColumnHandle, Object> getFilters()
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
