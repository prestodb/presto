package com.facebook.presto.connector.system;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SystemSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public SystemSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
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
    public Iterable<Split> getPartitionSplits(List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        Preconditions.checkArgument(!partitions.isEmpty(), "partitions is empty");

        Partition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof SystemPartition, "Partition must be a system partition");
        SystemPartition systemPartition = (SystemPartition) partition;

        Optional<Node> currentNode = nodeManager.getCurrentNode();
        Preconditions.checkState(currentNode.isPresent(), "current node is not in the active set");
        ImmutableList<HostAddress> localAddress = ImmutableList.of(currentNode.get().getHostAndPort());

        ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();
        for (Entry<ColumnHandle, Object> entry : systemPartition.getFilters().entrySet()) {
            SystemColumnHandle systemColumnHandle = (SystemColumnHandle) entry.getKey();
            filters.put(systemColumnHandle.getColumnName(), entry.getValue());
        }

        Split split = new SystemSplit(systemPartition.table, filters.build(), localAddress);

        return ImmutableList.of(split);
    }

    public static class SystemPartition
            implements Partition
    {
        private final SystemTableHandle table;
        private final Map<ColumnHandle, Object> filters;

        public SystemPartition(SystemTableHandle table, Map<ColumnHandle, Object> filters)
        {
            this.table = table;
            this.filters = ImmutableMap.copyOf(checkNotNull(filters, "filters is null"));
        }

        public SystemTableHandle getTable()
        {
            return table;
        }

        @Override
        public String getPartitionId()
        {
            return table.getSchemaTableName().toString();
        }

        @Override
        public Map<ColumnHandle, String> getKeys()
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
                    .add("table", table)
                    .add("filters", filters)
                    .toString();
        }
    }
}
