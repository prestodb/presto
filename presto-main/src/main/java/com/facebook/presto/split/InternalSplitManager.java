package com.facebook.presto.split;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.metadata.HostAddress;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
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

public class InternalSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public InternalSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof InternalTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        checkNotNull(table, "table is null");
        checkNotNull(bindings, "bindings is null");

        checkArgument(table instanceof InternalTableHandle, "TableHandle must be an InternalTableHandle");
        InternalTableHandle internalTableHandle = (InternalTableHandle) table;

        return ImmutableList.<Partition>of(new InternalPartition(internalTableHandle, bindings));
    }

    @Override
    public DataSource getPartitionSplits(List<Partition> partitions, List<ColumnHandle> columnNames)
    {
        checkNotNull(partitions, "partitions is null");
        Preconditions.checkArgument(!partitions.isEmpty(), "partitions is empty");
        checkNotNull(columnNames, "columnNames is null");

        Partition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof InternalPartition, "Partition must be an internal partition");
        InternalPartition internalPartition = (InternalPartition) partition;

        Optional<Node> currentNode = nodeManager.getCurrentNode();
        Preconditions.checkState(currentNode.isPresent(), "current node is not in the active set");
        ImmutableList<HostAddress> localAddress = ImmutableList.of(currentNode.get().getHostAndPort());

        ImmutableMap.Builder<InternalColumnHandle, Object> filters = ImmutableMap.builder();
        for (Entry<ColumnHandle, Object> entry : internalPartition.getFilters().entrySet()) {
            filters.put((InternalColumnHandle) entry.getKey(), entry.getValue());
        }

        Split split = new InternalSplit(internalPartition.table, filters.build(), localAddress);

        return new DataSource(null, ImmutableList.of(split));
    }

    public static class InternalPartition
            implements Partition
    {
        private final InternalTableHandle table;
        private final Map<ColumnHandle, Object> filters;

        public InternalPartition(InternalTableHandle table, Map<ColumnHandle, Object> filters)
        {
            this.table = table;
            this.filters = ImmutableMap.copyOf(checkNotNull(filters, "filters is null"));
        }

        public InternalTableHandle getTable()
        {
            return table;
        }

        @Override
        public String getPartitionId()
        {
            return table.getTableName().toString();
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
