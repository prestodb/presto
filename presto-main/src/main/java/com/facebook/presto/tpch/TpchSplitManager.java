package com.facebook.presto.tpch;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NodeManager nodeManager;

    @Inject
    public TpchSplitManager(String connectorId, NodeManager nodeManager)
    {
        this.connectorId = connectorId;
        this.nodeManager = nodeManager;
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof TpchTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        return ImmutableList.<Partition>of(new TpchPartition((TpchTableHandle) table));
    }

    @Override
    public Iterable<Split> getPartitionSplits(List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        Partition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof TpchPartition, "Partition must be a tpch partition");
        TpchTableHandle tableHandle = ((TpchPartition) partition).getTable();

        Set<Node> nodes = nodeManager.getActiveNodes();

        int totalParts = nodes.size();
        int partNumber = 0;

        // Split the data using split and skew by the number of nodes available.
        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        for (Node node : nodes) {
            TpchSplit tpchSplit = new TpchSplit(tableHandle, partNumber++, totalParts, ImmutableList.of(node.getHostAndPort()));
            splits.add(tpchSplit);
        }
        return splits.build();
    }

    public static class TpchPartition
            implements Partition
    {
        private final TpchTableHandle table;

        public TpchPartition(TpchTableHandle table)
        {
            this.table = table;
        }

        public TpchTableHandle getTable()
        {
            return table;
        }

        @Override
        public String getPartitionId()
        {
            return table.getTableName();
        }

        @Override
        public Map<ColumnHandle, Object> getKeys()
        {
            return ImmutableMap.of();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("table", table)
                    .toString();
        }
    }
}
