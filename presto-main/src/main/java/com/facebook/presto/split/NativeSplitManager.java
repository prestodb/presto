package com.facebook.presto.split;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.metadata.HostAddress;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Node.hostAndPortGetter;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;

public class NativeSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final ShardManager shardManager;

    @Inject
    public NativeSplitManager(NodeManager nodeManager, ShardManager shardManager)
    {
        this.nodeManager = nodeManager;
        this.shardManager = shardManager;
    }

    @Override
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof NativeTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        return ImmutableList.<Partition>of(new NativePartition((NativeTableHandle) table));
    }

    @Override
    public DataSource getPartitionSplits(List<Partition> partitions, List<ColumnHandle> columns)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new DataSource("native", ImmutableList.<Split>of());
        }
        checkNotNull(columns, "columns is null");

        Partition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof NativePartition, "Partition must be a native partition");
        NativePartition nativePartition = (NativePartition) partition;

        Map<String, Node> nodesById = uniqueIndex(nodeManager.getActiveNodes(), Node.getIdentifierFunction());

        Multimap<Long, String> shardNodes = shardManager.getCommittedShardNodes(nativePartition.getTable().getTableId());

        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        for (Map.Entry<Long, Collection<String>> entry : shardNodes.asMap().entrySet()) {
            List<HostAddress> addresses = getAddressesForNodes(nodesById, entry.getValue());
            Split split = new NativeSplit(entry.getKey(), addresses);
            splits.add(split);
        }
        return new DataSource("native", splits.build());
    }

    private static List<HostAddress> getAddressesForNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        return ImmutableList.copyOf(transform(transform(nodeIdentifiers, forMap(nodeMap)), hostAndPortGetter()));
    }

    public static class NativePartition
            implements Partition
    {
        private final NativeTableHandle table;

        public NativePartition(NativeTableHandle table)
        {
            this.table = table;
        }

        public NativeTableHandle getTable()
        {
            return table;
        }

        @Override
        public String getPartitionId()
        {
            return String.valueOf(table.getTableId());
        }

        @Override
        public Map<ColumnHandle, String> getKeys()
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
