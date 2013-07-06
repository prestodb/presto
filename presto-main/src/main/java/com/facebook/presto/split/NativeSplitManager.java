package com.facebook.presto.split;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TablePartition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.Node.hostAndPortGetter;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;

public class NativeSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(NativeSplitManager.class);

    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final Metadata metadata;

    @Inject
    public NativeSplitManager(NodeManager nodeManager, ShardManager shardManager, Metadata metadata)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public String getConnectorId()
    {
        return "native";
    }

    @Override
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof NativeTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle tableHandle, Map<ColumnHandle, Object> bindings)
    {
        Stopwatch partitionTimer = new Stopwatch();
        partitionTimer.start();

        checkArgument(tableHandle instanceof NativeTableHandle, "Table must be a native table");

        TableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);

        checkState(tableMetadata != null, "no metadata for %s found", tableHandle);

        Set<TablePartition> tablePartitions = shardManager.getPartitions(tableHandle);

        log.debug("Partition retrieval, native table %s (%d partitions): %dms", tableHandle, tablePartitions.size(), partitionTimer.elapsed(TimeUnit.MILLISECONDS));

        Multimap<String, ? extends PartitionKey> allPartitionKeys = shardManager.getAllPartitionKeys(tableHandle);
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(tableHandle);

        log.debug("Partition key retrieval, native table %s (%d keys): %dms", tableHandle, allPartitionKeys.size(), partitionTimer.elapsed(TimeUnit.MILLISECONDS));

        List<Partition> partitions = ImmutableList.copyOf(Collections2.transform(tablePartitions, new PartitionFunction(columnHandles, allPartitionKeys)));

        log.debug("Partition generation, native table %s (%d partitions): %dms", tableHandle, partitions.size(), partitionTimer.elapsed(TimeUnit.MILLISECONDS));

        return partitions;
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle tableHandle, List<Partition> partitions)
    {
        Stopwatch splitTimer = new Stopwatch();
        splitTimer.start();

        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        Map<String, Node> nodesById = uniqueIndex(nodeManager.getAllNodes().getActiveNodes(), Node.getIdentifierFunction());

        List<Split> splits = new ArrayList<>();

        Multimap<Long, Entry<Long, String>> partitionShardNodes = shardManager.getCommittedPartitionShardNodes(tableHandle);

        for (Partition partition : partitions) {
            checkArgument(partition instanceof NativePartition, "Partition must be a native partition");
            NativePartition nativePartition = (NativePartition) partition;

            ImmutableMultimap.Builder<Long, String> shardNodes = ImmutableMultimap.builder();
            for (Entry<Long, String> partitionShardNode : partitionShardNodes.get(nativePartition.getNativePartitionId())) {
                shardNodes.put(partitionShardNode.getKey(), partitionShardNode.getValue());
            }

            for (Map.Entry<Long, Collection<String>> entry : shardNodes.build().asMap().entrySet()) {
                List<HostAddress> addresses = getAddressesForNodes(nodesById, entry.getValue());
                checkState(addresses.size() > 0, "no host for shard %s found", entry.getKey());
                Split split = new NativeSplit(entry.getKey(), addresses);
                splits.add(split);
            }
        }

        log.debug("Split retrieval for %d partitions (%d splits): %dms", partitions.size(), splits.size(), splitTimer.elapsed(TimeUnit.MILLISECONDS));

        // the query engine assumes that splits are returned in a somewhat random fashion. The native split manager,
        // because it loads the data from a db table will return the splits somewhat ordered by node id so only a sub
        // set of nodes is fired up. Shuffle the splits to ensure random distribution.
        Collections.shuffle(splits);

        return ImmutableList.copyOf(splits);
    }

    private static List<HostAddress> getAddressesForNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        return ImmutableList.copyOf(transform(transform(nodeIdentifiers, forMap(nodeMap)), hostAndPortGetter()));
    }

    public static class NativePartition
            implements Partition
    {
        private final long partitionId;
        private Map<ColumnHandle, Object> keys;

        public NativePartition(long partitionId, Map<ColumnHandle, Object> keys)
        {
            this.partitionId = partitionId;
            this.keys = keys;
        }

        @Override
        public String getPartitionId()
        {
            return Long.toString(partitionId);
        }

        public long getNativePartitionId()
        {
            return partitionId;
        }

        @Override
        public Map<ColumnHandle, Object> getKeys()
        {
            return keys;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(partitionId, keys);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final NativePartition other = (NativePartition) obj;
            return this.partitionId == other.partitionId
                    && Objects.equal(this.keys, other.keys);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("partitionId", partitionId)
                    .add("keys", keys)
                    .toString();
        }
    }
}
