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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardNodes;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.util.CloseableIterator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_NO_HOST_FOR_SHARD;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.limit;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class RaptorSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final StorageManager storageManager;
    private final ExecutorService executor;

    @Inject
    public RaptorSplitManager(RaptorConnectorId connectorId, NodeManager nodeManager, ShardManager shardManager, StorageManager storageManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.executor = newCachedThreadPool(daemonThreadsNamed("raptor-split-" + connectorId + "-%s"));
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        RaptorTableHandle handle = checkType(tableHandle, RaptorTableHandle.class, "table");
        ConnectorPartition partition = new RaptorPartition(handle.getTableId(), tupleDomain);
        return new ConnectorPartitionResult(ImmutableList.of(partition), tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        RaptorTableHandle raptorTableHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");

        checkArgument(partitions.size() == 1, "expected exactly one partition");
        RaptorPartition partition = checkType(getOnlyElement(partitions), RaptorPartition.class, "partition");
        TupleDomain<RaptorColumnHandle> effectivePredicate = toRaptorTupleDomain(partition.getEffectivePredicate());

        return new RaptorSplitSource(raptorTableHandle.getTableId(), effectivePredicate);
    }

    private static List<HostAddress> getAddressesForNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        ImmutableList.Builder<HostAddress> nodes = ImmutableList.builder();
        for (String id : nodeIdentifiers) {
            Node node = nodeMap.get(id);
            if (node != null) {
                nodes.add(node.getHostAndPort());
            }
        }
        return nodes.build();
    }

    @SuppressWarnings("unchecked")
    private static TupleDomain<RaptorColumnHandle> toRaptorTupleDomain(TupleDomain<ColumnHandle> tupleDomain)
    {
        return tupleDomain.transform(new TupleDomain.Function<ColumnHandle, RaptorColumnHandle>()
        {
            @Override
            public RaptorColumnHandle apply(ColumnHandle handle)
            {
                return checkType(handle, RaptorColumnHandle.class, "columnHandle");
            }
        });
    }

    private static <T> T selectRandom(Iterable<T> elements)
    {
        List<T> list = ImmutableList.copyOf(elements);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    private class RaptorSplitSource
            implements ConnectorSplitSource
    {
        private final Map<String, Node> nodesById = uniqueIndex(nodeManager.getActiveNodes(), Node::getNodeIdentifier);
        private final long tableId;
        private final TupleDomain<RaptorColumnHandle> effectivePredicate;
        private final CloseableIterator<ShardNodes> iterator;

        public RaptorSplitSource(long tableId, TupleDomain<RaptorColumnHandle> effectivePredicate)
        {
            this.tableId = tableId;
            this.effectivePredicate = checkNotNull(effectivePredicate, "effectivePredicate is null");
            this.iterator = shardManager.getShardNodes(tableId, effectivePredicate);
        }

        @Override
        public String getDataSourceName()
        {
            return connectorId;
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
        {
            return supplyAsync(() -> ImmutableList.copyOf(transform(limit(iterator, maxSize), this::createSplit)), executor);
        }

        @Override
        public void close()
        {
            iterator.close();
        }

        @Override
        public boolean isFinished()
        {
            return !iterator.hasNext();
        }

        private ConnectorSplit createSplit(ShardNodes shard)
        {
            UUID shardId = shard.getShardUuid();
            Collection<String> nodeIds = shard.getNodeIdentifiers();

            List<HostAddress> addresses = getAddressesForNodes(nodesById, nodeIds);

            if (addresses.isEmpty()) {
                if (!storageManager.isBackupAvailable()) {
                    throw new PrestoException(RAPTOR_NO_HOST_FOR_SHARD, format("No host for shard %s found: %s", shardId, nodeIds));
                }

                // Pick a random node and optimistically assign the shard to it.
                // That node will restore the shard from the backup location.
                Set<Node> availableNodes = nodeManager.getActiveDatasourceNodes(connectorId);
                if (availableNodes.isEmpty()) {
                    throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }
                Node node = selectRandom(availableNodes);
                shardManager.assignShard(tableId, shardId, node.getNodeIdentifier());
                addresses = ImmutableList.of(node.getHostAndPort());
            }

            return new RaptorSplit(shardId, addresses, effectivePredicate);
        }
    }
}
