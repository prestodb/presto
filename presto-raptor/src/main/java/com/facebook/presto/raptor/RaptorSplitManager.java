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

import com.facebook.presto.raptor.backup.BackupService;
import com.facebook.presto.raptor.metadata.BucketShards;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardNodes;
import com.facebook.presto.raptor.util.SynchronizedResultIterator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import org.skife.jdbi.v2.ResultIterator;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_NO_HOST_FOR_SHARD;
import static com.facebook.presto.raptor.RaptorSessionProperties.getOneSplitPerBucketThreshold;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toSet;

public class RaptorSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NodeSupplier nodeSupplier;
    private final ShardManager shardManager;
    private final boolean backupAvailable;
    private final ExecutorService executor;

    @Inject
    public RaptorSplitManager(RaptorConnectorId connectorId, NodeSupplier nodeSupplier, ShardManager shardManager, BackupService backupService)
    {
        this(connectorId, nodeSupplier, shardManager, requireNonNull(backupService, "backupService is null").isBackupAvailable());
    }

    public RaptorSplitManager(RaptorConnectorId connectorId, NodeSupplier nodeSupplier, ShardManager shardManager, boolean backupAvailable)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.backupAvailable = backupAvailable;
        this.executor = newCachedThreadPool(daemonThreadsNamed("raptor-split-" + connectorId + "-%s"));
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, List<ColumnHandle> columns)
    {
        RaptorTableLayoutHandle handle = (RaptorTableLayoutHandle) layout;
        RaptorTableHandle table = handle.getTable();
        TupleDomain<RaptorColumnHandle> effectivePredicate = toRaptorTupleDomain(handle.getConstraint());
        long tableId = table.getTableId();
        boolean bucketed = table.getBucketCount().isPresent();
        boolean merged = bucketed && !table.isDelete() && (table.getBucketCount().getAsInt() >= getOneSplitPerBucketThreshold(session));
        OptionalLong transactionId = table.getTransactionId();
        Optional<Map<Integer, String>> bucketToNode = handle.getPartitioning().map(RaptorPartitioningHandle::getBucketToNode);
        verify(bucketed == bucketToNode.isPresent(), "mismatched bucketCount and bucketToNode presence");
        return new RaptorSplitSource(tableId, merged, effectivePredicate, transactionId, bucketToNode);
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
        return tupleDomain.transform(handle -> (RaptorColumnHandle) handle);
    }

    private static <T> T selectRandom(Iterable<T> elements)
    {
        List<T> list = ImmutableList.copyOf(elements);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    private class RaptorSplitSource
            implements ConnectorSplitSource
    {
        private final Map<String, Node> nodesById = uniqueIndex(nodeSupplier.getWorkerNodes(), Node::getNodeIdentifier);
        private final long tableId;
        private final TupleDomain<RaptorColumnHandle> effectivePredicate;
        private final OptionalLong transactionId;
        private final Optional<Map<Integer, String>> bucketToNode;
        private final ResultIterator<BucketShards> iterator;

        @GuardedBy("this")
        private CompletableFuture<List<ConnectorSplit>> future;

        public RaptorSplitSource(
                long tableId,
                boolean merged,
                TupleDomain<RaptorColumnHandle> effectivePredicate,
                OptionalLong transactionId,
                Optional<Map<Integer, String>> bucketToNode)
        {
            this.tableId = tableId;
            this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
            this.transactionId = requireNonNull(transactionId, "transactionId is null");
            this.bucketToNode = requireNonNull(bucketToNode, "bucketToNode is null");

            ResultIterator<BucketShards> iterator;
            if (bucketToNode.isPresent()) {
                iterator = shardManager.getShardNodesBucketed(tableId, merged, bucketToNode.get(), effectivePredicate);
            }
            else {
                iterator = shardManager.getShardNodes(tableId, effectivePredicate);
            }
            this.iterator = new SynchronizedResultIterator<>(iterator);
        }

        @Override
        public synchronized CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
        {
            checkState((future == null) || future.isDone(), "previous batch not completed");
            future = supplyAsync(batchSupplier(maxSize), executor);
            return future;
        }

        @Override
        public synchronized void close()
        {
            if (future != null) {
                future.cancel(true);
                future = null;
            }
            executor.execute(iterator::close);
        }

        @Override
        public boolean isFinished()
        {
            return !iterator.hasNext();
        }

        private Supplier<List<ConnectorSplit>> batchSupplier(int maxSize)
        {
            return () -> {
                ImmutableList.Builder<ConnectorSplit> list = ImmutableList.builder();
                for (int i = 0; i < maxSize; i++) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException("Split batch fetch was interrupted");
                    }
                    if (!iterator.hasNext()) {
                        break;
                    }
                    list.add(createSplit(iterator.next()));
                }
                return list.build();
            };
        }

        private ConnectorSplit createSplit(BucketShards bucketShards)
        {
            if (bucketShards.getBucketNumber().isPresent()) {
                return createBucketSplit(bucketShards.getBucketNumber().getAsInt(), bucketShards.getShards());
            }

            verify(bucketShards.getShards().size() == 1, "wrong shard count for non-bucketed table");
            ShardNodes shard = getOnlyElement(bucketShards.getShards());
            UUID shardId = shard.getShardUuid();
            Set<String> nodeIds = shard.getNodeIdentifiers();

            List<HostAddress> addresses = getAddressesForNodes(nodesById, nodeIds);
            if (addresses.isEmpty()) {
                if (!backupAvailable) {
                    throw new PrestoException(RAPTOR_NO_HOST_FOR_SHARD, format("No host for shard %s found: %s", shardId, nodeIds));
                }

                // Pick a random node and optimistically assign the shard to it.
                // That node will restore the shard from the backup location.
                Set<Node> availableNodes = nodeSupplier.getWorkerNodes();
                if (availableNodes.isEmpty()) {
                    throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }
                Node node = selectRandom(availableNodes);
                shardManager.assignShard(tableId, shardId, node.getNodeIdentifier(), true);
                addresses = ImmutableList.of(node.getHostAndPort());
            }

            return new RaptorSplit(connectorId, shardId, addresses, effectivePredicate, transactionId);
        }

        private ConnectorSplit createBucketSplit(int bucketNumber, Set<ShardNodes> shards)
        {
            // Bucket splits contain all the shards for the bucket
            // and run on the node assigned to the bucket.

            String nodeId = bucketToNode.get().get(bucketNumber);
            Node node = nodesById.get(nodeId);
            if (node == null) {
                throw new PrestoException(NO_NODES_AVAILABLE, "Node for bucket is offline: " + nodeId);
            }

            Set<UUID> shardUuids = shards.stream()
                    .map(ShardNodes::getShardUuid)
                    .collect(toSet());
            HostAddress address = node.getHostAndPort();

            return new RaptorSplit(connectorId, shardUuids, bucketNumber, address, effectivePredicate, transactionId);
        }
    }
}
