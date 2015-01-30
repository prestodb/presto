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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardNodes;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Starts a reassigner on all coordinators. Only one coordinator performs reassignment.
 * Keeps track of online nodes to identify if shards need to be reassigned
 */
public class ShardReassigner
{
    private static final Logger log = Logger.get(ShardReassigner.class);

    private final ScheduledExecutorService shardReassigner = Executors.newScheduledThreadPool(1, daemonThreadsNamed("shard-reassigner"));
    private final ScheduledExecutorService onlineNodesTracker = Executors.newScheduledThreadPool(1, daemonThreadsNamed("online-nodes-tracker"));

    private final AtomicBoolean shardReassignerStarted = new AtomicBoolean();
    private final AtomicBoolean onlineNodesTrackerStarted = new AtomicBoolean();

    private final LoadingCache<String, Long> onlineNodes;
    private final MetadataDao dao;

    private final StorageService storageService;
    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final Duration shardReassignmentInterval;

    @Inject
    public ShardReassigner(@ForMetadata IDBI dbi, StorageService storageService, NodeManager nodeManager, ShardManager shardManager, StorageManagerConfig config)
    {
        this(dbi, storageService, nodeManager, shardManager, config.getShardReassignmentInterval());
    }

    public ShardReassigner(@ForMetadata IDBI dbi, StorageService storageService, NodeManager nodeManager, ShardManager shardManager, Duration shardReassignmentInterval)
    {
        this.dao = checkNotNull(dbi, "dbi is null").onDemand(MetadataDao.class);
        this.storageService = checkNotNull(storageService, "storageService is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.shardReassignmentInterval = checkNotNull(shardReassignmentInterval, "shardReassignmentInterval is null");
        this.onlineNodes = CacheBuilder.newBuilder()
                .expireAfterWrite(shardReassignmentInterval.toMillis() * 10, MILLISECONDS) // evict nodes if they are not updated for a certain time
                .build(new CacheLoader<String, Long>()
                {
                    @Override
                    public Long load(String nodeIdentifier)
                            throws Exception
                    {
                        return System.nanoTime();
                    }
                });
    }

    @PostConstruct
    public void start()
    {
        if (!storageService.isBackupAvailable() || !isCoordinator()) {
            return;
        }

        if (onlineNodesTrackerStarted.compareAndSet(false, true)) {
            startOnlineNodesTracker();
        }

        if (shardReassignerStarted.compareAndSet(false, true)) {
            // start reassigner on all coordinators
            startShardReassigner();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        shardReassigner.shutdownNow();
        onlineNodesTracker.shutdownNow();
    }

    private void startOnlineNodesTracker()
    {
        // Ensure we don't refresh more than once per second
        long delay = Math.max(1000, shardReassignmentInterval.toMillis() / 10);
        onlineNodesTracker.scheduleWithFixedDelay(this::updateOnlineNodes, 0, delay, MILLISECONDS);
    }

    private void updateOnlineNodes()
    {
        try {
            nodeManager.getActiveNodes().stream().map(Node::getNodeIdentifier).forEach(onlineNodes::refresh);
        }
        catch (Throwable t) {
            log.error(t, "Unexpected error in updating online nodes");
        }
    }

    private void startShardReassigner()
    {
        shardReassigner.scheduleWithFixedDelay(() -> {
            if (!isActiveReassigner()) {
                return;
            }
            updateOnlineNodes();

            try {
                List<UUID> shardsToReassign = getShardsToReassign();
                if (shardsToReassign.isEmpty()) {
                    return;
                }
                Set<Node> activeDatasourceNodes = nodeManager.getActiveNodes();
                log.info(format("Need to reassign %s shards: ", shardsToReassign.size()));
                shardsToReassign.forEach(uuid -> reassignShard(uuid, activeDatasourceNodes));
            }
            catch (Throwable t) {
                log.error(t, "Unexpected error in shard reassignment");
            }
        }, shardReassignmentInterval.toMillis(), shardReassignmentInterval.toMillis(), MILLISECONDS);
    }

    private List<UUID> getShardsToReassign()
            throws ExecutionException
    {
        ImmutableList.Builder<UUID> shardsToReassign = ImmutableList.builder();
        // Getting all shards can be expensive, so break it down by table
        for (long tableId : dao.getAllTableIds()) {
            for (ShardNodes shardNode : shardManager.getShardNodes(tableId)) {
                if (!isAnyNodeOnline(shardNode.getNodeIdentifiers())) {
                    shardsToReassign.add(shardNode.getShardUuid());
                }
            }
        }
        return shardsToReassign.build();
    }

    public Node reassignShard(UUID shardId)
    {
        return reassignShard(shardId, nodeManager.getActiveNodes());
    }

    private Node reassignShard(UUID shardId, Set<Node> activeDatasourceNodes)
    {
        if (activeDatasourceNodes.isEmpty()) {
            throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
        }

        // Pick a random node and optimistically assign the shard to it.
        // That node will restore the shard from the backup location.
        Node node = selectRandom(activeDatasourceNodes);
        shardManager.assignShard(shardId, node.getNodeIdentifier());
        return node;
    }

    private boolean isCoordinator()
    {
        return nodeManager.getCoordinators().contains(nodeManager.getCurrentNode());
    }

    private boolean isAnyNodeOnline(Set<String> nodeIdentifiers)
    {
        for (String nodeId : nodeIdentifiers) {
            Long lastUpdate = onlineNodes.getIfPresent(nodeId);
            if (lastUpdate != null && Duration.nanosSince(lastUpdate).compareTo(shardReassignmentInterval) <= 0) {
                return true;
            }
        }
        return false;
    }

    private boolean isActiveReassigner()
    {
        return shardManager.compareAndUpdateShardReassigner(nodeManager.getCurrentNode().getNodeIdentifier(), shardReassignmentInterval);
    }

    private static <T> T selectRandom(Iterable<T> elements)
    {
        List<T> list = ImmutableList.copyOf(elements);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
