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

import com.facebook.presto.raptor.NodeSupplier;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.round;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ShardEjector
{
    private static final Logger log = Logger.get(ShardEjector.class);

    private final String currentNode;
    private final NodeSupplier nodeSupplier;
    private final ShardManager shardManager;
    private final StorageService storageService;
    private final Duration interval;
    private final Optional<BackupStore> backupStore;
    private final ScheduledExecutorService executor;

    private final AtomicBoolean started = new AtomicBoolean();

    private final CounterStat shardsEjected = new CounterStat();
    private final CounterStat jobErrors = new CounterStat();

    @Inject
    public ShardEjector(
            NodeManager nodeManager,
            NodeSupplier nodeSupplier,
            ShardManager shardManager,
            StorageService storageService,
            StorageManagerConfig config,
            Optional<BackupStore> backupStore,
            RaptorConnectorId connectorId)
    {
        this(nodeManager.getCurrentNode().getNodeIdentifier(),
                nodeSupplier,
                shardManager,
                storageService,
                config.getShardEjectorInterval(),
                backupStore,
                connectorId.toString());
    }

    public ShardEjector(
            String currentNode,
            NodeSupplier nodeSupplier,
            ShardManager shardManager,
            StorageService storageService,
            Duration interval,
            Optional<BackupStore> backupStore,
            String connectorId)
    {
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.interval = requireNonNull(interval, "interval is null");
        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.executor = newScheduledThreadPool(1, daemonThreadsNamed("shard-ejector-" + connectorId));
    }

    @PostConstruct
    public void start()
    {
        if (!backupStore.isPresent()) {
            return;
        }
        if (!started.getAndSet(true)) {
            startJob();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Managed
    @Nested
    public CounterStat getShardsEjected()
    {
        return shardsEjected;
    }

    @Managed
    @Nested
    public CounterStat getJobErrors()
    {
        return jobErrors;
    }

    private void startJob()
    {
        executor.scheduleWithFixedDelay(() -> {
            try {
                // jitter to avoid overloading database
                long interval = this.interval.roundTo(SECONDS);
                SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, interval));
                process();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error ejecting shards");
                jobErrors.update(1);
            }
        }, 0, interval.toMillis(), MILLISECONDS);
    }

    @VisibleForTesting
    void process()
    {
        checkState(backupStore.isPresent(), "backup store must be present");

        // get the size of assigned shards for each node
        Map<String, Long> nodes = shardManager.getNodeBytes();

        Set<String> activeNodes = nodeSupplier.getWorkerNodes().stream()
                .map(Node::getNodeIdentifier)
                .collect(toSet());

        // only include active nodes
        nodes = new HashMap<>(filterKeys(nodes, activeNodes::contains));

        if (nodes.isEmpty()) {
            return;
        }

        // get current node size
        if (!nodes.containsKey(currentNode)) {
            return;
        }
        long nodeSize = nodes.get(currentNode);

        // get average node size
        long averageSize = round(nodes.values().stream()
                .mapToLong(Long::longValue)
                .average()
                .getAsDouble());
        long maxSize = round(averageSize * 1.01);

        // skip if not above max
        if (nodeSize <= maxSize) {
            return;
        }

        // only include nodes that are below threshold
        nodes = new HashMap<>(filterValues(nodes, size -> size <= averageSize));

        // get non-bucketed node shards by size, largest to smallest
        List<ShardMetadata> shards = shardManager.getNodeShards(currentNode).stream()
                .filter(shard -> !shard.getBucketNumber().isPresent())
                .sorted(comparingLong(ShardMetadata::getCompressedSize).reversed())
                .collect(toList());

        // eject shards while current node is above max
        Queue<ShardMetadata> queue = new ArrayDeque<>(shards);
        while ((nodeSize > maxSize) && !queue.isEmpty()) {
            ShardMetadata shard = queue.remove();
            long shardSize = shard.getCompressedSize();
            UUID shardUuid = shard.getShardUuid();

            // verify backup exists
            if (!backupStore.get().shardExists(shardUuid)) {
                log.warn("No backup for shard: %s", shardUuid);
            }

            // pick target node
            String target = pickTargetNode(nodes, shardSize, averageSize);
            if (target == null) {
                return;
            }
            long targetSize = nodes.get(target);

            // stats
            log.info("Moving shard %s to node %s (shard: %s, node: %s, average: %s, target: %s)",
                    shardUuid, target, shardSize, nodeSize, averageSize, targetSize);
            shardsEjected.update(1);

            // update size
            nodes.put(target, targetSize + shardSize);
            nodeSize -= shardSize;

            // move assignment
            shardManager.assignShard(shard.getTableId(), shardUuid, target, false);
            shardManager.unassignShard(shard.getTableId(), shardUuid, currentNode);

            // delete local file
            File file = storageService.getStorageFile(shardUuid);
            if (file.exists() && !file.delete()) {
                log.warn("Failed to delete shard file: %s", file);
            }
        }
    }

    private static String pickTargetNode(Map<String, Long> nodes, long shardSize, long maxSize)
    {
        while (!nodes.isEmpty()) {
            String node = pickCandidateNode(nodes);
            if ((nodes.get(node) + shardSize) <= maxSize) {
                return node;
            }
            nodes.remove(node);
        }
        return null;
    }

    private static String pickCandidateNode(Map<String, Long> nodes)
    {
        checkArgument(!nodes.isEmpty());
        if (nodes.size() == 1) {
            return nodes.keySet().iterator().next();
        }

        // pick two random candidates, then choose the smaller one
        List<String> candidates = new ArrayList<>(nodes.keySet());
        Collections.shuffle(candidates);
        String first = candidates.get(0);
        String second = candidates.get(1);
        return (nodes.get(first) <= nodes.get(second)) ? first : second;
    }
}
