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
package com.facebook.presto.memory;

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.ObjectNames;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static java.util.Objects.requireNonNull;

public class ClusterMemoryManager
{
    private static final Logger log = Logger.get(ClusterMemoryManager.class);
    private final NodeManager nodeManager;
    private final LocationFactory locationFactory;
    private final HttpClient httpClient;
    private final MBeanExporter exporter;
    private final JsonCodec<MemoryInfo> memoryInfoCodec;
    private final JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec;
    private final DataSize maxQueryMemory;
    private final boolean enabled;
    private final AtomicLong memoryPoolAssignmentsVersion = new AtomicLong();
    private final AtomicLong clusterMemoryUsageBytes = new AtomicLong();
    private final AtomicLong clusterMemoryBytes = new AtomicLong();
    private final Map<String, RemoteNodeMemory> nodes = new HashMap<>();

    @GuardedBy("this")
    private final Map<MemoryPoolId, ClusterMemoryPool> pools = new HashMap<>();

    @Inject
    public ClusterMemoryManager(
            @ForMemoryManager HttpClient httpClient,
            NodeManager nodeManager,
            LocationFactory locationFactory,
            MBeanExporter exporter,
            JsonCodec<MemoryInfo> memoryInfoCodec,
            JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec,
            MemoryManagerConfig config)
    {
        requireNonNull(config, "config is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.memoryInfoCodec = requireNonNull(memoryInfoCodec, "memoryInfoCodec is null");
        this.assignmentsRequestJsonCodec = requireNonNull(assignmentsRequestJsonCodec, "assignmentsRequestJsonCodec is null");
        this.maxQueryMemory = config.getMaxQueryMemory();
        this.enabled = config.isClusterMemoryManagerEnabled();
    }

    public void process(Iterable<QueryExecution> queries)
    {
        if (!enabled) {
            return;
        }
        long totalBytes = 0;
        for (QueryExecution query : queries) {
            long bytes = query.getTotalMemoryReservation();
            totalBytes += bytes;
            if (bytes > maxQueryMemory.toBytes()) {
                query.fail(new ExceededMemoryLimitException("Query", maxQueryMemory));
            }
        }
        clusterMemoryUsageBytes.set(totalBytes);

        Map<MemoryPoolId, Integer> countByPool = new HashMap<>();
        for (QueryExecution query : queries) {
            MemoryPoolId id = query.getMemoryPool().getId();
            countByPool.put(id, countByPool.getOrDefault(id, 0) + 1);
        }

        updatePools(countByPool);

        updateNodes(updateAssignments(queries));
    }

    @VisibleForTesting
    synchronized Map<MemoryPoolId, ClusterMemoryPool> getPools()
    {
        return ImmutableMap.copyOf(pools);
    }

    private MemoryPoolAssignmentsRequest updateAssignments(Iterable<QueryExecution> queries)
    {
        ClusterMemoryPool reservedPool = pools.get(RESERVED_POOL);
        ClusterMemoryPool generalPool = pools.get(GENERAL_POOL);
        long version = memoryPoolAssignmentsVersion.incrementAndGet();
        // Check that all previous assignments have propagated to the visible nodes. This doesn't account for temporary network issues,
        // and is more of a safety check than a guarantee
        if (reservedPool != null && generalPool != null && allAssignmentsHavePropagated(queries)) {
            if (reservedPool.getQueries() == 0 && generalPool.getBlockedNodes() > 0) {
                QueryExecution biggestQuery = null;
                long maxMemory = -1;
                for (QueryExecution queryExecution : queries) {
                    long bytesUsed = queryExecution.getTotalMemoryReservation();
                    if (bytesUsed > maxMemory) {
                        biggestQuery = queryExecution;
                        maxMemory = bytesUsed;
                    }
                }
                for (QueryExecution queryExecution : queries) {
                    if (queryExecution.getQueryId().equals(biggestQuery.getQueryId())) {
                        queryExecution.setMemoryPool(new VersionedMemoryPoolId(RESERVED_POOL, version));
                    }
                }
            }
        }

        ImmutableList.Builder<MemoryPoolAssignment> assignments = ImmutableList.builder();
        for (QueryExecution queryExecution : queries) {
            assignments.add(new MemoryPoolAssignment(queryExecution.getQueryId(), queryExecution.getMemoryPool().getId()));
        }
        return new MemoryPoolAssignmentsRequest(version, assignments.build());
    }

    private boolean allAssignmentsHavePropagated(Iterable<QueryExecution> queries)
    {
        if (nodes.isEmpty()) {
            // Assignments can't have propagated, if there are no visible nodes.
            return false;
        }
        long newestAssignment = ImmutableList.copyOf(queries).stream()
                .map(QueryExecution::getMemoryPool)
                .mapToLong(VersionedMemoryPoolId::getVersion)
                .min()
                .orElse(-1);

        long mostOutOfDateNode = nodes.values().stream()
                .mapToLong(RemoteNodeMemory::getCurrentAssignmentVersion)
                .min()
                .orElse(Long.MAX_VALUE);

        return newestAssignment <= mostOutOfDateNode;
    }

    private void updateNodes(MemoryPoolAssignmentsRequest assignments)
    {
        Set<Node> activeNodes = nodeManager.getActiveNodes();
        ImmutableSet<String> activeNodeIds = activeNodes.stream()
                .map(Node::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        nodes.keySet().removeAll(difference(nodes.keySet(), activeNodeIds));

        // Add new nodes
        for (Node node : activeNodes) {
            if (!nodes.containsKey(node.getNodeIdentifier())) {
                nodes.put(node.getNodeIdentifier(), new RemoteNodeMemory(httpClient, memoryInfoCodec, assignmentsRequestJsonCodec, locationFactory.createMemoryInfoLocation(node)));
            }
        }

        // Schedule refresh
        for (RemoteNodeMemory node : nodes.values()) {
            node.asyncRefresh(assignments);
        }
    }

    private synchronized void updatePools(Map<MemoryPoolId, Integer> queryCounts)
    {
        // Update view of cluster memory and pools
        List<MemoryInfo> nodeMemoryInfos = nodes.values().stream()
                .map(RemoteNodeMemory::getInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        long totalClusterMemory = nodeMemoryInfos.stream()
                .map(MemoryInfo::getTotalNodeMemory)
                .mapToLong(DataSize::toBytes)
                .sum();
        clusterMemoryBytes.set(totalClusterMemory);

        Set<MemoryPoolId> activePoolIds = nodeMemoryInfos.stream()
                .flatMap(info -> info.getPools().keySet().stream())
                .collect(toImmutableSet());

        Set<MemoryPoolId> removedPools = difference(pools.keySet(), activePoolIds);
        for (MemoryPoolId removed : removedPools) {
            unexport(pools.get(removed));
            pools.remove(removed);
        }
        for (MemoryPoolId id : activePoolIds) {
            ClusterMemoryPool pool = pools.computeIfAbsent(id, poolId -> {
                ClusterMemoryPool newPool = new ClusterMemoryPool(poolId);
                String objectName = ObjectNames.builder(ClusterMemoryPool.class, newPool.getId().toString()).build();
                try {
                    exporter.export(objectName, newPool);
                }
                catch (JmxException e) {
                    log.error(e, "Error exporting memory pool %s", poolId);
                }
                return newPool;
            });
            pool.update(nodeMemoryInfos, queryCounts.getOrDefault(pool.getId(), 0));
        }
    }

    @PreDestroy
    public synchronized void destroy()
    {
        for (ClusterMemoryPool pool : pools.values()) {
            unexport(pool);
        }
        pools.clear();
    }

    private void unexport(ClusterMemoryPool pool)
    {
        try {
            String objectName = ObjectNames.builder(ClusterMemoryPool.class, pool.getId().toString()).build();
            exporter.unexport(objectName);
        }
        catch (JmxException e) {
            log.error(e, "Failed to unexport pool %s", pool.getId());
        }
    }

    @Managed
    public long getClusterMemoryUsageBytes()
    {
        return clusterMemoryUsageBytes.get();
    }

    @Managed
    public long getClusterMemoryBytes()
    {
        return clusterMemoryBytes.get();
    }
}
