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

import com.facebook.presto.ExceededCpuLimitException;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.ObjectNames;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.presto.ExceededMemoryLimitException.exceededGlobalLimit;
import static com.facebook.presto.SystemSessionProperties.RESOURCE_OVERCOMMIT;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxCpuTime;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxMemory;
import static com.facebook.presto.SystemSessionProperties.resourceOvercommit;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.facebook.presto.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ClusterMemoryManager
        implements ClusterMemoryPoolManager
{
    private static final Logger log = Logger.get(ClusterMemoryManager.class);
    private final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor();
    private final InternalNodeManager nodeManager;
    private final LocationFactory locationFactory;
    private final HttpClient httpClient;
    private final MBeanExporter exporter;
    private final JsonCodec<MemoryInfo> memoryInfoCodec;
    private final JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec;
    private final DataSize maxQueryMemory;
    private final Duration maxQueryCpuTime;
    private final boolean enabled;
    private final boolean killOnOutOfMemory;
    private final Duration killOnOutOfMemoryDelay;
    private final String coordinatorId;
    private final AtomicLong memoryPoolAssignmentsVersion = new AtomicLong();
    private final AtomicLong clusterMemoryUsageBytes = new AtomicLong();
    private final AtomicLong clusterMemoryBytes = new AtomicLong();
    private final AtomicLong queriesKilledDueToOutOfMemory = new AtomicLong();
    private final Map<String, RemoteNodeMemory> nodes = new HashMap<>();

    @GuardedBy("this")
    private final Map<MemoryPoolId, List<Consumer<MemoryPoolInfo>>> changeListeners = new HashMap<>();

    @GuardedBy("this")
    private final Map<MemoryPoolId, ClusterMemoryPool> pools = new HashMap<>();

    @GuardedBy("this")
    private long lastTimeNotOutOfMemory = System.nanoTime();

    @GuardedBy("this")
    private QueryId lastKilledQuery;

    @Inject
    public ClusterMemoryManager(
            @ForMemoryManager HttpClient httpClient,
            InternalNodeManager nodeManager,
            LocationFactory locationFactory,
            MBeanExporter exporter,
            JsonCodec<MemoryInfo> memoryInfoCodec,
            JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec,
            QueryIdGenerator queryIdGenerator,
            ServerConfig serverConfig,
            MemoryManagerConfig config,
            QueryManagerConfig queryManagerConfig)
    {
        requireNonNull(config, "config is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.memoryInfoCodec = requireNonNull(memoryInfoCodec, "memoryInfoCodec is null");
        this.assignmentsRequestJsonCodec = requireNonNull(assignmentsRequestJsonCodec, "assignmentsRequestJsonCodec is null");
        this.maxQueryMemory = config.getMaxQueryMemory();
        this.maxQueryCpuTime = queryManagerConfig.getQueryMaxCpuTime();
        this.coordinatorId = queryIdGenerator.getCoordinatorId();
        this.enabled = serverConfig.isCoordinator();
        this.killOnOutOfMemoryDelay = config.getKillOnOutOfMemoryDelay();
        this.killOnOutOfMemory = config.isKillOnOutOfMemory();
    }

    @Override
    public synchronized void addChangeListener(MemoryPoolId poolId, Consumer<MemoryPoolInfo> listener)
    {
        changeListeners.computeIfAbsent(poolId, id -> new ArrayList<>()).add(listener);
    }

    public synchronized void process(Iterable<QueryExecution> queries)
    {
        if (!enabled) {
            return;
        }

        boolean outOfMemory = isClusterOutOfMemory();
        if (!outOfMemory) {
            lastTimeNotOutOfMemory = System.nanoTime();
        }

        boolean queryKilled = false;
        long totalBytes = 0;
        for (QueryExecution query : queries) {
            long bytes = query.getTotalMemoryReservation();
            DataSize sessionMaxQueryMemory = getQueryMaxMemory(query.getSession());
            long queryMemoryLimit = Math.min(maxQueryMemory.toBytes(), sessionMaxQueryMemory.toBytes());
            totalBytes += bytes;
            if (resourceOvercommit(query.getSession()) && outOfMemory) {
                // If a query has requested resource overcommit, only kill it if the cluster has run out of memory
                DataSize memory = succinctBytes(bytes);
                query.fail(new PrestoException(CLUSTER_OUT_OF_MEMORY,
                        format("The cluster is out of memory and %s=true, so this query was killed. It was using %s of memory", RESOURCE_OVERCOMMIT, memory)));
                queryKilled = true;
            }
            if (!resourceOvercommit(query.getSession()) && bytes > queryMemoryLimit) {
                DataSize maxMemory = succinctBytes(queryMemoryLimit);
                query.fail(exceededGlobalLimit(maxMemory));
                queryKilled = true;
            }
        }
        clusterMemoryUsageBytes.set(totalBytes);

        if (killOnOutOfMemory) {
            boolean shouldKillQuery = nanosSince(lastTimeNotOutOfMemory).compareTo(killOnOutOfMemoryDelay) > 0 && outOfMemory;
            boolean lastKilledQueryIsGone = (lastKilledQuery == null);

            if (!lastKilledQueryIsGone) {
                ClusterMemoryPool generalPool = pools.get(GENERAL_POOL);
                if (generalPool != null) {
                    lastKilledQueryIsGone = generalPool.getQueryMemoryReservations().containsKey(lastKilledQuery);
                }
            }

            if (shouldKillQuery && lastKilledQueryIsGone && !queryKilled) {
                // Kill the biggest query in the general pool
                QueryExecution biggestQuery = null;
                long maxMemory = -1;
                for (QueryExecution query : queries) {
                    long bytesUsed = query.getTotalMemoryReservation();
                    if (bytesUsed > maxMemory && query.getMemoryPool().getId().equals(GENERAL_POOL)) {
                        biggestQuery = query;
                        maxMemory = bytesUsed;
                    }
                }
                if (biggestQuery != null) {
                    biggestQuery.fail(new PrestoException(CLUSTER_OUT_OF_MEMORY, "The cluster is out of memory, and your query was killed. Please try again in a few minutes."));
                    queriesKilledDueToOutOfMemory.incrementAndGet();
                    lastKilledQuery = biggestQuery.getQueryId();
                }
            }
        }

        Map<MemoryPoolId, Integer> countByPool = new HashMap<>();
        for (QueryExecution query : queries) {
            MemoryPoolId id = query.getMemoryPool().getId();
            countByPool.put(id, countByPool.getOrDefault(id, 0) + 1);
        }

        updatePools(countByPool);

        updateNodes(updateAssignments(queries));

        // check if CPU usage is over limit
        for (QueryExecution query : queries) {
            Duration cpuTime = query.getTotalCpuTime();
            Duration sessionLimit = getQueryMaxCpuTime(query.getSession());
            Duration limit = maxQueryCpuTime.compareTo(sessionLimit) < 0 ? maxQueryCpuTime : sessionLimit;
            if (cpuTime.compareTo(limit) > 0) {
                query.fail(new ExceededCpuLimitException(limit));
            }
        }
    }

    @VisibleForTesting
    synchronized Map<MemoryPoolId, ClusterMemoryPool> getPools()
    {
        return ImmutableMap.copyOf(pools);
    }

    private synchronized boolean isClusterOutOfMemory()
    {
        ClusterMemoryPool reservedPool = pools.get(RESERVED_POOL);
        ClusterMemoryPool generalPool = pools.get(GENERAL_POOL);
        return reservedPool != null && generalPool != null && reservedPool.getAssignedQueries() > 0 && generalPool.getBlockedNodes() > 0;
    }

    private synchronized MemoryPoolAssignmentsRequest updateAssignments(Iterable<QueryExecution> queries)
    {
        ClusterMemoryPool reservedPool = pools.get(RESERVED_POOL);
        ClusterMemoryPool generalPool = pools.get(GENERAL_POOL);
        long version = memoryPoolAssignmentsVersion.incrementAndGet();
        // Check that all previous assignments have propagated to the visible nodes. This doesn't account for temporary network issues,
        // and is more of a safety check than a guarantee
        if (reservedPool != null && generalPool != null && allAssignmentsHavePropagated(queries)) {
            if (reservedPool.getAssignedQueries() == 0 && generalPool.getBlockedNodes() > 0) {
                QueryExecution biggestQuery = null;
                long maxMemory = -1;
                for (QueryExecution queryExecution : queries) {
                    if (resourceOvercommit(queryExecution.getSession())) {
                        // Don't promote queries that requested resource overcommit to the reserved pool,
                        // since their memory usage is unbounded.
                        continue;
                    }
                    long bytesUsed = queryExecution.getTotalMemoryReservation();
                    if (bytesUsed > maxMemory) {
                        biggestQuery = queryExecution;
                        maxMemory = bytesUsed;
                    }
                }
                if (biggestQuery != null) {
                    biggestQuery.setMemoryPool(new VersionedMemoryPoolId(RESERVED_POOL, version));
                }
            }
        }

        ImmutableList.Builder<MemoryPoolAssignment> assignments = ImmutableList.builder();
        for (QueryExecution queryExecution : queries) {
            assignments.add(new MemoryPoolAssignment(queryExecution.getQueryId(), queryExecution.getMemoryPool().getId()));
        }
        return new MemoryPoolAssignmentsRequest(coordinatorId, version, assignments.build());
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
        ImmutableSet.Builder<Node> builder = ImmutableSet.builder();
        Set<Node> aliveNodes = builder
                .addAll(nodeManager.getNodes(ACTIVE))
                .addAll(nodeManager.getNodes(SHUTTING_DOWN))
                .build();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(Node::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = ImmutableSet.copyOf(difference(nodes.keySet(), aliveNodeIds));
        nodes.keySet().removeAll(deadNodes);

        // Add new nodes
        for (Node node : aliveNodes) {
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

        // Make a copy to materialize the set difference
        Set<MemoryPoolId> removedPools = ImmutableSet.copyOf(difference(pools.keySet(), activePoolIds));
        for (MemoryPoolId removed : removedPools) {
            unexport(pools.get(removed));
            pools.remove(removed);
            if (changeListeners.containsKey(removed)) {
                for (Consumer<MemoryPoolInfo> listener : changeListeners.get(removed)) {
                    listenerExecutor.execute(() -> listener.accept(new MemoryPoolInfo(0, 0, ImmutableMap.of())));
                }
            }
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
            if (changeListeners.containsKey(id)) {
                MemoryPoolInfo info = pool.getInfo();
                for (Consumer<MemoryPoolInfo> listener : changeListeners.get(id)) {
                    listenerExecutor.execute(() -> listener.accept(info));
                }
            }
        }
    }

    @PreDestroy
    public synchronized void destroy()
    {
        try {
            for (ClusterMemoryPool pool : pools.values()) {
                unexport(pool);
            }
            pools.clear();
        }
        finally {
            listenerExecutor.shutdownNow();
        }
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

    @Managed
    public long getQueriesKilledDueToOutOfMemory()
    {
        return queriesKilledDueToOutOfMemory.get();
    }
}
