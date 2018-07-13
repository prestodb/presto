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

import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.memory.LowMemoryKiller.QueryMemoryInfo;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.facebook.presto.ExceededMemoryLimitException.exceededGlobalTotalLimit;
import static com.facebook.presto.ExceededMemoryLimitException.exceededGlobalUserLimit;
import static com.facebook.presto.SystemSessionProperties.RESOURCE_OVERCOMMIT;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxMemory;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxTotalMemory;
import static com.facebook.presto.SystemSessionProperties.resourceOvercommit;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.SYSTEM_POOL;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.facebook.presto.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_MEMORY_LIMIT;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Sets.difference;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ClusterMemoryManager
        implements ClusterMemoryPoolManager
{
    private static final Set<MemoryPoolId> POOLS = ImmutableSet.of(GENERAL_POOL, RESERVED_POOL, SYSTEM_POOL);

    private static final Logger log = Logger.get(ClusterMemoryManager.class);

    private final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor();
    private final InternalNodeManager nodeManager;
    private final LocationFactory locationFactory;
    private final HttpClient httpClient;
    private final MBeanExporter exporter;
    private final JsonCodec<MemoryInfo> memoryInfoCodec;
    private final JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec;
    private final DataSize maxQueryMemory;
    private final DataSize maxQueryTotalMemory;
    private final boolean enabled;
    private final LowMemoryKiller lowMemoryKiller;
    private final Duration killOnOutOfMemoryDelay;
    private final String coordinatorId;
    private final AtomicLong memoryPoolAssignmentsVersion = new AtomicLong();
    private final AtomicLong clusterUserMemoryReservation = new AtomicLong();
    private final AtomicLong clusterTotalMemoryReservation = new AtomicLong();
    private final AtomicLong clusterMemoryBytes = new AtomicLong();
    private final AtomicLong queriesKilledDueToOutOfMemory = new AtomicLong();

    private final Map<QueryId, Long> preAllocations = new HashMap<>();
    private final Map<QueryId, Long> preAllocationsConsumed = new HashMap<>();

    @GuardedBy("this")
    private final Map<String, RemoteNodeMemory> nodes = new HashMap<>();

    //TODO remove when the system pool is completely removed
    private final boolean isLegacySystemPoolEnabled;

    @GuardedBy("this")
    private final Map<MemoryPoolId, List<Consumer<MemoryPoolInfo>>> changeListeners = new HashMap<>();

    @GuardedBy("this")
    private final Map<MemoryPoolId, ClusterMemoryPool> pools;

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
            LowMemoryKiller lowMemoryKiller,
            ServerConfig serverConfig,
            MemoryManagerConfig config,
            NodeMemoryConfig nodeMemoryConfig)
    {
        requireNonNull(config, "config is null");
        requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null");
        requireNonNull(serverConfig, "serverConfig is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.memoryInfoCodec = requireNonNull(memoryInfoCodec, "memoryInfoCodec is null");
        this.assignmentsRequestJsonCodec = requireNonNull(assignmentsRequestJsonCodec, "assignmentsRequestJsonCodec is null");
        this.lowMemoryKiller = requireNonNull(lowMemoryKiller, "lowMemoryKiller is null");
        this.maxQueryMemory = config.getMaxQueryMemory();
        this.maxQueryTotalMemory = config.getMaxQueryTotalMemory();
        this.coordinatorId = queryIdGenerator.getCoordinatorId();
        this.enabled = serverConfig.isCoordinator();
        this.killOnOutOfMemoryDelay = config.getKillOnOutOfMemoryDelay();
        this.isLegacySystemPoolEnabled = nodeMemoryConfig.isLegacySystemPoolEnabled();

        verify(maxQueryMemory.toBytes() <= maxQueryTotalMemory.toBytes(),
                "maxQueryMemory cannot be greater than maxQueryTotalMemory");

        ImmutableMap.Builder<MemoryPoolId, ClusterMemoryPool> builder = ImmutableMap.builder();
        for (MemoryPoolId poolId : POOLS) {
            ClusterMemoryPool pool = new ClusterMemoryPool(poolId);
            builder.put(poolId, pool);
            String objectName = ObjectNames.builder(ClusterMemoryPool.class, poolId.toString()).build();
            try {
                exporter.export(objectName, pool);
            }
            catch (JmxException e) {
                log.error(e, "Error exporting memory pool %s", poolId);
            }
        }
        this.pools = builder.build();
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

        preAllocationsConsumed.clear();

        boolean queryKilled = false;
        long totalUserMemoryBytes = 0L;
        long totalMemoryBytes = 0L;
        for (QueryExecution query : queries) {
            boolean resourceOvercommit = resourceOvercommit(query.getSession());
            long userMemoryReservation = query.getUserMemoryReservation();
            long totalMemoryReservation = query.getTotalMemoryReservation();

            if (resourceOvercommit && outOfMemory) {
                // If a query has requested resource overcommit, only kill it if the cluster has run out of memory
                DataSize memory = succinctBytes(getQueryMemoryReservation(query));
                query.fail(new PrestoException(CLUSTER_OUT_OF_MEMORY,
                        format("The cluster is out of memory and %s=true, so this query was killed. It was using %s of memory", RESOURCE_OVERCOMMIT, memory)));
                queryKilled = true;
            }

            if (!resourceOvercommit) {
                long userMemoryLimit = min(maxQueryMemory.toBytes(), getQueryMaxMemory(query.getSession()).toBytes());
                if (userMemoryReservation > userMemoryLimit) {
                    query.fail(exceededGlobalUserLimit(succinctBytes(userMemoryLimit)));
                    queryKilled = true;
                }

                // enforce global total memory limit if system pool is disabled
                long totalMemoryLimit = min(maxQueryTotalMemory.toBytes(), getQueryMaxTotalMemory(query.getSession()).toBytes());
                if (!isLegacySystemPoolEnabled && totalMemoryReservation > totalMemoryLimit) {
                    query.fail(exceededGlobalTotalLimit(succinctBytes(totalMemoryLimit)));
                    queryKilled = true;
                }
            }

            if (preAllocations.containsKey(query.getQueryId())) {
                preAllocationsConsumed.put(query.getQueryId(), userMemoryReservation);
            }

            totalUserMemoryBytes += userMemoryReservation;
            totalMemoryBytes += totalMemoryReservation;
        }

        clusterUserMemoryReservation.set(totalUserMemoryBytes);
        clusterTotalMemoryReservation.set(totalMemoryBytes);

        if (!(lowMemoryKiller instanceof NoneLowMemoryKiller) &&
                outOfMemory &&
                !queryKilled &&
                nanosSince(lastTimeNotOutOfMemory).compareTo(killOnOutOfMemoryDelay) > 0) {
            boolean lastKilledQueryGone = isLastKilledQueryGone();
            if (lastKilledQueryGone) {
                callOomKiller(queries);
            }
            else {
                log.debug("Last killed query is still not gone: %s", lastKilledQuery);
            }
        }

        Map<MemoryPoolId, Integer> countByPool = new HashMap<>();
        for (QueryExecution query : queries) {
            MemoryPoolId id = query.getMemoryPool().getId();
            countByPool.put(id, countByPool.getOrDefault(id, 0) + 1);
        }

        updatePools(countByPool);

        updateNodes(updateAssignments(queries));
    }

    private synchronized void callOomKiller(Iterable<QueryExecution> queries)
    {
        List<QueryMemoryInfo> queryMemoryInfoList = Streams.stream(queries)
                .map(this::createQueryMemoryInfo)
                .collect(toImmutableList());
        List<MemoryInfo> nodeMemoryInfos = nodes.values().stream()
                .map(RemoteNodeMemory::getInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        Optional<QueryId> chosenQueryId = lowMemoryKiller.chooseQueryToKill(queryMemoryInfoList, nodeMemoryInfos);
        if (chosenQueryId.isPresent()) {
            log.debug("Low memory killer chose %s", chosenQueryId.get());
            Optional<QueryExecution> chosenQuery = Streams.stream(queries).filter(query -> chosenQueryId.get().equals(query.getQueryId())).collect(toOptional());
            if (chosenQuery.isPresent()) {
                // See comments in  isLastKilledQueryGone for why chosenQuery might be absent.
                chosenQuery.get().fail(new PrestoException(CLUSTER_OUT_OF_MEMORY, "Query killed because the cluster is out of memory. Please try again in a few minutes."));
                queriesKilledDueToOutOfMemory.incrementAndGet();
                lastKilledQuery = chosenQueryId.get();
                logQueryKill(chosenQueryId.get(), nodeMemoryInfos);
            }
        }
    }

    @GuardedBy("this")
    private boolean isLastKilledQueryGone()
    {
        if (lastKilledQuery == null) {
            return true;
        }
        // pools fields is updated based on nodes field.
        // Therefore, if the query is gone from pools field, it should also be gone from nodes field.
        // However, since nodes can updated asynchronously, it has the potential of coming back after being gone.
        // Therefore, even if the query appears to be gone here, it might be back when one inspects nodes later.
        return !pools.get(GENERAL_POOL)
                .getQueryMemoryReservations()
                .containsKey(lastKilledQuery);
    }

    private void logQueryKill(QueryId killedQueryId, List<MemoryInfo> nodes)
    {
        if (!log.isInfoEnabled()) {
            return;
        }
        StringBuilder nodeDescription = new StringBuilder();
        nodeDescription.append("Query Kill Decision: Killed ").append(killedQueryId).append("\n");
        for (MemoryInfo node : nodes) {
            MemoryPoolInfo memoryPoolInfo = node.getPools().get(GENERAL_POOL);
            if (memoryPoolInfo == null) {
                continue;
            }
            nodeDescription.append("Query Kill Scenario: ");
            nodeDescription.append("MaxBytes ").append(memoryPoolInfo.getMaxBytes()).append(' ');
            nodeDescription.append("FreeBytes ").append(memoryPoolInfo.getFreeBytes() + memoryPoolInfo.getReservedRevocableBytes()).append(' ');
            nodeDescription.append("Queries ");
            Joiner.on(",").withKeyValueSeparator("=").appendTo(nodeDescription, memoryPoolInfo.getQueryMemoryReservations());
            nodeDescription.append('\n');
        }
        log.info(nodeDescription.toString());
    }

    public synchronized boolean preAllocateQueryMemory(QueryId queryId, long requiredBytes)
    {
        if (requiredBytes > maxQueryMemory.toBytes()) {
            throw new PrestoException(EXCEEDED_MEMORY_LIMIT, format("Cannot pre-allocate memory, exceeds maximum limit %s", maxQueryMemory));
        }

        ClusterMemoryPool generalPool = pools.get(GENERAL_POOL);
        ClusterMemoryPool reservedPool = pools.get(RESERVED_POOL);
        if (generalPool.getBlockedNodes() > 0 || reservedPool.getAssignedQueries() > 0) {
            return false;
        }

        long totalPreAllocation = preAllocations.values().stream()
                .mapToLong(Long::longValue)
                .sum();

        long totalPreAllocationConsumed = preAllocationsConsumed.values().stream()
                .mapToLong(Long::longValue)
                .sum();

        if (generalPool.getFreeDistributedBytes() - (totalPreAllocation - totalPreAllocationConsumed) >= requiredBytes) {
            preAllocations.put(queryId, requiredBytes);
            return true;
        }

        return false;
    }

    public synchronized void removePreAllocation(QueryId queryId)
    {
        preAllocations.remove(queryId);
        MemoryPoolInfo info = pools.get(GENERAL_POOL).getInfo();
        for (Consumer<MemoryPoolInfo> listener : changeListeners.get(GENERAL_POOL)) {
            listenerExecutor.execute(() -> listener.accept(info));
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

                    long bytesUsed = getQueryMemoryReservation(queryExecution);
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

    private QueryMemoryInfo createQueryMemoryInfo(QueryExecution query)
    {
        // when the legacy system pool is enabled we use the user memory instead of the total memory
        if (isLegacySystemPoolEnabled) {
            return new QueryMemoryInfo(query.getQueryId(), query.getMemoryPool().getId(), query.getUserMemoryReservation());
        }
        return new QueryMemoryInfo(query.getQueryId(), query.getMemoryPool().getId(), query.getTotalMemoryReservation());
    }

    private long getQueryMemoryReservation(QueryExecution query)
    {
        // when the legacy system pool is enabled we use the user memory instead of the total memory
        if (isLegacySystemPoolEnabled) {
            return query.getUserMemoryReservation();
        }
        return query.getTotalMemoryReservation();
    }

    private synchronized boolean allAssignmentsHavePropagated(Iterable<QueryExecution> queries)
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

    private synchronized void updateNodes(MemoryPoolAssignmentsRequest assignments)
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
                nodes.put(node.getNodeIdentifier(), new RemoteNodeMemory(node, httpClient, memoryInfoCodec, assignmentsRequestJsonCodec, locationFactory.createMemoryInfoLocation(node)));
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

        for (ClusterMemoryPool pool : pools.values()) {
            pool.update(nodeMemoryInfos, queryCounts.getOrDefault(pool.getId(), 0));
            if (changeListeners.containsKey(pool.getId())) {
                MemoryPoolInfo info = pool.getInfo();
                for (Consumer<MemoryPoolInfo> listener : changeListeners.get(pool.getId())) {
                    listenerExecutor.execute(() -> listener.accept(info));
                }
            }
        }
    }

    public synchronized Map<String, Optional<MemoryInfo>> getWorkerMemoryInfo()
    {
        Map<String, Optional<MemoryInfo>> memoryInfo = new HashMap<>();
        for (Entry<String, RemoteNodeMemory> entry : nodes.entrySet()) {
            // workerId is of the form "node_identifier [node_host]"
            String workerId = entry.getKey() + " [" + entry.getValue().getNode().getHostAndPort().getHostText() + "]";
            memoryInfo.put(workerId, entry.getValue().getInfo());
        }
        return memoryInfo;
    }

    @PreDestroy
    public synchronized void destroy()
    {
        try {
            for (ClusterMemoryPool pool : pools.values()) {
                unexport(pool);
            }
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
    public long getClusterUserMemoryReservation()
    {
        return clusterUserMemoryReservation.get();
    }

    @Managed
    public long getClusterTotalMemoryReservation()
    {
        return clusterTotalMemoryReservation.get();
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
