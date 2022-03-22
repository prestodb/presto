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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryLimit;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.memory.LowMemoryKiller.QueryMemoryInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourcemanager.ClusterMemoryManagerService;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.io.Closer;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.facebook.presto.ExceededMemoryLimitException.exceededGlobalTotalLimit;
import static com.facebook.presto.ExceededMemoryLimitException.exceededGlobalUserLimit;
import static com.facebook.presto.SystemSessionProperties.RESOURCE_OVERCOMMIT;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxMemory;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxTotalMemory;
import static com.facebook.presto.SystemSessionProperties.resourceOvercommit;
import static com.facebook.presto.execution.QueryLimit.Source.QUERY;
import static com.facebook.presto.execution.QueryLimit.Source.RESOURCE_GROUP;
import static com.facebook.presto.execution.QueryLimit.Source.SYSTEM;
import static com.facebook.presto.execution.QueryLimit.createDataSizeLimit;
import static com.facebook.presto.execution.QueryLimit.getMinimum;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.facebook.presto.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Sets.difference;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.AbstractMap.SimpleEntry;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;

public class ClusterMemoryManager
        implements ClusterMemoryPoolManager
{
    private static final Logger log = Logger.get(ClusterMemoryManager.class);

    private final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor();
    private final ClusterMemoryLeakDetector memoryLeakDetector = new ClusterMemoryLeakDetector();
    private final InternalNodeManager nodeManager;
    private final LocationFactory locationFactory;
    private final Optional<ClusterMemoryManagerService> memoryManagerService;
    private final HttpClient httpClient;
    private final MBeanExporter exporter;
    private final Codec<MemoryInfo> memoryInfoCodec;
    private final Codec<MemoryPoolAssignmentsRequest> assignmentsRequestCodec;
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
    private final boolean isWorkScheduledOnCoordinator;
    private final boolean isBinaryTransportEnabled;

    @GuardedBy("this")
    private final Map<String, RemoteNodeMemory> nodes = new HashMap<>();

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
            Optional<ClusterMemoryManagerService> memoryManagerService,
            MBeanExporter exporter,
            JsonCodec<MemoryInfo> memoryInfoJsonCodec,
            SmileCodec<MemoryInfo> memoryInfoSmileCodec,
            JsonCodec<MemoryPoolAssignmentsRequest> assignmentsRequestJsonCodec,
            SmileCodec<MemoryPoolAssignmentsRequest> assignmentsRequestSmileCodec,
            QueryIdGenerator queryIdGenerator,
            LowMemoryKiller lowMemoryKiller,
            ServerConfig serverConfig,
            MemoryManagerConfig config,
            NodeMemoryConfig nodeMemoryConfig,
            NodeSchedulerConfig schedulerConfig,
            InternalCommunicationConfig communicationConfig)
    {
        requireNonNull(config, "config is null");
        requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null");
        requireNonNull(serverConfig, "serverConfig is null");
        requireNonNull(schedulerConfig, "schedulerConfig is null");
        requireNonNull(communicationConfig, "communicationConfig is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.memoryManagerService = requireNonNull(memoryManagerService, "memoryManagerService is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.lowMemoryKiller = requireNonNull(lowMemoryKiller, "lowMemoryKiller is null");
        this.maxQueryMemory = config.getMaxQueryMemory();
        this.maxQueryTotalMemory = config.getMaxQueryTotalMemory();
        this.coordinatorId = queryIdGenerator.getCoordinatorId();
        this.enabled = serverConfig.isCoordinator();
        this.killOnOutOfMemoryDelay = config.getKillOnOutOfMemoryDelay();
        this.isWorkScheduledOnCoordinator = schedulerConfig.isIncludeCoordinator();
        this.isBinaryTransportEnabled = communicationConfig.isBinaryTransportEnabled();
        if (this.isBinaryTransportEnabled) {
            this.memoryInfoCodec = requireNonNull(memoryInfoSmileCodec, "memoryInfoSmileCodec is null");
            this.assignmentsRequestCodec = requireNonNull(assignmentsRequestSmileCodec, "assignmentsRequestSmileCodec is null");
        }
        else {
            this.memoryInfoCodec = requireNonNull(memoryInfoJsonCodec, "memoryInfoJsonCodec is null");
            this.assignmentsRequestCodec = requireNonNull(assignmentsRequestJsonCodec, "assignmentsRequestJsonCodec is null");
        }

        verify(maxQueryMemory.toBytes() <= maxQueryTotalMemory.toBytes(),
                "maxQueryMemory cannot be greater than maxQueryTotalMemory");
        verify(config.getSoftMaxQueryMemory().toBytes() <= maxQueryMemory.toBytes(),
                "Soft max query memory cannot be greater than hard limit");
        verify(config.getSoftMaxQueryTotalMemory().toBytes() <= maxQueryTotalMemory.toBytes(),
                "Soft max query total memory cannot be greater than hard limit");

        this.pools = createClusterMemoryPools(nodeMemoryConfig.isReservedPoolEnabled());
    }

    private Map<MemoryPoolId, ClusterMemoryPool> createClusterMemoryPools(boolean reservedPoolEnabled)
    {
        Set<MemoryPoolId> memoryPools = new HashSet<>();
        memoryPools.add(GENERAL_POOL);
        if (reservedPoolEnabled) {
            memoryPools.add(RESERVED_POOL);
        }

        ImmutableMap.Builder<MemoryPoolId, ClusterMemoryPool> builder = ImmutableMap.builder();
        for (MemoryPoolId poolId : memoryPools) {
            ClusterMemoryPool pool = new ClusterMemoryPool(poolId);
            builder.put(poolId, pool);
            try {
                exporter.export(generatedNameOf(ClusterMemoryPool.class, poolId.toString()), pool);
            }
            catch (JmxException e) {
                log.error(e, "Error exporting memory pool %s", poolId);
            }
        }
        return builder.build();
    }

    @Override
    public synchronized void addChangeListener(MemoryPoolId poolId, Consumer<MemoryPoolInfo> listener)
    {
        verify(memoryPoolExists(poolId), "Memory pool does not exist: %s", poolId);
        changeListeners.computeIfAbsent(poolId, id -> new ArrayList<>()).add(listener);
    }

    public synchronized boolean memoryPoolExists(MemoryPoolId poolId)
    {
        return pools.containsKey(poolId);
    }

    public synchronized void process(Iterable<QueryExecution> runningQueries, Supplier<List<BasicQueryInfo>> allQueryInfoSupplier)
    {
        if (!enabled) {
            return;
        }

        // TODO revocable memory reservations can also leak and may need to be detected in the future
        // We are only concerned about the leaks in general pool.
        memoryLeakDetector.checkForMemoryLeaks(allQueryInfoSupplier, pools.get(GENERAL_POOL).getQueryMemoryReservations());

        boolean outOfMemory = isClusterOutOfMemory();
        if (!outOfMemory) {
            lastTimeNotOutOfMemory = System.nanoTime();
        }

        boolean queryKilled = false;
        long totalUserMemoryBytes = 0L;
        long totalMemoryBytes = 0L;
        for (QueryExecution query : runningQueries) {
            boolean resourceOvercommit = resourceOvercommit(query.getSession());
            long userMemoryReservation = query.getUserMemoryReservation().toBytes();
            long totalMemoryReservation = query.getTotalMemoryReservation().toBytes();

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
                QueryLimit<DataSize> queryTotalMemoryLimit = getMinimum(
                        createDataSizeLimit(maxQueryTotalMemory, SYSTEM),
                        query.getResourceGroupQueryLimits()
                                .flatMap(ResourceGroupQueryLimits::getTotalMemoryLimit)
                                .map(rgLimit -> createDataSizeLimit(rgLimit, RESOURCE_GROUP))
                                .orElse(null),
                        createDataSizeLimit(getQueryMaxTotalMemory(query.getSession()), QUERY));
                if (totalMemoryReservation > queryTotalMemoryLimit.getLimit().toBytes()) {
                    query.fail(exceededGlobalTotalLimit(queryTotalMemoryLimit.getLimit(), queryTotalMemoryLimit.getLimitSource().name()));
                    queryKilled = true;
                }
            }
            totalUserMemoryBytes += userMemoryReservation;
            totalMemoryBytes += totalMemoryReservation;
        }

        clusterUserMemoryReservation.set(totalUserMemoryBytes);
        clusterTotalMemoryReservation.set(totalMemoryBytes);

        boolean killOnOomDelayPassed = nanosSince(lastTimeNotOutOfMemory).compareTo(killOnOutOfMemoryDelay) > 0;
        boolean lastKilledQueryGone = isLastKilledQueryGone();
        boolean shouldCallOomKiller = !(lowMemoryKiller instanceof NoneLowMemoryKiller) &&
                outOfMemory &&
                !queryKilled &&
                killOnOomDelayPassed &&
                lastKilledQueryGone;

        if (shouldCallOomKiller) {
            callOomKiller(runningQueries);
        }
        else {
            // if the cluster is out of memory and we didn't trigger the oom killer we log the state to make debugging easier
            if (outOfMemory) {
                log.debug("The cluster is out of memory and the OOM killer is not called (query killed: %s, kill on OOM delay passed: %s, last killed query gone: %s).",
                        queryKilled,
                        killOnOomDelayPassed,
                        lastKilledQueryGone);
            }
        }

        Map<MemoryPoolId, Integer> countByPool = new HashMap<>();
        for (QueryExecution query : runningQueries) {
            MemoryPoolId id = query.getMemoryPool().getId();
            countByPool.put(id, countByPool.getOrDefault(id, 0) + 1);
        }

        updatePools(countByPool);

        MemoryPoolAssignmentsRequest assignmentsRequest;
        if (pools.containsKey(RESERVED_POOL)) {
            assignmentsRequest = updateAssignments(runningQueries);
        }
        else {
            // If reserved pool is not enabled, we don't create a MemoryPoolAssignmentsRequest that puts all the queries
            // in the general pool (as they already are). In this case we create an effectively NOOP MemoryPoolAssignmentsRequest.
            // Once the reserved pool is removed we should get rid of the logic of putting queries into reserved pool including
            // this piece of code.
            assignmentsRequest = new MemoryPoolAssignmentsRequest(coordinatorId, Long.MIN_VALUE, ImmutableList.of());
        }
        updateNodes(assignmentsRequest);
    }

    private synchronized void callOomKiller(Iterable<QueryExecution> runningQueries)
    {
        List<QueryMemoryInfo> queryMemoryInfoList = Streams.stream(runningQueries)
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
            Optional<QueryExecution> chosenQuery = Streams.stream(runningQueries).filter(query -> chosenQueryId.get().equals(query.getQueryId())).collect(toOptional());
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

        // If the lastKilledQuery is marked as leaked by the ClusterMemoryLeakDetector we consider the lastKilledQuery as gone,
        // so that the ClusterMemoryManager can continue to make progress even if there are leaks.
        // Even if the weak references to the leaked queries are GCed in the ClusterMemoryLeakDetector, it will mark the same queries
        // as leaked in its next run, and eventually the ClusterMemoryManager will make progress.
        if (memoryLeakDetector.wasQueryPossiblyLeaked(lastKilledQuery)) {
            lastKilledQuery = null;
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
        Comparator<Entry<MemoryPoolInfo, Long>> nodeMemoryComparator = comparingLong(Entry::getValue);
        nodes.stream()
                .filter(node -> node.getPools().get(GENERAL_POOL) != null)
                .map(node ->
                        new SimpleEntry<MemoryPoolInfo, Long>(
                                node.getPools().get(GENERAL_POOL),
                                node.getPools().get(GENERAL_POOL).getQueryMemoryReservations().values().stream().mapToLong(l -> l).sum()))
                .sorted(nodeMemoryComparator.reversed())
                .map(Entry::getKey)
                .forEachOrdered(memoryPoolInfo -> {
                    nodeDescription.append("Query Kill Scenario: ");
                    nodeDescription.append("MaxBytes ").append(memoryPoolInfo.getMaxBytes()).append(' ');
                    nodeDescription.append("FreeBytes ").append(memoryPoolInfo.getFreeBytes() + memoryPoolInfo.getReservedRevocableBytes()).append(' ');
                    nodeDescription.append("Queries ");
                    Comparator<Entry<QueryId, Long>> queryMemoryComparator = comparingLong(Entry::getValue);
                    Stream<Entry<QueryId, Long>> sortedMemoryReservations =
                            memoryPoolInfo.getQueryMemoryReservations().entrySet().stream()
                                    .sorted(queryMemoryComparator.reversed());
                    Joiner.on(",").withKeyValueSeparator("=").appendTo(nodeDescription, (Iterable<Entry<QueryId, Long>>) sortedMemoryReservations::iterator);
                    nodeDescription.append('\n');
                });
        log.info(nodeDescription.toString());
    }

    @VisibleForTesting
    synchronized Map<MemoryPoolId, ClusterMemoryPool> getPools()
    {
        return ImmutableMap.copyOf(pools);
    }

    public synchronized Map<MemoryPoolId, MemoryPoolInfo> getMemoryPoolInfo()
    {
        ImmutableMap.Builder<MemoryPoolId, MemoryPoolInfo> builder = new ImmutableMap.Builder<>();
        pools.forEach((poolId, memoryPool) -> builder.put(poolId, memoryPool.getInfo()));
        return builder.build();
    }

    private synchronized boolean isClusterOutOfMemory()
    {
        ClusterMemoryPoolInfo reservedPool = getClusterInfo(RESERVED_POOL);
        ClusterMemoryPoolInfo generalPool = getClusterInfo(GENERAL_POOL);
        if (reservedPool == null) {
            return generalPool.getBlockedNodes() > 0;
        }
        return reservedPool.getAssignedQueries() > 0 && generalPool.getBlockedNodes() > 0;
    }

    // TODO once the reserved pool is removed we can remove this method. We can also update
    // RemoteNodeMemory as we don't need to POST anything.
    private synchronized MemoryPoolAssignmentsRequest updateAssignments(Iterable<QueryExecution> queries)
    {
        ClusterMemoryPoolInfo reservedPool = getClusterInfo(RESERVED_POOL);
        ClusterMemoryPoolInfo generalPool = getClusterInfo(GENERAL_POOL);
        verify(generalPool != null, "generalPool is null");
        verify(reservedPool != null, "reservedPool is null");
        long version = memoryPoolAssignmentsVersion.incrementAndGet();
        // Check that all previous assignments have propagated to the visible nodes. This doesn't account for temporary network issues,
        // and is more of a safety check than a guarantee
        if (allAssignmentsHavePropagated(queries)) {
            if (reservedPool.getAssignedQueries() == 0 && generalPool.getBlockedNodes() > 0) {
                QueryExecution biggestQuery = findLargestMemoryQuery(generalPool, queries);
                if (biggestQuery != null) {
                    log.info("Moving query %s to the reserved pool", biggestQuery.getQueryId());
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

    private QueryExecution findLargestMemoryQuery(ClusterMemoryPoolInfo generalPool, Iterable<QueryExecution> queries)
    {
        QueryExecution biggestQuery = null;
        long maxMemory = -1;
        Optional<QueryId> largestMemoryQuery = generalPool.getLargestMemoryQuery();
        // If present, this means the resource manager is determining the largest query, so do not make this determination locally
        if (memoryManagerService.isPresent()) {
            return largestMemoryQuery.flatMap(largestMemoryQueryId -> Streams.stream(queries)
                    .filter(query -> query.getQueryId().equals(largestMemoryQueryId))
                    .findFirst())
                    .orElse(null);
        }
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
        return biggestQuery;
    }

    private QueryMemoryInfo createQueryMemoryInfo(QueryExecution query)
    {
        return new QueryMemoryInfo(query.getQueryId(), query.getMemoryPool().getId(), query.getTotalMemoryReservation().toBytes());
    }

    private long getQueryMemoryReservation(QueryExecution query)
    {
        return query.getTotalMemoryReservation().toBytes();
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
        ImmutableSet.Builder<InternalNode> builder = ImmutableSet.builder();
        Set<InternalNode> aliveNodes = builder
                .addAll(nodeManager.getNodes(ACTIVE))
                .addAll(nodeManager.getNodes(SHUTTING_DOWN))
                .build();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = ImmutableSet.copyOf(difference(nodes.keySet(), aliveNodeIds));
        nodes.keySet().removeAll(deadNodes);

        // Add new nodes
        for (InternalNode node : aliveNodes) {
            if (!nodes.containsKey(node.getNodeIdentifier())) {
                nodes.put(
                        node.getNodeIdentifier(),
                        new RemoteNodeMemory(
                                node,
                                httpClient,
                                memoryInfoCodec,
                                assignmentsRequestCodec,
                                locationFactory.createMemoryInfoLocation(node),
                                isBinaryTransportEnabled));
            }
        }

        // If work isn't scheduled on the coordinator (the current node) there is no point
        // in polling or updating (when moving queries to the reserved pool) its memory pools
        if (!isWorkScheduledOnCoordinator) {
            nodes.remove(nodeManager.getCurrentNode().getNodeIdentifier());
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
                MemoryPoolInfo info = getClusterInfo(pool.getId()).getMemoryPoolInfo();
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
            String workerId = entry.getKey() + " [" + entry.getValue().getNode().getHost() + "]";
            memoryInfo.put(workerId, entry.getValue().getInfo());
        }
        return memoryInfo;
    }

    @VisibleForTesting
    synchronized ClusterMemoryPoolInfo getClusterInfo(MemoryPoolId poolId)
    {
        return memoryManagerService
                .map(service -> service.getMemoryPoolInfo().get(poolId))
                .orElseGet(() -> {
                    ClusterMemoryPool clusterMemoryPool = pools.get(poolId);
                    return clusterMemoryPool != null ? clusterMemoryPool.getClusterInfo() : null;
                });
    }

    @PreDestroy
    public synchronized void destroy()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            for (ClusterMemoryPool pool : pools.values()) {
                closer.register(() -> exporter.unexport(generatedNameOf(ClusterMemoryPool.class, pool.getId().toString())));
            }
            closer.register(listenerExecutor::shutdownNow);
        }
    }

    @Managed
    public int getNumberOfLeakedQueries()
    {
        return memoryLeakDetector.getNumberOfLeakedQueries();
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
