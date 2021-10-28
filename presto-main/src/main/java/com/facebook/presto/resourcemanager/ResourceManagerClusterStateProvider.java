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
package com.facebook.presto.resourcemanager;

import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.memory.ClusterMemoryPool;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.resourceOvercommit;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Stream.concat;

public class ResourceManagerClusterStateProvider
{
    private final Map<String, CoordinatorQueriesState> nodeQueryStates = new ConcurrentHashMap<>();
    private final Map<String, InternalNodeState> nodeStatuses = new ConcurrentHashMap<>();

    private final InternalNodeManager internalNodeManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final int maxCompletedQueries;
    private final Duration queryExpirationTimeout;
    private final Duration completedQueryExpirationTimeout;
    private final boolean isReservedPoolEnabled;
    private final Supplier<Map<MemoryPoolId, ClusterMemoryPoolInfo>> clusterMemoryPoolInfosSupplier;

    @Inject
    public ResourceManagerClusterStateProvider(
            InternalNodeManager internalNodeManager,
            SessionPropertyManager sessionPropertyManager,
            ResourceManagerConfig resourceManagerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            @ForResourceManager ScheduledExecutorService scheduledExecutorService)
    {
        this(
                requireNonNull(internalNodeManager, "internalNodeManager is null"),
                requireNonNull(sessionPropertyManager, "sessionPropertyManager is null"),
                requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getMaxCompletedQueries(),
                resourceManagerConfig.getQueryExpirationTimeout(),
                resourceManagerConfig.getCompletedQueryExpirationTimeout(),
                resourceManagerConfig.getNodeStatusTimeout(),
                resourceManagerConfig.getMemoryPoolInfoRefreshDuration(),
                requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").isReservedPoolEnabled(),
                requireNonNull(scheduledExecutorService, "scheduledExecutorService is null"));
    }

    public ResourceManagerClusterStateProvider(
            InternalNodeManager internalNodeManager,
            SessionPropertyManager sessionPropertyManager,
            int maxCompletedQueries,
            Duration queryExpirationTimeout,
            Duration completedQueryExpirationTimeout,
            Duration nodeStatusTimeout,
            Duration memoryPoolInfoRefreshDuration,
            boolean isReservedPoolEnabled,
            ScheduledExecutorService scheduledExecutorService)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        checkArgument(maxCompletedQueries > 0, "maxCompletedQueries must be > 0, was %s", maxCompletedQueries);
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.maxCompletedQueries = maxCompletedQueries;
        this.queryExpirationTimeout = requireNonNull(queryExpirationTimeout, "queryExpirationTimeout is null");
        this.completedQueryExpirationTimeout = requireNonNull(completedQueryExpirationTimeout, "completedQueryExpirationTimeout is null");
        // Memoized suppliers take in a time unit > 0
        requireNonNull(memoryPoolInfoRefreshDuration, "memoryPoolInfoRefreshDuration is null");
        if (memoryPoolInfoRefreshDuration.toMillis() > 0) {
            this.clusterMemoryPoolInfosSupplier = Suppliers.memoizeWithExpiration(this::getClusterMemoryPoolInfoInternal, memoryPoolInfoRefreshDuration.toMillis(), MILLISECONDS);
        }
        else {
            this.clusterMemoryPoolInfosSupplier = this::getClusterMemoryPoolInfoInternal;
        }
        this.isReservedPoolEnabled = isReservedPoolEnabled;

        requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            for (CoordinatorQueriesState coordinatorQueriesState : ImmutableList.copyOf(nodeQueryStates.values())) {
                coordinatorQueriesState.purgeExpiredQueries();
            }
        }, 100, 100, MILLISECONDS);

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, InternalNodeState> nodeEntry : ImmutableList.copyOf(nodeStatuses.entrySet())) {
                if ((System.currentTimeMillis() - nodeEntry.getValue().getLastHeartbeatInMillis()) > nodeStatusTimeout.toMillis()) {
                    nodeStatuses.remove(nodeEntry.getKey());
                }
            }
        }, 100, 100, MILLISECONDS);
    }

    public void registerQueryHeartbeat(String nodeId, BasicQueryInfo basicQueryInfo, long sequenceId)
    {
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(basicQueryInfo, "basicQueryInfo is null");
        Stream<InternalNode> activeOrShuttingDownCoordinators = concat(internalNodeManager.getCoordinators().stream(),
                internalNodeManager.getShuttingDownCoordinator().stream());
        checkArgument(
                activeOrShuttingDownCoordinators.anyMatch(i -> nodeId.equals(i.getNodeIdentifier())),
                "%s is not a coordinator (coordinators: %s)",
                nodeId,
                internalNodeManager.getCoordinators().stream().collect(toImmutableSet()));
        CoordinatorQueriesState state = nodeQueryStates.computeIfAbsent(nodeId, identifier -> new CoordinatorQueriesState(
                nodeId,
                maxCompletedQueries,
                queryExpirationTimeout.toMillis(),
                completedQueryExpirationTimeout.toMillis()));
        state.addOrUpdateQuery(basicQueryInfo, sequenceId);
    }

    public void registerNodeHeartbeat(NodeStatus nodeStatus)
    {
        requireNonNull(nodeStatus, "nodeStatus is null");
        InternalNodeState nodeState = nodeStatuses.get(nodeStatus.getNodeId());
        if (nodeState == null) {
            nodeStatuses.put(nodeStatus.getNodeId(), new InternalNodeState(nodeStatus));
        }
        else {
            nodeState.updateNodeStatus(nodeStatus);
        }
    }

    public List<ResourceGroupRuntimeInfo> getClusterResourceGroups(String excludingNode)
            throws ResourceManagerInconsistentException
    {
        requireNonNull(excludingNode, "excludingNode is null");
        validateCoordinatorConsistency();

        Map<ResourceGroupId, ResourceGroupRuntimeInfo.Builder> resourceGroupBuilders = new HashMap<>();
        nodeQueryStates.values().stream()
                .filter(state -> !state.getNodeId().equals(excludingNode))
                .map(CoordinatorQueriesState::getActiveQueries)
                .flatMap(Collection::stream)
                .map(Query::getBasicQueryInfo)
                .filter(info -> info.getResourceGroupId().isPresent())
                .forEach(info -> {
                    ResourceGroupId resourceGroupId = info.getResourceGroupId().get();
                    ResourceGroupRuntimeInfo.Builder builder = resourceGroupBuilders.computeIfAbsent(resourceGroupId, ResourceGroupRuntimeInfo::builder);
                    if (info.getState() == QUEUED) {
                        builder.addQueuedQueries(1);
                    }
                    else if (!info.getState().isDone()) {
                        builder.addRunningQueries(1);
                    }
                    builder.addUserMemoryReservationBytes(info.getQueryStats().getUserMemoryReservation().toBytes());
                    while (resourceGroupId.getParent().isPresent()) {
                        resourceGroupId = resourceGroupId.getParent().get();
                        ResourceGroupRuntimeInfo.Builder parentBuilder = resourceGroupBuilders.computeIfAbsent(resourceGroupId, ResourceGroupRuntimeInfo::builder);
                        if (info.getState() == QUEUED) {
                            parentBuilder.addDescendantQueuedQueries(1);
                        }
                        else if (!info.getState().isDone()) {
                            parentBuilder.addDescendantRunningQueries(1);
                        }
                    }
                });
        return resourceGroupBuilders.values().stream().map(ResourceGroupRuntimeInfo.Builder::build).collect(toImmutableList());
    }

    public List<BasicQueryInfo> getClusterQueries()
    {
        return ImmutableList.copyOf(nodeQueryStates.values()).stream()
                .map(CoordinatorQueriesState::getAllQueries)
                .flatMap(Collection::stream)
                .map(Query::getBasicQueryInfo)
                .collect(toImmutableList());
    }

    public Map<MemoryPoolId, ClusterMemoryPoolInfo> getClusterMemoryPoolInfo()
    {
        return clusterMemoryPoolInfosSupplier.get();
    }

    private Map<MemoryPoolId, ClusterMemoryPoolInfo> getClusterMemoryPoolInfoInternal()
    {
        List<MemoryInfo> memoryInfos = nodeStatuses.values().stream()
                .map(nodeStatus -> nodeStatus.getNodeStatus().getMemoryInfo())
                .collect(toImmutableList());

        int queriesAssignedToGeneralPool = 0;
        int queriesAssignedToReservedPool = 0;
        Query largestGeneralPoolQuery = null;
        for (CoordinatorQueriesState nodeQueryState : nodeQueryStates.values()) {
            for (Query query : nodeQueryState.getActiveQueries()) {
                MemoryPoolId memoryPool = query.getBasicQueryInfo().getMemoryPool();
                if (GENERAL_POOL.equals(memoryPool)) {
                    queriesAssignedToGeneralPool = Math.incrementExact(queriesAssignedToGeneralPool);
                    if (!resourceOvercommit(query.getBasicQueryInfo().getSession().toSession(sessionPropertyManager))) {
                        largestGeneralPoolQuery = getLargestMemoryQuery(Optional.ofNullable(largestGeneralPoolQuery), query);
                    }
                }
                else if (RESERVED_POOL.equals(memoryPool)) {
                    queriesAssignedToReservedPool = Math.incrementExact(queriesAssignedToReservedPool);
                }
                else {
                    throw new IllegalArgumentException("Unrecognized memory pool: " + memoryPool);
                }
            }
        }

        ImmutableMap.Builder<MemoryPoolId, ClusterMemoryPoolInfo> memoryPoolInfos = ImmutableMap.builder();
        ClusterMemoryPool pool = new ClusterMemoryPool(GENERAL_POOL);
        pool.update(memoryInfos, queriesAssignedToGeneralPool);
        ClusterMemoryPoolInfo clusterInfo = pool.getClusterInfo(Optional.ofNullable(largestGeneralPoolQuery).map(Query::getQueryId));
        memoryPoolInfos.put(GENERAL_POOL, clusterInfo);
        if (isReservedPoolEnabled) {
            pool = new ClusterMemoryPool(RESERVED_POOL);
            pool.update(memoryInfos, queriesAssignedToReservedPool);
            memoryPoolInfos.put(RESERVED_POOL, pool.getClusterInfo());
        }
        return memoryPoolInfos.build();
    }

    private Query getLargestMemoryQuery(Optional<Query> existingLargeQuery, Query newQuery)
    {
        requireNonNull(newQuery, "newQuery must not be null");
        return existingLargeQuery
                .map(largeQuery -> {
                    long largestGeneralBytes = largeQuery.getBasicQueryInfo().getQueryStats().getTotalMemoryReservation().toBytes();
                    long currentGeneralBytes = newQuery.getBasicQueryInfo().getQueryStats().getTotalMemoryReservation().toBytes();
                    if (currentGeneralBytes > largestGeneralBytes) {
                        return newQuery;
                    }
                    return largeQuery;
                })
                .orElse(newQuery);
    }

    public Map<String, MemoryInfo> getWorkerMemoryInfo()
    {
        return nodeStatuses.entrySet().stream().collect(toImmutableMap(e -> {
            String nodeIdentifier = e.getValue().getNodeStatus().getNodeId();
            String nodeHost = URI.create(e.getValue().getNodeStatus().getExternalAddress()).getHost();
            return nodeIdentifier + " [" + nodeHost + "]";
        }, e -> e.getValue().getNodeStatus().getMemoryInfo()));
    }

    private void validateCoordinatorConsistency()
    {
        Set<String> coordinators = internalNodeManager.getCoordinators().stream().map(InternalNode::getNodeIdentifier).collect(toImmutableSet());
        Set<String> heartbeatedCoordinatorNodes = nodeStatuses.values().stream()
                .map(InternalNodeState::getNodeStatus)
                .filter(NodeStatus::isCoordinator)
                .map(NodeStatus::getNodeId)
                .collect(toImmutableSet());
        if (!(Sets.difference(coordinators, heartbeatedCoordinatorNodes).isEmpty() && !coordinators.isEmpty())) {
            throw new ResourceManagerInconsistentException(format("%s nodes found in discovery vs. %s nodes found in heartbeats", coordinators.size(), heartbeatedCoordinatorNodes.size()));
        }
    }

    private static class CoordinatorQueriesState
    {
        private final String nodeId;
        private final int maxCompletedQueries;
        private final long queryExpirationTimeoutMillis;
        private final long completedQueryExpirationTimeoutMillis;

        @GuardedBy("this")
        private final Map<QueryId, Query> activeQueries = new HashMap<>();
        @GuardedBy("this")
        private final Map<QueryId, Query> completedQueries = new LinkedHashMap<>();

        public CoordinatorQueriesState(
                String nodeId,
                int maxCompletedQueries,
                long queryExpirationTimeoutMillis,
                long completedQueryExpirationTimeoutMillis)
        {
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            checkArgument(maxCompletedQueries > 0);
            checkArgument(queryExpirationTimeoutMillis > 0);
            checkArgument(completedQueryExpirationTimeoutMillis > 0);
            this.maxCompletedQueries = maxCompletedQueries;
            this.queryExpirationTimeoutMillis = queryExpirationTimeoutMillis;
            this.completedQueryExpirationTimeoutMillis = completedQueryExpirationTimeoutMillis;
        }

        public synchronized void addOrUpdateQuery(BasicQueryInfo basicQueryInfo, long sequenceId)
        {
            requireNonNull(basicQueryInfo, "basicQueryInfo is null");
            QueryId queryId = basicQueryInfo.getQueryId();
            Query query = activeQueries.get(queryId);
            if (query == null) {
                query = completedQueries.get(queryId);
            }
            if (query == null) {
                query = new Query(basicQueryInfo, sequenceId);
                activeQueries.put(queryId, query);
            }
            else {
                query = query.updateQueryInfo(basicQueryInfo, sequenceId);
            }
            if (isQueryCompleted(query)) {
                completedQueries.put(query.getQueryId(), query);
                activeQueries.remove(query.getQueryId());
            }
        }

        public synchronized void purgeExpiredQueries()
        {
            long currentTimeMillis = System.currentTimeMillis();

            Iterator<Query> queryIterator = activeQueries.values().iterator();
            while (queryIterator.hasNext()) {
                Query query = queryIterator.next();
                if (isQueryExpired(query, currentTimeMillis, queryExpirationTimeoutMillis)) {
                    completedQueries.put(query.getQueryId(), query);
                    queryIterator.remove();
                }
            }

            Iterator<Query> completedQueriesIterator = completedQueries.values().iterator();
            while (completedQueriesIterator.hasNext()) {
                Query query = completedQueriesIterator.next();
                if (completedQueries.size() <= maxCompletedQueries && !isQueryExpired(query, currentTimeMillis, completedQueryExpirationTimeoutMillis)) {
                    break;
                }
                completedQueriesIterator.remove();
            }
        }

        public String getNodeId()
        {
            return nodeId;
        }

        public synchronized List<Query> getActiveQueries()
        {
            return ImmutableList.copyOf(activeQueries.values());
        }

        public synchronized List<Query> getAllQueries()
        {
            purgeExpiredQueries();
            return ImmutableList.<Query>builder().addAll(activeQueries.values()).addAll(completedQueries.values()).build();
        }
    }

    private static final class InternalNodeState
    {
        private volatile NodeStatus nodeStatus;
        private final AtomicLong lastHeartbeatInMillis = new AtomicLong();

        private InternalNodeState(NodeStatus nodeStatus)
        {
            this.nodeStatus = nodeStatus;
            recordHeartbeat();
        }

        private void recordHeartbeat()
        {
            this.lastHeartbeatInMillis.set(System.currentTimeMillis());
        }

        public long getLastHeartbeatInMillis()
        {
            return lastHeartbeatInMillis.get();
        }

        public InternalNodeState updateNodeStatus(NodeStatus nodeStatus)
        {
            requireNonNull(nodeStatus, "nodeStatus is null");
            this.nodeStatus = nodeStatus;
            recordHeartbeat();
            return this;
        }

        public NodeStatus getNodeStatus()
        {
            return nodeStatus;
        }
    }

    public static class Query
    {
        private final QueryId queryId;

        private volatile BasicQueryInfo basicQueryInfo;
        private final AtomicLong lastHeartbeatInMillis = new AtomicLong();
        private final AtomicLong sequenceId;

        public Query(BasicQueryInfo basicQueryInfo, long sequenceId)
        {
            this.queryId = basicQueryInfo.getQueryId();
            this.basicQueryInfo = basicQueryInfo;
            this.sequenceId = new AtomicLong(sequenceId);
            recordHeartbeat();
        }

        private void recordHeartbeat()
        {
            this.lastHeartbeatInMillis.set(System.currentTimeMillis());
        }

        public long getLastHeartbeatInMillis()
        {
            return lastHeartbeatInMillis.get();
        }

        public Query updateQueryInfo(BasicQueryInfo basicQueryInfo, long sequenceId)
        {
            requireNonNull(basicQueryInfo, "basicQueryInfo is null");
            long newSequenceId = this.sequenceId.updateAndGet(operand -> Math.max(operand, sequenceId));
            if (newSequenceId == sequenceId) {
                this.basicQueryInfo = basicQueryInfo;
            }
            recordHeartbeat();
            return this;
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public BasicQueryInfo getBasicQueryInfo()
        {
            return basicQueryInfo;
        }
    }

    private static boolean isQueryExpired(Query query, long timeoutInMillis, long timeout)
    {
        return (timeoutInMillis - query.getLastHeartbeatInMillis()) > timeout;
    }

    private static boolean isQueryCompleted(Query query)
    {
        return query.getBasicQueryInfo().getState().isDone();
    }
}
