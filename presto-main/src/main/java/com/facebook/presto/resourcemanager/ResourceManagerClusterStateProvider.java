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
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class ResourceManagerClusterStateProvider
{
    private final Map<String, CoordinatorQueriesState> nodeQueryStates = new ConcurrentHashMap<>();
    private final Map<String, InternalNodeState> nodeStatuses = new ConcurrentHashMap<>();

    private final InternalNodeManager internalNodeManager;
    private final int maxCompletedQueries;
    private final Duration queryExpirationTimeout;
    private final Duration completedQueryExpirationTimeout;
    private final boolean isReservedPoolEnabled;
    private final Supplier<Map<MemoryPoolId, ClusterMemoryPoolInfo>> clusterMemoryPoolInfosSupplier;

    @Inject
    public ResourceManagerClusterStateProvider(
            InternalNodeManager internalNodeManager,
            ResourceManagerConfig resourceManagerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            @ForResourceManager ScheduledExecutorService scheduledExecutorService)
    {
        this(
                requireNonNull(internalNodeManager, "internalNodeManager is null"),
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

    public void registerQueryHeartbeat(String nodeId, BasicQueryInfo basicQueryInfo)
    {
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(basicQueryInfo, "basicQueryInfo is null");
        checkArgument(
                internalNodeManager.getCoordinators().stream().anyMatch(i -> nodeId.equals(i.getNodeIdentifier())),
                "%s is not a coordinator",
                nodeId);
        CoordinatorQueriesState state = nodeQueryStates.computeIfAbsent(nodeId, identifier -> new CoordinatorQueriesState(
                nodeId,
                maxCompletedQueries,
                queryExpirationTimeout.toMillis(),
                completedQueryExpirationTimeout.toMillis()));
        state.addOrUpdateQuery(basicQueryInfo);
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
    {
        requireNonNull(excludingNode, "excludingNode is null");
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

        Map<MemoryPoolId, Long> counts = nodeQueryStates.values().stream()
                .map(CoordinatorQueriesState::getActiveQueries)
                .flatMap(Collection::stream)
                .collect(groupingBy(query -> query.getBasicQueryInfo().getMemoryPool(), counting()));
        // Add defaults when queries are not running
        counts.putIfAbsent(GENERAL_POOL, 0L);
        if (isReservedPoolEnabled) {
            counts.putIfAbsent(RESERVED_POOL, 0L);
        }

        return counts.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> {
            ClusterMemoryPool pool = new ClusterMemoryPool(entry.getKey());
            pool.update(memoryInfos, toIntExact(entry.getValue()));
            return pool.getClusterInfo();
        }));
    }

    public Map<String, MemoryInfo> getWorkerMemoryInfo()
    {
        return nodeStatuses.entrySet().stream().collect(toImmutableMap(e -> {
            String nodeIdentifier = e.getValue().getNodeStatus().getNodeId();
            String nodeHost = URI.create(e.getValue().getNodeStatus().getExternalAddress()).getHost();
            return nodeIdentifier + " [" + nodeHost + "]";
        }, e -> e.getValue().getNodeStatus().getMemoryInfo()));
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

        public synchronized void addOrUpdateQuery(BasicQueryInfo basicQueryInfo)
        {
            requireNonNull(basicQueryInfo, "basicQueryInfo is null");
            QueryId queryId = basicQueryInfo.getQueryId();
            Query query = activeQueries.get(queryId);
            if (query == null) {
                query = new Query(basicQueryInfo);
                activeQueries.put(queryId, query);
            }
            else {
                query = query.updateQueryInfo(basicQueryInfo);
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

        public Query(BasicQueryInfo basicQueryInfo)
        {
            this.queryId = basicQueryInfo.getQueryId();
            this.basicQueryInfo = basicQueryInfo;
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

        public Query updateQueryInfo(BasicQueryInfo basicQueryInfo)
        {
            requireNonNull(basicQueryInfo, "basicQueryInfo is null");
            if (basicQueryInfo.getState().getValue() >= this.basicQueryInfo.getState().getValue()) {
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
