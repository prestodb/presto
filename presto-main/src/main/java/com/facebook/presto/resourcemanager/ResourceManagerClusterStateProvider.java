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
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.SystemSessionProperties.resourceOvercommit;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ResourceManagerClusterStateProvider
{
    private final Map<String, NodeQueryState> nodeQueryStates = new ConcurrentHashMap<>();
    private final Map<String, InternalNodeState> nodeStatuses = new ConcurrentHashMap<>();

    private final SessionPropertyManager sessionPropertyManager;
    private final int maxCompletedQueries;
    private final Duration queryExpirationTimeout;
    private final Duration completedQueryExpirationTimeout;
    private final boolean isReservedPoolEnabled;

    @Inject
    public ResourceManagerClusterStateProvider(
            SessionPropertyManager sessionPropertyManager,
            ResourceManagerConfig resourceManagerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            @ForResourceManager ScheduledExecutorService scheduledExecutorService)
    {
        this(
                sessionPropertyManager,
                requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getMaxCompletedQueries(),
                requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getQueryExpirationTimeout(),
                requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getCompletedQueryExpirationTimeout(),
                requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getNodeStatusTimeout(),
                requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").isReservedPoolEnabled(),
                scheduledExecutorService);
    }

    public ResourceManagerClusterStateProvider(
            SessionPropertyManager sessionPropertyManager,
            int maxCompletedQueries,
            Duration queryExpirationTimeout,
            Duration completedQueryExpirationTimeout,
            Duration nodeStatusTimeout,
            boolean isReservedPoolEnabled,
            ScheduledExecutorService scheduledExecutorService)
    {
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.maxCompletedQueries = maxCompletedQueries;
        this.queryExpirationTimeout = queryExpirationTimeout;
        this.completedQueryExpirationTimeout = completedQueryExpirationTimeout;
        this.isReservedPoolEnabled = isReservedPoolEnabled;

        requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            for (NodeQueryState nodeQueryState : ImmutableList.copyOf(nodeQueryStates.values())) {
                nodeQueryState.purgeExpiredQueries();
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

    public void registerHeartbeat(String nodeId, BasicQueryInfo basicQueryInfo)
    {
        NodeQueryState state = nodeQueryStates.computeIfAbsent(nodeId, identifier -> new NodeQueryState(
                nodeId,
                maxCompletedQueries,
                queryExpirationTimeout.toMillis(),
                completedQueryExpirationTimeout.toMillis()));
        state.updateQuery(basicQueryInfo);
    }

    public void registerHeartbeat(NodeStatus nodeStatus)
    {
        InternalNodeState nodeState = nodeStatuses.get(nodeStatus.getNodeId());
        if (nodeState == null) {
            nodeStatuses.put(nodeStatus.getNodeId(), new InternalNodeState(nodeStatus));
        }
        else {
            nodeState.updateNodeStatus(nodeStatus);
        }
    }

    public List<ResourceGroupRuntimeInfo> getResourceGroups(String excludingNode)
    {
        Map<ResourceGroupId, ResourceGroupRuntimeInfo.Builder> resourceGroupBuilders = new HashMap<>();
        nodeQueryStates.values().stream()
                .filter(state -> !state.getNodeId().equals(excludingNode))
                .map(NodeQueryState::getActiveQueries)
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

    public List<BasicQueryInfo> getQueryInfos()
    {
        return ImmutableList.copyOf(nodeQueryStates.values()).stream()
                .map(NodeQueryState::getAllQueries)
                .flatMap(Collection::stream)
                .map(Query::getBasicQueryInfo)
                .collect(toImmutableList());
    }

    public Map<MemoryPoolId, ClusterMemoryPoolInfo> getClusterMemoryPoolInfos()
    {
        List<MemoryInfo> memoryInfos = nodeStatuses.values().stream()
                .map(nodeStatus -> nodeStatus.getNodeStatus().getMemoryInfo())
                .collect(toImmutableList());

        int generalPoolCounts = 0;
        int reservedPoolCounts = 0;
        Query largestGeneralPoolQuery = null;
        for (NodeQueryState nodeQueryState : nodeQueryStates.values()) {
            for (Query query : nodeQueryState.getActiveQueries()) {
                MemoryPoolId memoryPool = query.getBasicQueryInfo().getMemoryPool();
                if (GENERAL_POOL.equals(memoryPool)) {
                    generalPoolCounts = Math.incrementExact(generalPoolCounts);
                    if (!resourceOvercommit(query.getBasicQueryInfo().getSession().toSession(sessionPropertyManager))) {
                        largestGeneralPoolQuery = getLargestMemoryQuery(largestGeneralPoolQuery, query);
                    }
                }
                else if (RESERVED_POOL.equals(memoryPool)) {
                    reservedPoolCounts = Math.incrementExact(reservedPoolCounts);
                }
                else {
                    throw new IllegalArgumentException("Unrecognized memory pool: " + memoryPool);
                }
            }
        }

        ImmutableMap.Builder<MemoryPoolId, ClusterMemoryPoolInfo> memoryPoolInfos = ImmutableMap.builder();
        ClusterMemoryPool pool = new ClusterMemoryPool(GENERAL_POOL);
        pool.update(memoryInfos, generalPoolCounts);
        ClusterMemoryPoolInfo clusterInfo = pool.getClusterInfo(Optional.ofNullable(largestGeneralPoolQuery).map(Query::getQueryId));
        memoryPoolInfos.put(GENERAL_POOL, clusterInfo);
        if (isReservedPoolEnabled) {
            pool = new ClusterMemoryPool(RESERVED_POOL);
            pool.update(memoryInfos, reservedPoolCounts);
            memoryPoolInfos.put(RESERVED_POOL, pool.getClusterInfo());
        }
        return memoryPoolInfos.build();
    }

    private Query getLargestMemoryQuery(@Nullable Query existingLargeQuery, @NotNull Query newQuery)
    {
        requireNonNull(newQuery, "newQuery must not be null");
        if (existingLargeQuery == null) {
            return newQuery;
        }
        long largestGeneralBytes = existingLargeQuery.getBasicQueryInfo().getQueryStats().getTotalMemoryReservation().toBytes();
        long currentGeneralBytes = newQuery.getBasicQueryInfo().getQueryStats().getTotalMemoryReservation().toBytes();
        if (currentGeneralBytes > largestGeneralBytes) {
            return newQuery;
        }
        return existingLargeQuery;
    }

    public synchronized Map<String, MemoryInfo> getWorkerMemoryInfo()
    {
        return nodeStatuses.entrySet().stream().collect(toImmutableMap(e -> {
            String nodeIdentifier = e.getValue().getNodeStatus().getNodeId();
            String nodeHost = URI.create(e.getValue().getNodeStatus().getExternalAddress()).getHost();
            return nodeIdentifier + " [" + nodeHost + "]";
        }, e -> e.getValue().getNodeStatus().getMemoryInfo()));
    }

    private static class NodeQueryState
    {
        private final String nodeId;
        private final int maxCompletedQueries;
        private final long queryExpirationTimeoutMillis;
        private final long completedQueryExpirationTimeoutMillis;

        @GuardedBy("this")
        private final Map<QueryId, Query> activeQueries = new HashMap<>();
        @GuardedBy("this")
        private final Map<QueryId, Query> completedQueries = new LinkedHashMap<>();

        public NodeQueryState(String nodeId, int maxCompletedQueries, long queryExpirationTimeoutMillis, long completedQueryExpirationTimeoutMillis)
        {
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            checkArgument(maxCompletedQueries > 0);
            checkArgument(queryExpirationTimeoutMillis > 0);
            checkArgument(completedQueryExpirationTimeoutMillis > 0);
            this.maxCompletedQueries = maxCompletedQueries;
            this.queryExpirationTimeoutMillis = queryExpirationTimeoutMillis;
            this.completedQueryExpirationTimeoutMillis = completedQueryExpirationTimeoutMillis;
        }

        public synchronized NodeQueryState updateQuery(BasicQueryInfo basicQueryInfo)
        {
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
            return this;
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

        private boolean isQueryExpired(Query query, long currentTimeMillis, long timeout)
        {
            return (currentTimeMillis - query.getLastHeartbeatInMillis()) > timeout;
        }

        private boolean isQueryCompleted(Query query)
        {
            return query.getBasicQueryInfo().getState().isDone();
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
}
