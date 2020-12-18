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
package com.facebook.presto.dispatcher;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThriftService(value = "presto-resource-manager", idlName = "PrestoResourceManager")
public class ResourceManagerServer
{
    public static final long QUERY_EXPIRATION_TIMEOUT = SECONDS.toMillis(10);
    private final InternalNodeStateManager internalNodeManager = new InternalNodeStateManager();

    @ThriftMethod
    public void queryHeartbeat(InternalNode internalNode, BasicQueryInfo basicQueryInfo)
    {
        internalNodeManager.registerHeartbeat(internalNode, basicQueryInfo);
    }

    @ThriftMethod
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo(InternalNode excludingNode)
    {
        return internalNodeManager.getResourceGroups(excludingNode);
    }

    @ThriftMethod
    public void nodeHeartbeat(InternalNode internalNode, List<TaskStatus> taskStatuses)
    {
        return;
    }

    private static class InternalNodeStateManager
    {
        private final ConcurrentMap<InternalNode, InternalNodeState> internalNodes = new ConcurrentHashMap<>();
        private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        public InternalNodeStateManager()
        {
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                for (InternalNodeState internalNodeState : ImmutableList.copyOf(internalNodes.values())) {
                    internalNodeState.purgeExpiredQueries();
                    if (internalNodeState.getActiveQueries() == 0) {
                        internalNodes.remove(internalNodeState.getInternalNode());
                    }
                }
            }, 100, 100, MILLISECONDS);
        }

        public void registerHeartbeat(InternalNode internalNode, BasicQueryInfo basicQueryInfo)
        {
            InternalNodeState state = internalNodes.computeIfAbsent(internalNode, InternalNodeState::new);
            state.updateQuery(basicQueryInfo);
        }

        public List<ResourceGroupRuntimeInfo> getResourceGroups(InternalNode excludingNode)
        {
            Map<ResourceGroupId, ResourceGroupRuntimeInfo.Builder> resourceGroupBuilders = new HashMap<>();
            for (InternalNodeState internalNodeState : ImmutableList.copyOf(internalNodes.values())) {
                if (internalNodeState.getInternalNode().equals(excludingNode)) {
                    continue;
                }
                for (Query query : internalNodeState.getQueries()) {
                    query.getBasicQueryInfo().getResourceGroupId().ifPresent(resourceGroupId -> {
                        ResourceGroupRuntimeInfo.Builder builder = resourceGroupBuilders.computeIfAbsent(resourceGroupId, (ResourceGroupRuntimeInfo::builder));
                        builder.addMemoryUsageBytes(query.getBasicQueryInfo().getQueryStats().getTotalMemoryReservation().toBytes());
                        builder.addQueuedQueries(query.getBasicQueryInfo().getState() == QUEUED ? 1 : 0);
                        builder.addRunningQueries(query.getBasicQueryInfo().getState() == RUNNING ? 1 : 0);
                    });
                }
            }
            return resourceGroupBuilders.values().stream().map(ResourceGroupRuntimeInfo.Builder::build).collect(toImmutableList());
        }
    }

    private static class InternalNodeState
    {
        private final InternalNode internalNode;
        private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();

        public InternalNodeState(InternalNode internalNode)
        {
            this.internalNode = requireNonNull(internalNode, "internalNode is null");
        }

        public InternalNodeState updateQuery(BasicQueryInfo basicQueryInfo)
        {
            if (queries.computeIfPresent(basicQueryInfo.getQueryId(), (queryId, query) -> query.updateQueryInfo(basicQueryInfo)) == null) {
                queries.put(basicQueryInfo.getQueryId(), new Query(basicQueryInfo));
            }
            return this;
        }

        public void purgeExpiredQueries()
        {
            for (Query query : ImmutableList.copyOf(queries.values())) {
                long currentTimeMillis = System.currentTimeMillis();
                if (isQueryExpired(query, currentTimeMillis)) {
                    queries.remove(query.getQueryId());
                }
            }
        }

        boolean isQueryExpired(Query query, long currentTimeMillis)
        {
            if ((currentTimeMillis - query.getLastHeartbeatInMillis()) > QUERY_EXPIRATION_TIMEOUT) {
                return true;
            }
            if (query.getBasicQueryInfo().getState().isDone()) {
                return true;
            }
            return false;
        }

        public InternalNode getInternalNode()
        {
            return internalNode;
        }

        public int getActiveQueries()
        {
            return queries.size();
        }

        public List<Query> getQueries()
        {
            return ImmutableList.copyOf(queries.values());
        }
    }

    public static class Query
    {
        private final QueryId queryId;

        private volatile BasicQueryInfo basicQueryInfo;
        private volatile long lastHeartbeatInMillis;

        public Query(BasicQueryInfo basicQueryInfo)
        {
            this.queryId = basicQueryInfo.getQueryId();
            this.basicQueryInfo = basicQueryInfo;
            recordHeartbeat();
        }

        private void recordHeartbeat()
        {
            this.lastHeartbeatInMillis = System.currentTimeMillis();
        }

        public long getLastHeartbeatInMillis()
        {
            return lastHeartbeatInMillis;
        }

        public Query updateQueryInfo(BasicQueryInfo basicQueryInfo)
        {
            this.basicQueryInfo = basicQueryInfo;
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
