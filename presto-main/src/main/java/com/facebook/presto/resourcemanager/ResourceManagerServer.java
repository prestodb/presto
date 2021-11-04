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

import com.facebook.drift.annotations.ThriftException;
import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ThriftService(value = "presto-resource-manager", idlName = "PrestoResourceManager")
public class ResourceManagerServer
{
    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final ListeningExecutorService executor;

    @Inject
    public ResourceManagerServer(ResourceManagerClusterStateProvider clusterStateProvider, @ForResourceManager ListeningExecutorService executor)
    {
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "internalNodeManager is null");
        this.executor = executor;
    }

    /**
     * This method registers a heartbeat to the resource manager.  A query heartbeat is used for the following purposes:
     *
     * 1) Inform resource managers about current resource group utilization.
     * 2) Inform resource managers about current running queries.
     * 3) Inform resource managers about coordinator status and health.
     */
    @ThriftMethod
    public void queryHeartbeat(String nodeId, BasicQueryInfo basicQueryInfo, long sequenceId)
    {
        executor.execute(() -> clusterStateProvider.registerQueryHeartbeat(nodeId, basicQueryInfo, sequenceId));
    }

    /**
     * Returns the resource group information across all clusters except for {@code excludingNode}, which is excluded
     * to prevent redundancy with local resource group information.
     */
    @ThriftMethod(exception = {@ThriftException(type = ResourceManagerInconsistentException.class, id = 1)})
    public ListenableFuture<List<ResourceGroupRuntimeInfo>> getResourceGroupInfo(String excludingNode)
    {
        return executor.submit(() -> clusterStateProvider.getClusterResourceGroups(excludingNode));
    }

    @ThriftMethod
    public ListenableFuture<Map<MemoryPoolId, ClusterMemoryPoolInfo>> getMemoryPoolInfo()
    {
        return executor.submit(clusterStateProvider::getClusterMemoryPoolInfo);
    }

    /**
     * Registers a node heartbeat with the resource manager.
     */
    @ThriftMethod
    public void nodeHeartbeat(NodeStatus nodeStatus)
    {
        executor.execute(() -> clusterStateProvider.registerNodeHeartbeat(nodeStatus));
    }
}
