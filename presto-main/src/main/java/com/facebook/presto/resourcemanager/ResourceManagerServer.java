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

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ThriftService(value = "presto-resource-manager", idlName = "PrestoResourceManager")
public class ResourceManagerServer
{
    private final ResourceManagerClusterStateProvider clusterStateProvider;

    @Inject
    public ResourceManagerServer(ResourceManagerClusterStateProvider clusterStateProvider)
    {
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "internalNodeManager is null");
    }

    @ThriftMethod
    public void queryHeartbeat(String nodeId, BasicQueryInfo basicQueryInfo)
    {
        clusterStateProvider.registerHeartbeat(nodeId, basicQueryInfo);
    }

    @ThriftMethod
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo(String excludingNode)
    {
        return clusterStateProvider.getResourceGroups(excludingNode);
    }

    @ThriftMethod
    public Map<MemoryPoolId, ClusterMemoryPoolInfo> getMemoryPoolInfo()
    {
        return clusterStateProvider.getClusterMemoryPoolInfos();
    }

    @ThriftMethod
    public void nodeHeartbeat(NodeStatus nodeStatus)
    {
        clusterStateProvider.registerHeartbeat(nodeStatus);
    }
}
