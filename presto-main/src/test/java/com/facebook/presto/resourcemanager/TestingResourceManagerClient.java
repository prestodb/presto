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
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class TestingResourceManagerClient
        implements ResourceManagerClient
{
    private final AtomicInteger queryHeartbeats = new AtomicInteger();
    private final AtomicInteger nodeHeartbeats = new AtomicInteger();
    private final Map<String, Integer> resourceGroupInfoCalls = new ConcurrentHashMap<>();

    private volatile List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos = ImmutableList.of();

    @Override
    public void queryHeartbeat(String internalNode, BasicQueryInfo basicQueryInfo, long sequenceId)
    {
        queryHeartbeats.incrementAndGet();
    }

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo(String excludingNode)
    {
        resourceGroupInfoCalls.putIfAbsent(excludingNode, 0);
        resourceGroupInfoCalls.compute(excludingNode, (s, integer) -> integer + 1);
        return resourceGroupRuntimeInfos;
    }

    public void setResourceGroupRuntimeInfos(List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos)
    {
        this.resourceGroupRuntimeInfos = ImmutableList.copyOf(resourceGroupRuntimeInfos);
    }

    @Override
    public void nodeHeartbeat(NodeStatus nodeStatus)
    {
        nodeHeartbeats.incrementAndGet();
    }

    @Override
    public Map<MemoryPoolId, ClusterMemoryPoolInfo> getMemoryPoolInfo()
    {
        return ImmutableMap.of();
    }

    public int getQueryHeartbeats()
    {
        return queryHeartbeats.get();
    }

    public int getNodeHeartbeats()
    {
        return nodeHeartbeats.get();
    }

    public int getResourceGroupInfoCalls(String identifier)
    {
        return resourceGroupInfoCalls.get(identifier);
    }
}
