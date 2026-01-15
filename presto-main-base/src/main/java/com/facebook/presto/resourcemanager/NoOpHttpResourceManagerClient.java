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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NoOpHttpResourceManagerClient
        implements HttpResourceManagerClient
{
    @Override
    public void queryHeartbeat(Optional<URI> target, String internalNode, BasicQueryInfo basicQueryInfo, long sequenceId) {}

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo(Optional<URI> target, String excludingNode) throws ResourceManagerInconsistentException
    {
        return ImmutableList.of();
    }

    @Override
    public void nodeHeartbeat(Optional<URI> target, NodeStatus nodeStatus) {}

    @Override
    public Map<MemoryPoolId, ClusterMemoryPoolInfo> getMemoryPoolInfo(Optional<URI> target)
    {
        return ImmutableMap.of();
    }

    @Override
    public void resourceGroupRuntimeHeartbeat(Optional<URI> target, String node, List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo)
    {}

    @Override
    public int getRunningTaskCount(Optional<URI> target)
    {
        return 0;
    }
}
