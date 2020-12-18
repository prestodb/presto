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

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.HostAddress;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ResourceManagerHeartbeatSender
        implements HeartbeatSender
{
    private final DriftClient<ResourceManagerClient> resourceManagerClient;
    private final InternalNodeManager internalNodeManager;
    private final Cache<InternalNode, List<ResourceGroupRuntimeInfo>> cache;
    private final Executor executor = Executors.newCachedThreadPool();

    @Inject
    public ResourceManagerHeartbeatSender(
            @ForResourceManager DriftClient<ResourceManagerClient> resourceManagerClient,
            InternalNodeManager internalNodeManager)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerService is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(10, SECONDS)
                .refreshAfterWrite(1, SECONDS)
                .build(asyncReloading(CacheLoader.from(this::getResourceGroupInfos), executor));
    }

    @Override
    public void sendQueryHeartbeat(BasicQueryInfo basicQueryInfo)
    {
        getResourceManagers().forEach(hostAndPort -> resourceManagerClient.get(Optional.of(hostAndPort.toString())).queryHeartbeat(internalNodeManager.getCurrentNode(), basicQueryInfo));
    }

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo()
    {
        try {
            InternalNode currentNode = internalNodeManager.getCurrentNode();
            return cache.get(currentNode, () -> getResourceGroupInfos(currentNode));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    // TODO: rename
    private List<ResourceGroupRuntimeInfo> getResourceGroupInfos(InternalNode internalNode)
    {
        return resourceManagerClient.get().getResourceGroupInfo(internalNode);
    }

    @Override
    public void sendNodeHeartbeat(List<TaskStatus> taskStatuses)
    {
        getResourceManagers().forEach(hostAndPort -> resourceManagerClient.get(Optional.of(hostAndPort.toString())).nodeHeartbeat(internalNodeManager.getCurrentNode(), taskStatuses));
    }

    private List<HostAddress> getResourceManagers()
    {
        return internalNodeManager.getResourceManagers()
                .stream()
                .filter(node -> node.getThriftPort().isPresent())
                .map(resourceManagerNode -> {
                    HostAddress hostAndPort = resourceManagerNode.getHostAndPort();
                    return HostAddress.fromParts(hostAndPort.getHostText(), resourceManagerNode.getThriftPort().getAsInt());
                })
                .collect(toImmutableList());
    }
}
