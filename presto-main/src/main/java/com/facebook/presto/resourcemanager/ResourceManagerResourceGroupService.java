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

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ResourceManagerResourceGroupService
        implements ResourceGroupService
{
    private final DriftClient<ResourceManagerClient> resourceManagerClient;
    private final InternalNodeManager internalNodeManager;
    private final Cache<InternalNode, List<ResourceGroupRuntimeInfo>> cache;
    private final Executor executor = Executors.newCachedThreadPool();

    @Inject
    public ResourceManagerResourceGroupService(
            @ForResourceManager DriftClient<ResourceManagerClient> resourceManagerClient,
            InternalNodeManager internalNodeManager)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerService is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(10, SECONDS)
                .refreshAfterWrite(1, SECONDS)
                .build(asyncReloading(new CacheLoader<InternalNode, List<ResourceGroupRuntimeInfo>>() {
                    @Override
                    public List<ResourceGroupRuntimeInfo> load(InternalNode internalNode)
                            throws ResourceManagerInconsistentException
                    {
                        return getResourceGroupInfos(internalNode);
                    }
                }, executor));
    }

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupInfo()
            throws ResourceManagerInconsistentException
    {
        try {
            InternalNode currentNode = internalNodeManager.getCurrentNode();
            return cache.get(currentNode, () -> getResourceGroupInfos(currentNode));
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), ResourceManagerInconsistentException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private List<ResourceGroupRuntimeInfo> getResourceGroupInfos(InternalNode internalNode)
            throws ResourceManagerInconsistentException
    {
        return resourceManagerClient.get().getResourceGroupInfo(internalNode.getNodeIdentifier());
    }
}
