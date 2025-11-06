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
package com.facebook.presto.server;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.resourcemanager.ForResourceManager;
import com.facebook.presto.resourcemanager.ResourceManagerClusterStateProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;

import java.util.List;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/resourcemanager")
public class ResourceManagerResource
{
    Logger log = Logger.get(ResourceGroupRuntimeInfo.class);
    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final ListeningExecutorService executor;

    @Inject
    public ResourceManagerResource(
            ResourceManagerClusterStateProvider clusterStateProvider,
            @ForResourceManager ListeningExecutorService executor)
    {
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "clusterStateProvider is null");
        this.executor = executor;
    }

    /**
     * Registers a node heartbeat with the resource manager.
     */
    @PUT
    @Consumes(APPLICATION_JSON)
    @Path("nodeHeartbeat")
    public void nodeHeartbeat(NodeStatus nodeStatus)
    {
        executor.execute(() -> clusterStateProvider.registerNodeHeartbeat(nodeStatus));
    }

    /**
     * This method registers a heartbeat to the resource manager.  A query heartbeat is used for the following purposes:
     *
     * 1) Inform resource managers about current resource group utilization.
     * 2) Inform resource managers about current running queries.
     * 3) Inform resource managers about coordinator status and health.
     */
    @PUT
    @Consumes(APPLICATION_JSON)
    @Path("queryHeartbeat")
    public void queryHeartbeat(@QueryParam("nodeId") String nodeId, BasicQueryInfo basicQueryInfo, @QueryParam("sequenceId") long sequenceId)
    {
        executor.execute(() -> clusterStateProvider.registerQueryHeartbeat(nodeId, basicQueryInfo, sequenceId));
    }

    /**
     * Returns the resource group information across all clusters except for {@code excludingNode}, which is excluded
     * to prevent redundancy with local resource group information.
     */
    @GET
    @Produces(APPLICATION_JSON)
    @Path("resourceGroupInfo")
    public void getResourceGroupInfo(@QueryParam("excludingNode") String excludingNode, @Suspended AsyncResponse async)
    {
        executor.execute(() -> {
            try {
                async.resume(clusterStateProvider.getClusterResourceGroups(excludingNode));
            }
            catch (Throwable t) {
                async.resume(Response.serverError()
                        .entity(ImmutableMap.of("error", t.getMessage()))
                        .type(APPLICATION_JSON)
                        .build());
            }
        });
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("memoryPoolInfo")
    public void getMemoryPoolInfo(@Suspended AsyncResponse async)
    {
        executor.execute(() -> {
            try {
                async.resume(clusterStateProvider.getClusterMemoryPoolInfo());
            }
            catch (Throwable t) {
                async.resume(Response.serverError()
                        .entity(ImmutableMap.of("error", t.getMessage()))
                        .type(APPLICATION_JSON)
                        .build());
            }
        });
    }

    @PUT
    @Consumes(APPLICATION_JSON)
    @Path("resourceGroupRuntimeHeartbeat")
    public void resourceGroupRuntimeHeartbeat(@QueryParam("node") String node, List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos)
    {
        executor.execute(() -> clusterStateProvider.registerResourceGroupRuntimeHeartbeat(node, resourceGroupRuntimeInfos));
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("getRunningTaskCount")
    public void getRunningTaskCount(@Suspended AsyncResponse async)
    {
        executor.execute(() -> {
            try {
                async.resume(clusterStateProvider.getRunningTaskCount());
            }
            catch (Throwable t) {
                async.resume(Response.serverError()
                        .entity(ImmutableMap.of("error", t.getMessage()))
                        .type(APPLICATION_JSON)
                        .build());
            }
        });
    }
}
