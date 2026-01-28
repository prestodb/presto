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
import com.facebook.presto.resourcemanager.ResourceManagerException;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;

import java.util.List;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static java.util.Objects.requireNonNull;

@Path("/v1/resource-manager")
public class ResourceManagerResource
{
    private static final Logger LOG = Logger.get(ResourceGroupRuntimeInfo.class);
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
    @PATCH
    @Consumes(APPLICATION_JSON)
    @Path("/node/{nodeId}")
    public void patchNodeHeartbeat(@PathParam("nodeId") String nodeId, NodeStatus nodeStatus)
    {
        if (!nodeId.equals(nodeStatus.getNodeId())) {
            throw new WebApplicationException(
                    Response.status(BAD_REQUEST)
                            .type(APPLICATION_JSON)
                            .entity(ImmutableMap.of(
                                    "error", "nodeId mismatch",
                                    "pathNodeId", nodeId,
                                    "bodyNodeId", nodeStatus.getNodeId()))
                            .build());
        }
        executor.execute(() -> clusterStateProvider.registerNodeHeartbeat(nodeStatus));
    }

    /**
     * This method registers a heartbeat to the resource manager.  A query heartbeat is used for the following purposes:
     *
     * 1) Inform resource managers about current resource group utilization.
     * 2) Inform resource managers about current running queries.
     * 3) Inform resource managers about coordinator status and health.
     */
    @PATCH
    @Consumes(APPLICATION_JSON)
    @Path("/nodes/{nodeId}/queries/{queryId}")
    public void patchQueryHeartbeat(
            @PathParam("nodeId") String nodeId,
            @PathParam("queryId") String queryId,
            @HeaderParam("Sequence-Id") long sequenceId,
            BasicQueryInfo basicQueryInfo)
    {
        if (!queryId.equals(basicQueryInfo.getQueryId().toString())) {
            throw new WebApplicationException(
                    Response.status(BAD_REQUEST)
                            .type(APPLICATION_JSON)
                            .entity(ImmutableMap.of(
                                    "error", "nodeId mismatch",
                                    "pathQueryId", nodeId,
                                    "bodyQueryId", basicQueryInfo.getQueryId()))
                            .build());
        }
        executor.execute(() -> {
            clusterStateProvider.registerQueryHeartbeat(nodeId, basicQueryInfo, sequenceId);
        });
    }

    /**
     * Returns the resource group information across all clusters except for {@code excludingNode}, which is excluded
     * to prevent redundancy with local resource group information.
     */
    @GET
    @Produces(APPLICATION_JSON)
    @Path("/resource-groups")
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
    @Path("/memory-pools")
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

    @PATCH
    @Consumes(APPLICATION_JSON)
    @Path("/nodes/{nodeId}/resource-groups")
    public void putResourceGroupRuntimeInfo(@PathParam("nodeId") String node, List<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos)
    {
        executor.execute(() -> clusterStateProvider.registerResourceGroupRuntimeHeartbeat(node, resourceGroupRuntimeInfos));
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/tasks/count")
    public void getTaskCount(@QueryParam("state") String state, @Suspended AsyncResponse async)
    {
        executor.execute(() -> {
            try {
                if (state.equals("running")) {
                    async.resume(clusterStateProvider.getRunningTaskCount());
                }
                else {
                    throw new ResourceManagerException("Invalid task count state requested");
                }
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
