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

import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourcemanager.ResourceManagerProxy;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.ttl.clusterttlprovidermanagers.ClusterTtlProviderManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Iterator;
import java.util.Optional;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

@Path("/v1/cluster")
@RolesAllowed({ADMIN, USER})
public class ClusterStatsResource
{
    private final InternalNodeManager nodeManager;
    private final DispatchManager dispatchManager;
    private final boolean isIncludeCoordinator;
    private final boolean resourceManagerEnabled;
    private final ClusterMemoryManager clusterMemoryManager;
    private final Optional<ResourceManagerProxy> proxyHelper;
    private final InternalResourceGroupManager internalResourceGroupManager;
    private final ClusterTtlProviderManager clusterTtlProviderManager;

    @Inject
    public ClusterStatsResource(
            NodeSchedulerConfig nodeSchedulerConfig,
            ServerConfig serverConfig,
            InternalNodeManager nodeManager,
            DispatchManager dispatchManager,
            ClusterMemoryManager clusterMemoryManager,
            Optional<ResourceManagerProxy> proxyHelper,
            InternalResourceGroupManager internalResourceGroupManager,
            ClusterTtlProviderManager clusterTtlProviderManager)
    {
        this.isIncludeCoordinator = requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator();
        this.resourceManagerEnabled = requireNonNull(serverConfig, "serverConfig is null").isResourceManagerEnabled();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.clusterMemoryManager = requireNonNull(clusterMemoryManager, "clusterMemoryManager is null");
        this.proxyHelper = requireNonNull(proxyHelper, "internalNodeManager is null");
        this.internalResourceGroupManager = requireNonNull(internalResourceGroupManager, "internalResourceGroupManager is null");
        this.clusterTtlProviderManager = requireNonNull(clusterTtlProviderManager, "clusterTtlProvider is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void getClusterStats(
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (resourceManagerEnabled) {
            proxyClusterStats(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            return;
        }

        long runningQueries = 0;
        long blockedQueries = 0;
        long queuedQueries = 0;

        long activeNodes = nodeManager.getNodes(NodeState.ACTIVE).size();
        if (!isIncludeCoordinator) {
            activeNodes -= 1;
        }

        long runningDrivers = 0;
        long runningTasks = 0;
        double memoryReservation = 0;

        long totalInputRows = dispatchManager.getStats().getConsumedInputRows().getTotalCount();
        long totalInputBytes = dispatchManager.getStats().getConsumedInputBytes().getTotalCount();
        long totalCpuTimeSecs = dispatchManager.getStats().getConsumedCpuTimeSecs().getTotalCount();

        for (BasicQueryInfo query : dispatchManager.getQueries()) {
            if (query.getState() == QueryState.QUEUED) {
                queuedQueries++;
            }
            else if (query.getState() == QueryState.RUNNING) {
                if (query.getQueryStats().isFullyBlocked()) {
                    blockedQueries++;
                }
                else {
                    runningQueries++;
                }
            }

            if (!query.getState().isDone()) {
                totalInputBytes += query.getQueryStats().getRawInputDataSize().toBytes();
                totalInputRows += query.getQueryStats().getRawInputPositions();
                totalCpuTimeSecs += query.getQueryStats().getTotalCpuTime().getValue(SECONDS);

                memoryReservation += query.getQueryStats().getUserMemoryReservation().toBytes();
                runningDrivers += query.getQueryStats().getRunningDrivers();
                runningTasks += query.getQueryStats().getRunningTasks();
            }
        }

        asyncResponse.resume(Response.ok(new ClusterStats(
                runningQueries,
                blockedQueries,
                queuedQueries,
                activeNodes,
                runningDrivers,
                runningTasks,
                memoryReservation,
                totalInputRows,
                totalInputBytes,
                totalCpuTimeSecs,
                internalResourceGroupManager.getQueriesQueuedOnInternal())).build());
    }

    @GET
    @Path("memory")
    public Response getClusterMemoryPoolInfo(@HeaderParam(X_FORWARDED_PROTO) String xForwardedProto, @Context UriInfo uriInfo)
    {
        return Response.ok()
                .entity(clusterMemoryManager.getMemoryPoolInfo())
                .build();
    }

    @GET
    @Path("workerMemory")
    public Response getWorkerMemoryInfo(@HeaderParam(X_FORWARDED_PROTO) String xForwardedProto, @Context UriInfo uriInfo)
    {
        return Response.ok()
                .entity(clusterMemoryManager.getWorkerMemoryInfo())
                .build();
    }

    @GET
    @Path("ttl")
    public Response getClusterTtl()
    {
        return Response.ok().entity(clusterTtlProviderManager.getClusterTtl()).build();
    }

    private void proxyClusterStats(HttpServletRequest servletRequest, AsyncResponse asyncResponse, String xForwardedProto, UriInfo uriInfo)
    {
        try {
            checkState(proxyHelper.isPresent());
            Iterator<InternalNode> resourceManagers = nodeManager.getResourceManagers().iterator();
            if (!resourceManagers.hasNext()) {
                asyncResponse.resume(Response.status(SERVICE_UNAVAILABLE).build());
                return;
            }
            InternalNode resourceManagerNode = resourceManagers.next();

            URI uri = uriInfo.getRequestUriBuilder()
                    .scheme(resourceManagerNode.getInternalUri().getScheme())
                    .host(resourceManagerNode.getHostAndPort().toInetAddress().getHostName())
                    .port(resourceManagerNode.getInternalUri().getPort())
                    .build();
            proxyHelper.get().performRequest(servletRequest, asyncResponse, uri);
        }
        catch (Exception e) {
            asyncResponse.resume(e);
        }
    }

    public static class ClusterStats
    {
        private final long runningQueries;
        private final long blockedQueries;
        private final long queuedQueries;

        private final long activeWorkers;
        private final long runningDrivers;
        private final long runningTasks;
        private final double reservedMemory;

        private final long totalInputRows;
        private final long totalInputBytes;
        private final long totalCpuTimeSecs;
        private final long adjustedQueueSize;

        @JsonCreator
        public ClusterStats(
                @JsonProperty("runningQueries") long runningQueries,
                @JsonProperty("blockedQueries") long blockedQueries,
                @JsonProperty("queuedQueries") long queuedQueries,
                @JsonProperty("activeWorkers") long activeWorkers,
                @JsonProperty("runningDrivers") long runningDrivers,
                @JsonProperty("runningTasks") long runningTasks,
                @JsonProperty("reservedMemory") double reservedMemory,
                @JsonProperty("totalInputRows") long totalInputRows,
                @JsonProperty("totalInputBytes") long totalInputBytes,
                @JsonProperty("totalCpuTimeSecs") long totalCpuTimeSecs,
                @JsonProperty("adjustedQueueSize") long adjustedQueueSize)
        {
            this.runningQueries = runningQueries;
            this.blockedQueries = blockedQueries;
            this.queuedQueries = queuedQueries;
            this.activeWorkers = activeWorkers;
            this.runningDrivers = runningDrivers;
            this.runningTasks = runningTasks;
            this.reservedMemory = reservedMemory;
            this.totalInputRows = totalInputRows;
            this.totalInputBytes = totalInputBytes;
            this.totalCpuTimeSecs = totalCpuTimeSecs;
            this.adjustedQueueSize = adjustedQueueSize;
        }

        @JsonProperty
        public long getRunningQueries()
        {
            return runningQueries;
        }

        @JsonProperty
        public long getBlockedQueries()
        {
            return blockedQueries;
        }

        @JsonProperty
        public long getQueuedQueries()
        {
            return queuedQueries;
        }

        @JsonProperty
        public long getActiveWorkers()
        {
            return activeWorkers;
        }

        @JsonProperty
        public long getRunningDrivers()
        {
            return runningDrivers;
        }

        @JsonProperty
        public long getRunningTasks()
        {
            return runningTasks;
        }

        @JsonProperty
        public double getReservedMemory()
        {
            return reservedMemory;
        }

        @JsonProperty
        public long getTotalInputRows()
        {
            return totalInputRows;
        }

        @JsonProperty
        public long getTotalInputBytes()
        {
            return totalInputBytes;
        }

        @JsonProperty
        public long getTotalCpuTimeSecs()
        {
            return totalCpuTimeSecs;
        }

        @JsonProperty
        public long getAdjustedQueueSize()
        {
            return adjustedQueueSize;
        }
    }
}
