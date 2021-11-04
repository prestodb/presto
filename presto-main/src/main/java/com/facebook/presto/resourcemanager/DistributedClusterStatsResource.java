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

import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.ClusterStatsResource;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/cluster")
@RolesAllowed({USER, ADMIN})
public class DistributedClusterStatsResource
{
    private final boolean isIncludeCoordinator;
    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final InternalNodeManager internalNodeManager;

    @Inject
    public DistributedClusterStatsResource(NodeSchedulerConfig nodeSchedulerConfig, ResourceManagerClusterStateProvider clusterStateProvider, InternalNodeManager internalNodeManager)
    {
        this.isIncludeCoordinator = requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator();
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "nodeStateManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getClusterStats()
    {
        long runningQueries = 0;
        long blockedQueries = 0;
        long queuedQueries = 0;

        long activeNodes = internalNodeManager.getNodes(NodeState.ACTIVE).size();
        if (!isIncludeCoordinator) {
            activeNodes -= internalNodeManager.getCoordinators().size();
        }
        activeNodes -= internalNodeManager.getResourceManagers().size();

        long runningDrivers = 0;
        long runningTasks = 0;
        double memoryReservation = 0;

        long totalInputRows = 0;
        long totalInputBytes = 0;
        long totalCpuTimeSecs = 0;

        for (BasicQueryInfo query : clusterStateProvider.getClusterQueries()) {
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
        //TODO compute adjusted queue size on RM
        return Response.ok(new ClusterStatsResource.ClusterStats(
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
                0))
                .build();
    }

    @GET
    @Path("memory")
    public Response getClusterMemoryPoolInfo()
    {
        Map<MemoryPoolId, MemoryPoolInfo> memoryPoolInfos = clusterStateProvider.getClusterMemoryPoolInfo().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue().getMemoryPoolInfo()));
        return Response.ok()
                .entity(memoryPoolInfos)
                .build();
    }

    @GET
    @Path("workerMemory")
    public Response getWorkerMemoryInfo()
    {
        return Response.ok()
                .entity(clusterStateProvider.getWorkerMemoryInfo())
                .build();
    }
}
