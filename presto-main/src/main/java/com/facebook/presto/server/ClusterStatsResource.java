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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.NodeState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/cluster")
public class ClusterStatsResource
{
    private final InternalNodeManager nodeManager;
    private final QueryManager queryManager;
    private final boolean isIncludeCoordinator;
    private final ClusterMemoryManager clusterMemoryManager;

    @Inject
    public ClusterStatsResource(NodeSchedulerConfig nodeSchedulerConfig, InternalNodeManager nodeManager, QueryManager queryManager, ClusterMemoryManager clusterMemoryManager)
    {
        this.isIncludeCoordinator = requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.clusterMemoryManager = requireNonNull(clusterMemoryManager, "clusterMemoryManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterStats getClusterStats()
    {
        long runningQueries = 0;
        long blockedQueries = 0;
        long queuedQueries = 0;

        long activeNodes = nodeManager.getNodes(NodeState.ACTIVE).size();
        if (!isIncludeCoordinator) {
            activeNodes -= 1;
        }

        long runningDrivers = 0;
        double memoryReservation = 0;

        long totalInputRows = queryManager.getStats().getConsumedInputRows().getTotalCount();
        long totalInputBytes = queryManager.getStats().getConsumedInputBytes().getTotalCount();
        long totalCpuTimeSecs = queryManager.getStats().getConsumedCpuTimeSecs().getTotalCount();

        for (QueryInfo query : queryManager.getAllQueryInfo()) {
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
            }
        }

        return new ClusterStats(runningQueries, blockedQueries, queuedQueries, activeNodes, runningDrivers, memoryReservation, totalInputRows, totalInputBytes, totalCpuTimeSecs);
    }

    @GET
    @Path("workerMemory")
    public Response getWorkerMemoryInfo()
    {
        return Response.ok()
                .entity(clusterMemoryManager.getWorkerMemoryInfo())
                .build();
    }

    public static class ClusterStats
    {
        private final long runningQueries;
        private final long blockedQueries;
        private final long queuedQueries;

        private final long activeWorkers;
        private final long runningDrivers;
        private final double reservedMemory;

        private final long totalInputRows;
        private final long totalInputBytes;
        private final long totalCpuTimeSecs;

        @JsonCreator
        public ClusterStats(
                @JsonProperty("runningQueries") long runningQueries,
                @JsonProperty("blockedQueries") long blockedQueries,
                @JsonProperty("queuedQueries") long queuedQueries,
                @JsonProperty("activeWorkers") long activeWorkers,
                @JsonProperty("runningDrivers") long runningDrivers,
                @JsonProperty("reservedMemory") double reservedMemory,
                @JsonProperty("totalInputRows") long totalInputRows,
                @JsonProperty("totalInputBytes") long totalInputBytes,
                @JsonProperty("totalCpuTimeSecs") long totalCpuTimeSecs)
        {
            this.runningQueries = runningQueries;
            this.blockedQueries = blockedQueries;
            this.queuedQueries = queuedQueries;
            this.activeWorkers = activeWorkers;
            this.runningDrivers = runningDrivers;
            this.reservedMemory = reservedMemory;
            this.totalInputRows = totalInputRows;
            this.totalInputBytes = totalInputBytes;
            this.totalCpuTimeSecs = totalCpuTimeSecs;
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
    }
}
