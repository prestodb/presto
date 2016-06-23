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
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodeState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/cluster")
public class ClusterStatsResource
{
    private final NodeManager nodeManager;
    private final QueryManager queryManager;

    @Inject
    public ClusterStatsResource(NodeManager nodeManager, QueryManager queryManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterStats getClusterStats()
    {
        long runningQueries = 0;
        long blockedQueries = 0;
        long queuedQueries = 0;

        long activeNodes = nodeManager.getNodes(NodeState.ACTIVE).size();
        long runningDrivers = 0;
        double memoryReservation = 0;

        double rowInputRate = 0;
        double byteInputRate = 0;
        double cpuTimeRate = 0;

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
                double totalExecutionTimeSeconds = query.getQueryStats().getElapsedTime().getValue(SECONDS);
                if (totalExecutionTimeSeconds != 0) {
                    byteInputRate += query.getQueryStats().getProcessedInputDataSize().toBytes() / totalExecutionTimeSeconds;
                    rowInputRate += query.getQueryStats().getProcessedInputPositions() / totalExecutionTimeSeconds;
                    cpuTimeRate += (query.getQueryStats().getTotalCpuTime().getValue(SECONDS)) / totalExecutionTimeSeconds;
                }
                memoryReservation += query.getQueryStats().getTotalMemoryReservation().toBytes();
                runningDrivers += query.getQueryStats().getRunningDrivers();
            }
        }

        return new ClusterStats(runningQueries, blockedQueries, queuedQueries, activeNodes, runningDrivers, memoryReservation, rowInputRate, byteInputRate, cpuTimeRate);
    }

    public static class ClusterStats
    {
        private final long runningQueries;
        private final long blockedQueries;
        private final long queuedQueries;

        private final long activeWorkers;
        private final long runningDrivers;
        private final double reservedMemory;

        private final double rowInputRate;
        private final double byteInputRate;
        private final double cpuTimeRate;

        @JsonCreator
        public ClusterStats(
                @JsonProperty("runningQueries") long runningQueries,
                @JsonProperty("blockedQueries") long blockedQueries,
                @JsonProperty("queuedQueries") long queuedQueries,
                @JsonProperty("activeWorkers") long activeWorkers,
                @JsonProperty("runningDrivers") long runningDrivers,
                @JsonProperty("reservedMemory") double reservedMemory,
                @JsonProperty("rowInputRate") double rowInputRate,
                @JsonProperty("byteInputRate") double byteInputRate,
                @JsonProperty("cpuTimeRate") double cpuTimeRate)
        {
            this.runningQueries = runningQueries;
            this.blockedQueries = blockedQueries;
            this.queuedQueries = queuedQueries;
            this.activeWorkers = activeWorkers;
            this.runningDrivers = runningDrivers;
            this.reservedMemory = reservedMemory;
            this.rowInputRate = rowInputRate;
            this.byteInputRate = byteInputRate;
            this.cpuTimeRate = cpuTimeRate;
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
        public double getRowInputRate()
        {
            return rowInputRate;
        }

        @JsonProperty
        public double getByteInputRate()
        {
            return byteInputRate;
        }

        @JsonProperty
        public double getCpuTimeRate()
        {
            return cpuTimeRate;
        }
    }
}
