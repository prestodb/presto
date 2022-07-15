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
package com.facebook.presto.router.cluster;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.client.ServerInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;

import javax.annotation.concurrent.Immutable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.client.NodeVersion.UNKNOWN;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/")
public class ClusterStatusResource
{
    private final String environment;
    private final ClusterStatusTracker clusterStatusTracker;
    private final ClusterManager clusterManager;

    @Inject
    public ClusterStatusResource(NodeInfo nodeInfo, ClusterStatusTracker clusterStatusTracker, ClusterManager clusterManager)
    {
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.clusterStatusTracker = requireNonNull(clusterStatusTracker, "clusterStatusTracker is null");
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
    }

    // The web UI depend on the following service endpoints.
    @GET
    @Path("/v1/info")
    @Produces(APPLICATION_JSON)
    public ServerInfo getInfo()
    {
        return new ServerInfo(UNKNOWN, environment, true, false, Optional.empty());
    }

    @GET
    @Path("/v1/cluster")
    @Produces(APPLICATION_JSON)
    public ClusterStats getClusterStats()
    {
        return new ClusterStats(
                clusterStatusTracker.getRunningQueries(),
                clusterStatusTracker.getBlockedQueries(),
                clusterStatusTracker.getQueuedQueries(),
                clusterStatusTracker.getClusterCount(),
                clusterStatusTracker.getActiveWorkers(),
                clusterStatusTracker.getRunningDrivers());
    }

    @GET
    @Path("/v1/query")
    public List<JsonNode> getAllQueryInfo(@QueryParam("state") String stateFilter)
    {
        return clusterStatusTracker.getAllQueryInfos();
    }

    @GET
    @Path("/v1/all")
    @Produces(APPLICATION_JSON)
    public List<URI> getAllClusters()
    {
        return clusterManager.getAllClusters();
    }

    @Immutable
    public static class ClusterStats
    {
        private final long runningQueries;
        private final long blockedQueries;
        private final long queuedQueries;
        private final long clusterCount;
        private final long activeWorkers;
        private final long runningDrivers;

        @JsonCreator
        public ClusterStats(
                @JsonProperty("runningQueries") long runningQueries,
                @JsonProperty("blockedQueries") long blockedQueries,
                @JsonProperty("queuedQueries") long queuedQueries,
                @JsonProperty("clusterCount") long clusterCount,
                @JsonProperty("activeWorkers") long activeWorkers,
                @JsonProperty("runningDrivers") long runningDrivers)
        {
            this.runningQueries = runningQueries;
            this.blockedQueries = blockedQueries;
            this.queuedQueries = queuedQueries;
            this.clusterCount = clusterCount;
            this.activeWorkers = activeWorkers;
            this.runningDrivers = runningDrivers;
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
        public long getClusterCount()
        {
            return clusterCount;
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
    }
}
