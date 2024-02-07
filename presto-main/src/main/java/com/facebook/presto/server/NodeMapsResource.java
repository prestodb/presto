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
import com.facebook.presto.execution.scheduler.FixedSubsetNodeSetSupplier;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.QueryId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static java.lang.String.format;

@Path("/v1/node-map")
@RolesAllowed({ADMIN, USER})
public class NodeMapsResource
{
    private static final Logger log = Logger.get(NodeMapsResource.class);

    private final FixedSubsetNodeSetSupplier fixedSubsetNodeSetSupplier;
    @Inject
    public NodeMapsResource(
            InternalNodeManager internalNodeManager,
            FixedSubsetNodeSetSupplier fixedSubsetNodeSetSupplier)
    {
        this.fixedSubsetNodeSetSupplier = fixedSubsetNodeSetSupplier;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/debug")
    public NodeMaps getDebugInfo()
    {
        // nodes acquired per query
        ImmutableMap.Builder<String, Integer> queryNodeMaps = ImmutableMap.builder();
        for (Map.Entry<QueryId, List<InternalNode>> entry : fixedSubsetNodeSetSupplier.getQueryNodesMap().entrySet()) {
            queryNodeMaps.put(
                    format("%s[%s]",
                            entry.getKey().toString(),
                            fixedSubsetNodeSetSupplier.getQueryStateMap().get(entry.getKey()).get().toString()),
                    entry.getValue().size());
        }

        return new NodeMaps(
                fixedSubsetNodeSetSupplier.getPendingRequests().stream()
                        .map(FixedSubsetNodeSetSupplier.NodeSetAcquireRequest::toString)
                        .collect(Collectors.toList()),
                queryNodeMaps.build(),
                fixedSubsetNodeSetSupplier.computeFreeNodesInCluster().size());
    }

    public static class NodeMaps
    {
        private final List<String> pendingRequests;
        private final Map<String, Integer> queryNodesMap;
        private final long idleNodeCount;

        @JsonCreator
        public NodeMaps(
                @JsonProperty("pendingRequests") List<String> pendingRequests,
                @JsonProperty("queryNodesMap") Map<String, Integer> queryNodesMap,
                @JsonProperty("idleNodeCount") long idleNodeCount)
        {
            this.pendingRequests = pendingRequests;
            this.queryNodesMap = queryNodesMap;
            this.idleNodeCount = idleNodeCount;
        }

        @JsonProperty
        public List<String> getPendingRequests()
        {
            return pendingRequests;
        }

        @JsonProperty
        public Map<String, Integer> getQueryNodesMap()
        {
            return queryNodesMap;
        }

        @JsonProperty
        public long getIdleNodeCount()
        {
            return idleNodeCount;
        }
    }
}
