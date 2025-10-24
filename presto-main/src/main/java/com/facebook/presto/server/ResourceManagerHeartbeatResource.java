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

import com.facebook.presto.resourcemanager.ForResourceManager;
import com.facebook.presto.resourcemanager.ResourceManagerClusterStateProvider;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/heartbeat")
public class ResourceManagerHeartbeatResource
{
    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final ListeningExecutorService executor;

    @Inject
    public ResourceManagerHeartbeatResource(
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
    public void nodeHeartbeat(NodeStatus nodeStatus)
    {
        executor.execute(() -> clusterStateProvider.registerNodeHeartbeat(nodeStatus));
    }
}
