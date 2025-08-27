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

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.NodeStats;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import java.util.Optional;

import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_BINARY;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_COMPACT;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_FB_COMPACT;
import static com.facebook.airlift.units.Duration.nanosSince;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.INACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Path("/v1/info")
public class ServerInfoResource
{
    private final NodeVersion version;
    private final String environment;
    private final boolean coordinator;
    private final boolean resourceManager;
    private final StaticCatalogStore catalogStore;
    private final GracefulShutdownHandler shutdownHandler;
    private final long startTime = System.nanoTime();
    private final NodeResourceStatusProvider nodeResourceStatusProvider;
    private final ResourceGroupManager resourceGroupManager;
    private NodeState nodeState = ACTIVE;

    @Inject
    public ServerInfoResource(NodeVersion nodeVersion, NodeInfo nodeInfo, ServerConfig serverConfig, StaticCatalogStore catalogStore, GracefulShutdownHandler shutdownHandler, NodeResourceStatusProvider nodeResourceStatusProvider, ResourceGroupManager resourceGroupManager)
    {
        this.version = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.coordinator = requireNonNull(serverConfig, "serverConfig is null").isCoordinator();
        this.resourceManager = serverConfig.isResourceManager();
        this.catalogStore = requireNonNull(catalogStore, "catalogStore is null");
        this.shutdownHandler = requireNonNull(shutdownHandler, "shutdownHandler is null");
        this.nodeResourceStatusProvider = requireNonNull(nodeResourceStatusProvider, "nodeResourceStatusProvider is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
    }

    @GET
    @Produces({APPLICATION_JSON, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    public ServerInfo getInfo()
    {
        boolean starting = resourceManager ? true : !catalogStore.areCatalogsLoaded();
        return new ServerInfo(version, environment, coordinator, starting, Optional.of(nanosSince(startTime)));
    }

    @PUT
    @Path("state")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    @RolesAllowed(ADMIN)
    public Response updateState(NodeState state)
            throws WebApplicationException
    {
        requireNonNull(state, "state is null");
        switch (state) {
            case SHUTTING_DOWN:
                shutdownHandler.requestShutdown();
                return Response.ok().build();
            case ACTIVE:
            case INACTIVE:
                if (shutdownHandler.isShutdownRequested()) {
                    return Response.status(BAD_REQUEST)
                            .type(TEXT_PLAIN)
                            .entity("Cluster is shutting down")
                            .build();
                }
                nodeState = state;
                return Response.ok().build();
            default:
                return Response.status(BAD_REQUEST)
                        .type(TEXT_PLAIN)
                        .entity(format("Invalid state %s", state))
                        .build();
        }
    }

    @GET
    @Path("state")
    @Produces({APPLICATION_JSON, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    @RolesAllowed(ADMIN)
    public NodeState getServerState()
    {
        if (shutdownHandler.isShutdownRequested()) {
            return SHUTTING_DOWN;
        }
        else if (!nodeResourceStatusProvider.hasResources() || !resourceGroupManager.isConfigurationManagerLoaded()) {
            return INACTIVE;
        }
        else {
            return nodeState;
        }
    }

    @GET
    @Path("stats")
    @Produces({APPLICATION_JSON, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    @RolesAllowed(ADMIN)
    public NodeStats getServerStats()
    {
        NodeStats stats = new NodeStats(getServerState(), null);
        return stats;
    }

    @GET
    @Path("coordinator")
    @Produces(TEXT_PLAIN)
    @RolesAllowed(ADMIN)
    public Response getServerCoordinator()
    {
        if (coordinator) {
            return Response.ok().build();
        }
        // return 404 to allow load balancers to only send traffic to the coordinator
        return Response.status(Response.Status.NOT_FOUND).build();
    }
}
