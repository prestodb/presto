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

import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.spi.NodeState;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/info")
public class ServerInfoResource
{
    private final ServerInfo serverInfo;
    private final GracefulShutdownHandler shutdownHandler;

    @Inject
    public ServerInfoResource(ServerInfo serverInfo, GracefulShutdownHandler shutdownHandler)
    {
        this.serverInfo = requireNonNull(serverInfo, "serverInfo is null");
        this.shutdownHandler = requireNonNull(shutdownHandler, "shutdownHandler is null");
    }

    @GET
    @Produces(APPLICATION_JSON)
    public ServerInfo getServerInfo()
    {
        return serverInfo;
    }

    @PUT
    @Path("state")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response updateState(NodeState state)
            throws WebApplicationException
    {
        switch (state) {
            case SHUTTING_DOWN:
                shutdownHandler.requestShutdown();
                return Response.ok().build();
            case ACTIVE:
            case INACTIVE:
                throw new WebApplicationException(Response
                        .status(BAD_REQUEST)
                        .type(MediaType.TEXT_PLAIN)
                        .entity(format("Invalid state transition to %s", state))
                        .build());
            default:
                return Response.status(BAD_REQUEST)
                        .type(TEXT_PLAIN)
                        .entity(format("Invalid state %s", state))
                        .build();
        }
    }

    @GET
    @Path("state")
    @Produces(APPLICATION_JSON)
    public NodeState getServerState()
    {
        if (shutdownHandler.isShutdownRequested()) {
            return SHUTTING_DOWN;
        }
        else {
            return ACTIVE;
        }
    }

    @GET
    @Path("coordinator")
    @Produces(TEXT_PLAIN)
    public Response getServerCoordinator()
    {
        if (serverInfo.isCoordinator()) {
            return Response.ok().build();
        }
        // return 404 to allow load balancers to only send traffic to the coordinator
        return Response.status(Response.Status.NOT_FOUND).build();
    }
}
