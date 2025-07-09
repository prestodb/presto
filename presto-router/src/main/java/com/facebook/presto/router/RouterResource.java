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
package com.facebook.presto.router;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.RequestInfo;
import com.google.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.IOException;
import java.net.URI;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.BAD_GATEWAY;
import static java.util.Objects.requireNonNull;

@Path("/")
public class RouterResource
{
    private static final Logger log = Logger.get(RouterResource.class);
    private final ClusterManager clusterManager;
    private static final CounterStat successRedirectRequests = new CounterStat();
    private static final CounterStat failedRedirectRequests = new CounterStat();

    @Inject
    public RouterResource(ClusterManager clusterManager)
    {
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
    }

    @POST
    @Path("/v1/statement")
    @Produces(APPLICATION_JSON)
    public Response routeQuery(String statement, @Context HttpServletRequest servletRequest)
    {
        RequestInfo requestInfo = new RequestInfo(servletRequest, statement);
        URI coordinatorUri = clusterManager.getDestination(requestInfo).orElseThrow(() -> badRequest(BAD_GATEWAY, "No Presto cluster available"));
        URI statementUri = uriBuilderFrom(coordinatorUri).replacePath("/v1/statement").build();
        successRedirectRequests.update(1);
        log.info("route query to %s", statementUri);
        return Response.temporaryRedirect(statementUri).build();
    }

    @GET
    @Path("/")
    public void rootRedirect(@Context HttpServletRequest servletRequest, @Context HttpServletResponse servletResponse)
            throws IOException
    {
        log.info("redirecting to UI");
        servletResponse.sendRedirect("ui");
    }

    private static WebApplicationException badRequest(Response.Status status, String message)
    {
        failedRedirectRequests.update(1);
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    @Managed
    @Nested
    public CounterStat getFailedRedirectRequests()
    {
        return failedRedirectRequests;
    }

    @Managed
    @Nested
    public CounterStat getSuccessRedirectRequests()
    {
        return successRedirectRequests;
    }
}
