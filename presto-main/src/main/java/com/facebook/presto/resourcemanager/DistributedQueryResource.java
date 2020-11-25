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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.ForQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/query")
@RolesAllowed({USER, ADMIN})
public class DistributedQueryResource
{
    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final HttpClient httpClient;

    @Inject
    public DistributedQueryResource(ResourceManagerClusterStateProvider clusterStateProvider, @ForQueryInfo HttpClient httpClient)
    {
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "nodeStateManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @GET
    public List<BasicQueryInfo> getAllQueryInfo(@QueryParam("state") String stateFilter)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));
        if (stateFilter == null) {
            return clusterStateProvider.getQueryInfos();
        }
        else {
            ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
            for (BasicQueryInfo queryInfo : clusterStateProvider.getQueryInfos()) {
                if (queryInfo.getState() == expectedState) {
                    builder.add(queryInfo);
                }
            }
            return builder.build();
        }
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId)
    {
        return proxyResponse(queryId, "GET", "");
    }

    @DELETE
    @Path("{queryId}")
    public Response cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        return proxyResponse(queryId, "DELETE", "");
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        return proxyResponse(queryId, "PUT", "killed");
    }

    @PUT
    @Path("{queryId}/preempted")
    public Response preemptQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        return proxyResponse(queryId, "PUT", "preempted");
    }

    // TODO: add a trace from original client IP address
    private Response proxyResponse(QueryId queryId, String httpMethod, String additionalPath)
    {
        Optional<BasicQueryInfo> queryInfo = clusterStateProvider.getQueryInfos().stream()
                .filter(query -> query.getQueryId().equals(queryId))
                .findFirst();

        if (!queryInfo.isPresent()) {
            return Response.status(NOT_FOUND).build();
        }

        Request request = new Request.Builder()
                .setMethod(httpMethod)
                .setUri(uriBuilderFrom(queryInfo.get().getSelf()).appendPath(additionalPath).build())
                .build();
        InputStream responseStream = httpClient.execute(request, new StreamingJsonResponseHandler());
        return Response.ok(responseStream, APPLICATION_JSON_TYPE).build();
    }

    private static class StreamingJsonResponseHandler
            implements ResponseHandler<InputStream, RuntimeException>
    {
        @Override
        public InputStream handleException(Request request, Exception exception)
        {
            throw new RuntimeException("Request to coordinator failed", exception);
        }

        @Override
        public InputStream handle(Request request, com.facebook.airlift.http.client.Response response)
        {
            try {
                if (APPLICATION_JSON.equals(response.getHeader(CONTENT_TYPE))) {
                    return response.getInputStream();
                }
                throw new RuntimeException("Response received was not of type " + APPLICATION_JSON);
            }
            catch (IOException e) {
                throw new RuntimeException("Unable to read response from coordinator", e);
            }
        }
    }
}
