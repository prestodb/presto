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
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/queryState")
@RolesAllowed({USER, ADMIN})
public class DistributedQueryInfoResource
{
    private static final Logger log = Logger.get(DistributedQueryInfoResource.class);
    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final InternalNodeManager internalNodeManager;
    private final ListeningExecutorService executor;
    private final ResourceManagerProxy proxyHelper;
    private final JsonCodec<List<QueryStateInfo>> jsonCodec;
    private final HttpClient httpClient;

    @Inject
    public DistributedQueryInfoResource(ResourceManagerClusterStateProvider clusterStateProvider, InternalNodeManager internalNodeManager,
            @ForResourceManager ListeningExecutorService executor, ResourceManagerProxy proxyHelper, JsonCodec<List<QueryStateInfo>> jsonCodec,
            @ForResourceManager HttpClient httpClient)
    {
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "clusterStateProvider is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.proxyHelper = requireNonNull(proxyHelper, "proxyHelper is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @GET
    public void getAllQueryInfo(@QueryParam("user") String user,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        try {
            Set<InternalNode> coordinators = internalNodeManager.getCoordinators();
            ImmutableList.Builder<ListenableFuture<List<QueryStateInfo>>> queryStateInfoFutureBuilder = ImmutableList.builder();
            for (InternalNode coordinator : coordinators) {
                queryStateInfoFutureBuilder.add(getQueryStateFromCoordinator(uriInfo, coordinator));
            }
            List<ListenableFuture<List<QueryStateInfo>>> queryStateInfoFutureList = queryStateInfoFutureBuilder.build();
            Futures.whenAllComplete(queryStateInfoFutureList).call(() -> {
                try {
                    List<QueryStateInfo> queryStateInfoList = new ArrayList<>();
                    for (Future<List<QueryStateInfo>> queryStateInfoFuture : queryStateInfoFutureList) {
                        queryStateInfoList.addAll(queryStateInfoFuture.get());
                    }
                    return asyncResponse.resume(Response.ok(queryStateInfoList).build());
                }
                catch (Exception ex) {
                    log.error(ex, "Error in getting query info from one of the coordinators");
                    return asyncResponse.resume(Response.serverError().entity(ex.getMessage()).build());
                }
            }, executor);
        }
        catch (Exception ex) {
            log.error(ex, "Error in getting query info");
            asyncResponse.resume(Response.serverError().entity(ex.getMessage()).build());
        }
    }

    @GET
    @Path("{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryStateInfo(@PathParam("queryId") QueryId queryId,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
            throws WebApplicationException
    {
        proxyQueryInfoResponse(servletRequest, asyncResponse, uriInfo, queryId);
    }

    private ListenableFuture<List<QueryStateInfo>> getQueryStateFromCoordinator(UriInfo uriInfo, InternalNode coordinatorNode)
            throws IOException
    {
        URI uri = uriInfo.getRequestUriBuilder()
                .queryParam("includeLocalQueryOnly", true)
                .scheme(coordinatorNode.getInternalUri().getScheme())
                .host(coordinatorNode.getHostAndPort().toInetAddress().getHostName())
                .port(coordinatorNode.getInternalUri().getPort())
                .build();

        Request request = prepareGet().setUri(uri).build();
        return httpClient.executeAsync(request, createJsonResponseHandler(jsonCodec));
    }

    private void proxyQueryInfoResponse(HttpServletRequest servletRequest, AsyncResponse asyncResponse, UriInfo uriInfo, QueryId queryId)
    {
        Optional<BasicQueryInfo> queryInfo = clusterStateProvider.getClusterQueries().stream()
                .filter(query -> query.getQueryId().equals(queryId))
                .findFirst();

        if (!queryInfo.isPresent()) {
            asyncResponse.resume(Response.status(NOT_FOUND).type(MediaType.APPLICATION_JSON).build());
            return;
        }

        proxyHelper.performRequest(servletRequest, asyncResponse, uriBuilderFrom(queryInfo.get().getSelf()).replacePath(uriInfo.getPath()).build());
    }
}
