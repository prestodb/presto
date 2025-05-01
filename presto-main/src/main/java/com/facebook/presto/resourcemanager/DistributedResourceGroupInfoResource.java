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
import com.facebook.airlift.http.client.JsonResponseHandler;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.UnexpectedResponseException;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.google.common.base.Strings.isNullOrEmpty;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static java.util.Objects.requireNonNull;

@Path("/v1/resourceGroupState")
@RolesAllowed(ADMIN)
public class DistributedResourceGroupInfoResource
{
    private static final Logger log = Logger.get(DistributedResourceGroupInfoResource.class);
    private final InternalNodeManager internalNodeManager;
    private final ListeningExecutorService executor;
    private final HttpClient httpClient;
    private final JsonCodec<ResourceGroupInfo> jsonCodec;
    private final JsonCodec<List<ResourceGroupInfo>> jsonCodecRootGroups;

    @Inject
    public DistributedResourceGroupInfoResource(InternalNodeManager internalNodeManager,
            @ForResourceManager ListeningExecutorService executor, @ForResourceManager HttpClient httpClient,
            JsonCodec<ResourceGroupInfo> jsonCodec, JsonCodec<List<ResourceGroupInfo>> jsonCodecRootGroups)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.jsonCodecRootGroups = requireNonNull(jsonCodecRootGroups, "jsonCodecRootGroups is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Encoded
    @Path("{resourceGroupId: .*}")
    public void getResourceGroupInfos(
            @PathParam("resourceGroupId") String resourceGroupIdString,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        try {
            if (isNullOrEmpty(resourceGroupIdString)) {
                // when no resourceGroupId is specified, query all root groups from the coordinators
                ImmutableList.Builder<ListenableFuture<List<ResourceGroupInfo>>> resourceGroupInfoFutureBuilder = ImmutableList.builder();
                for (InternalNode coordinator : internalNodeManager.getCoordinators()) {
                    resourceGroupInfoFutureBuilder.add(
                            getResourceGroupInfoFromCoordinator(uriInfo, coordinator, createJsonResponseHandler(jsonCodecRootGroups)));
                }
                List<ListenableFuture<List<ResourceGroupInfo>>> resourceGroupInfoFutureList = resourceGroupInfoFutureBuilder.build();
                Futures.whenAllComplete(resourceGroupInfoFutureList).call(() -> {
                    try {
                        List<ResourceGroupInfo> aggregatedRootGroups = aggregateRootGroups(resourceGroupInfoFutureList);
                        if (aggregatedRootGroups == null) {
                            return asyncResponse.resume(Response.status(NOT_FOUND).build());
                        }
                        return asyncResponse.resume(Response.ok(aggregatedRootGroups).build());
                    }
                    catch (Exception ex) {
                        log.error(ex, "Error in getting root resource groups info from one of the coordinators");
                        return asyncResponse.resume(Response.serverError().entity(ex.getMessage()).build());
                    }
                }, executor);
            }
            else {
                ImmutableList.Builder<ListenableFuture<ResourceGroupInfo>> resourceGroupInfoFutureBuilder = ImmutableList.builder();
                for (InternalNode coordinator : internalNodeManager.getCoordinators()) {
                    resourceGroupInfoFutureBuilder.add(getResourceGroupInfoFromCoordinator(uriInfo, coordinator, createJsonResponseHandler(jsonCodec)));
                }
                List<ListenableFuture<ResourceGroupInfo>> resourceGroupInfoFutureList = resourceGroupInfoFutureBuilder.build();
                Futures.whenAllComplete(resourceGroupInfoFutureList).call(() -> {
                    try {
                        ResourceGroupInfo aggregatedResourceGroupInfo = aggregateResourceGroupInfo(resourceGroupInfoFutureList);
                        if (aggregatedResourceGroupInfo == null) {
                            return asyncResponse.resume(Response.status(NOT_FOUND).build());
                        }
                        return asyncResponse.resume(Response.ok(aggregatedResourceGroupInfo).build());
                    }
                    catch (Exception ex) {
                        log.error(ex, "Error in getting resource group info from one of the coordinators");
                        return asyncResponse.resume(Response.serverError().entity(ex.getMessage()).build());
                    }
                }, executor);
            }
        }
        catch (IOException ex) {
            log.error(ex, "Error in getting resource group info");
            asyncResponse.resume(Response.serverError().entity(ex.getMessage()).build());
        }
    }

    private ResourceGroupInfo aggregateResourceGroupInfo(List<ListenableFuture<ResourceGroupInfo>> queryStateInfoFutureList)
            throws InterruptedException, ExecutionException

    {
        Iterator<ListenableFuture<ResourceGroupInfo>> iterator = queryStateInfoFutureList.iterator();
        AggregatedResourceGroupInfoBuilder builder = new AggregatedResourceGroupInfoBuilder();
        while (iterator.hasNext()) {
            try {
                builder.add(iterator.next().get());
            }
            catch (ExecutionException e) {
                Throwable exceptionCause = e.getCause();
                //airlift JsonResponseHandler throws UnexpectedResponseException for cases where http status code != 2xx
                if (!(exceptionCause instanceof UnexpectedResponseException) ||
                        ((UnexpectedResponseException) exceptionCause).getStatusCode() != NOT_FOUND.getStatusCode()) {
                    throw e;
                }
            }
        }
        return builder.build();
    }

    private List<ResourceGroupInfo> aggregateRootGroups(List<ListenableFuture<List<ResourceGroupInfo>>> rootGroupsFutureList)
            throws InterruptedException, ExecutionException

    {
        Map<ResourceGroupId, AggregatedResourceGroupInfoBuilder> groupAggregators = new HashMap<>();
        // Iterator through all root resource groups from all coordinators and aggregate each resource group.
        for (ListenableFuture<List<ResourceGroupInfo>> rootGroupsFuture : rootGroupsFutureList) {
            try {
                for (ResourceGroupInfo rootGroup : rootGroupsFuture.get()) {
                    ResourceGroupId gid = rootGroup.getId();
                    AggregatedResourceGroupInfoBuilder builder = Optional.ofNullable(groupAggregators.get(gid))
                            .orElseGet(() -> new AggregatedResourceGroupInfoBuilder());

                    builder.add(rootGroup);
                    groupAggregators.put(gid, builder);
                }
            }
            catch (ExecutionException e) {
                Throwable exceptionCause = e.getCause();
                //airlift JsonResponseHandler throws UnexpectedResponseException for cases where http status code != 2xx
                if (!(exceptionCause instanceof UnexpectedResponseException) ||
                        ((UnexpectedResponseException) exceptionCause).getStatusCode() != NOT_FOUND.getStatusCode()) {
                    throw e;
                }
            }
        }
        return groupAggregators.values().stream().map(agg -> agg.build()).collect(Collectors.toList());
    }

    private <T> ListenableFuture<T> getResourceGroupInfoFromCoordinator(UriInfo uriInfo,
            InternalNode coordinatorNode, JsonResponseHandler<T> handler) throws IOException

    {
        String scheme = uriInfo.getRequestUri().getScheme();
        URI uri = uriInfo.getRequestUriBuilder()
                .queryParam("includeLocalInfoOnly", true)
                .scheme(scheme)
                .host(coordinatorNode.getHostAndPort().toInetAddress().getHostName())
                .port(coordinatorNode.getInternalUri().getPort())
                .build();
        Request request = prepareGet().setUri(uri).build();
        return httpClient.executeAsync(request, handler);
    }
}
