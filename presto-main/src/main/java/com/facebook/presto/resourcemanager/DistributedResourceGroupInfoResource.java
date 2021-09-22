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
import com.facebook.airlift.http.client.UnexpectedResponseException;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.ResourceGroupInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/resourceGroupState")
@RolesAllowed(ADMIN)
public class DistributedResourceGroupInfoResource
{
    private static final Logger log = Logger.get(DistributedResourceGroupInfoResource.class);
    private final InternalNodeManager internalNodeManager;
    private final ListeningExecutorService executor;
    private final HttpClient httpClient;
    private final JsonCodec<ResourceGroupInfo> jsonCodec;

    @Inject
    public DistributedResourceGroupInfoResource(InternalNodeManager internalNodeManager,
            @ForResourceManager ListeningExecutorService executor, @ForResourceManager HttpClient httpClient, JsonCodec<ResourceGroupInfo> jsonCodec)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Encoded
    @Path("{resourceGroupId: .+}")
    public void getResourceGroupInfos(
            @PathParam("resourceGroupId") String resourceGroupIdString,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (isNullOrEmpty(resourceGroupIdString)) {
            asyncResponse.resume(Response.status(NOT_FOUND).build());
        }
        try {
            ImmutableList.Builder<ListenableFuture<ResourceGroupInfo>> resourceGroupInfoFutureBuilder = ImmutableList.builder();
            for (InternalNode coordinator : internalNodeManager.getCoordinators()) {
                resourceGroupInfoFutureBuilder.add(getResourceGroupInfoFromCoordinator(uriInfo, coordinator));
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

    private ListenableFuture<ResourceGroupInfo> getResourceGroupInfoFromCoordinator(UriInfo uriInfo,
            InternalNode coordinatorNode)
            throws IOException
    {
        String scheme = uriInfo.getRequestUri().getScheme();
        URI uri = uriInfo.getRequestUriBuilder()
                .queryParam("includeLocalInfoOnly", true)
                .scheme(scheme)
                .host(coordinatorNode.getHostAndPort().toInetAddress().getHostName())
                .port(coordinatorNode.getInternalUri().getPort())
                .build();
        Request request = prepareGet().setUri(uri).build();
        return httpClient.executeAsync(request, createJsonResponseHandler(jsonCodec));
    }
}
