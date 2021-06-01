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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.ResourceGroupInfo;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/resourceGroupState")
@RolesAllowed(ADMIN)
public class DistributedResourceGroupInfoResource
{
    private static final Logger log = Logger.get(DistributedResourceGroupInfoResource.class);
    private final InternalNodeManager internalNodeManager;
    private final ResourceManagerProxy proxyHelper;
    private final ListeningExecutorService executor;
    private static final JsonCodec<ResourceGroupInfo> JSON_CODEC = JsonCodec.jsonCodec(ResourceGroupInfo.class);

    @Inject
    public DistributedResourceGroupInfoResource(InternalNodeManager internalNodeManager, @ForResourceManager ListeningExecutorService executor, ResourceManagerProxy proxyHelper)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "clusterStateProvider is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.proxyHelper = requireNonNull(proxyHelper, "proxyHelper is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Encoded
    @Path("{resourceGroupId: .+}")
    public void getResourceGroupInfos(
            @PathParam("resourceGroupId") String resourceGroupIdString,
            @QueryParam("includeQueryInfo") @DefaultValue("true") boolean includeQueryInfo,
            @QueryParam("summarizeSubgroups") @DefaultValue("true") boolean summarizeSubgroups,
            @QueryParam("includeStaticSubgroupsOnly") @DefaultValue("false") boolean includeStaticSubgroupsOnlytring,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (isNullOrEmpty(resourceGroupIdString)) {
            throw new WebApplicationException(NOT_FOUND);
        }
        try {
            List<ListenableFuture<ResourceGroupInfo>> queryStateInfoFutureList = new ArrayList<>();
            for (InternalNode coordinator : internalNodeManager.getCoordinators()) {
                queryStateInfoFutureList.add(getResourceGroupInfoFromCoordinator(servletRequest, xForwardedProto, uriInfo, coordinator));
            }
            Futures.whenAllComplete(queryStateInfoFutureList).call(() -> {
                try {
                    ResourceGroupInfo aggregatedResourceGroupInfo = aggregateResourceGroupInfo(queryStateInfoFutureList);
                    return asyncResponse.resume(Response.ok(aggregatedResourceGroupInfo).build());
                }
                catch (Exception ex) {
                    log.error(ex, "Error in getting resource group info from one of the coordinators");
                    return asyncResponse.resume(Response.serverError().entity(ex.getMessage()).build());
                }
            }, executor);
        }
        catch (NoSuchElementException e) {
            throw new WebApplicationException(NOT_FOUND);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ResourceGroupInfo aggregateResourceGroupInfo(List<ListenableFuture<ResourceGroupInfo>> queryStateInfoFutureList)
            throws InterruptedException, java.util.concurrent.ExecutionException
    {
        Iterator<ListenableFuture<ResourceGroupInfo>> iterator = queryStateInfoFutureList.iterator();
        if (iterator.hasNext()) {
            AggregatedResourceGroupInfoBuilder builder = new AggregatedResourceGroupInfoBuilder(iterator.next().get());
            while (iterator.hasNext()) {
                builder.add(iterator.next().get());
            }
            return builder.build();
        }
        return null;
    }

    private static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new WebApplicationException(BAD_REQUEST);
        }
    }

    private void proxyQueryInfoResponse(HttpServletRequest servletRequest, AsyncResponse asyncResponse, UriInfo uriInfo, URI remoteURI)
    {
        if (remoteURI == null) {
            asyncResponse.resume(Response.status(NOT_FOUND).type(MediaType.APPLICATION_JSON).build());
            return;
        }
        URI coordinatorURI = uriInfo.getRequestUriBuilder().queryParam("includeLocalQueryOnly", true).build();
        proxyHelper.performRequest(servletRequest, asyncResponse, uriBuilderFrom(remoteURI).replacePath(coordinatorURI.getPath()).build());
    }

    private ListenableFuture<ResourceGroupInfo> getResourceGroupInfoFromCoordinator(HttpServletRequest servletRequest, String xForwardedProto, UriInfo uriInfo, InternalNode coordinatorNode)
            throws IOException
    {
        String scheme = isNullOrEmpty(xForwardedProto) ? uriInfo.getRequestUri().getScheme() : xForwardedProto;
        URI uri = uriInfo.getRequestUriBuilder()
                .queryParam("includeLocalInfoOnly", true)
                .scheme(scheme)
                .host(coordinatorNode.getHostAndPort().toInetAddress().getHostName())
                .port(coordinatorNode.getInternalUri().getPort())
                .build();
        return proxyHelper.getResponse(servletRequest, uri, JSON_CODEC);
    }
}
