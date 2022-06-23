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

import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourcemanager.ResourceManagerProxy;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

@Path("/v1/resourceGroupState")
@RolesAllowed(ADMIN)
public class ResourceGroupStateInfoResource
{
    private final ResourceGroupManager<?> resourceGroupManager;
    private final boolean resourceManagerEnabled;
    private final InternalNodeManager internalNodeManager;
    private final Optional<ResourceManagerProxy> proxyHelper;

    @Inject
    public ResourceGroupStateInfoResource(
            ServerConfig serverConfig,
            ResourceGroupManager<?> resourceGroupManager,
            InternalNodeManager internalNodeManager,
            Optional<ResourceManagerProxy> proxyHelper)
    {
        this.resourceManagerEnabled = requireNonNull(serverConfig, "serverConfig is null").isResourceManagerEnabled();
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.proxyHelper = requireNonNull(proxyHelper, "proxyHelper is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Encoded
    @Path("{resourceGroupId: .+}")
    public void getResourceGroupInfos(
            @PathParam("resourceGroupId") String resourceGroupIdString,
            @QueryParam("includeQueryInfo") @DefaultValue("true") boolean includeQueryInfo,
            @QueryParam("includeLocalInfoOnly") @DefaultValue("false") boolean includeLocalInfoOnly,
            @QueryParam("summarizeSubgroups") @DefaultValue("true") boolean summarizeSubgroups,
            @QueryParam("includeStaticSubgroupsOnly") @DefaultValue("false") boolean includeStaticSubgroupsOnly,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (resourceManagerEnabled && !includeLocalInfoOnly) {
            proxyResourceGroupInfoResponse(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            return;
        }
        if (!isNullOrEmpty(resourceGroupIdString)) {
            try {
                asyncResponse.resume(Response.ok().entity(resourceGroupManager.getResourceGroupInfo(
                        new ResourceGroupId(
                                Arrays.stream(resourceGroupIdString.split("/"))
                                        .map(ResourceGroupStateInfoResource::urlDecode)
                                        .collect(toImmutableList())),
                        includeQueryInfo,
                        summarizeSubgroups,
                        includeStaticSubgroupsOnly)).build());
            }
            catch (NoSuchElementException | IllegalArgumentException e) {
                asyncResponse.resume(Response.status(NOT_FOUND).build());
            }
        }
        asyncResponse.resume(Response.status(NOT_FOUND).build());
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

    //TODO move this to a common place and reuse in all resource
    private void proxyResourceGroupInfoResponse(HttpServletRequest servletRequest, AsyncResponse asyncResponse, String xForwardedProto, UriInfo uriInfo)
    {
        try {
            checkState(proxyHelper.isPresent());
            Iterator<InternalNode> resourceManagers = internalNodeManager.getResourceManagers().iterator();
            if (!resourceManagers.hasNext()) {
                asyncResponse.resume(Response.status(SERVICE_UNAVAILABLE).build());
                return;
            }
            InternalNode resourceManagerNode = resourceManagers.next();

            URI uri = uriInfo.getRequestUriBuilder()
                    .scheme(resourceManagerNode.getInternalUri().getScheme())
                    .host(resourceManagerNode.getHostAndPort().toInetAddress().getHostName())
                    .port(resourceManagerNode.getInternalUri().getPort())
                    .build();
            proxyHelper.get().performRequest(servletRequest, asyncResponse, uri);
        }
        catch (Exception e) {
            asyncResponse.resume(e);
        }
    }
}
