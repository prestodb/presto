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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourcemanager.ResourceManagerProxy;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static jakarta.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Path("/v1/resourceGroupState")
@RolesAllowed(ADMIN)
public class ResourceGroupStateInfoResource
{
    private static class ResourceGroupStateInfoKey
    {
        private final ResourceGroupId resourceGroupId;
        private final boolean includeQueryInfo;
        private final boolean summarizeSubGroups;
        private final boolean includeStaticSubgroupsOnly;

        public ResourceGroupStateInfoKey(ResourceGroupId resourceGroupId, boolean includeQueryInfo, boolean summarizeSubGroups, boolean includeStaticSubgroupsOnly)
        {
            this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
            this.includeQueryInfo = includeQueryInfo;
            this.summarizeSubGroups = summarizeSubGroups;
            this.includeStaticSubgroupsOnly = includeStaticSubgroupsOnly;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResourceGroupStateInfoKey that = (ResourceGroupStateInfoKey) o;
            return Objects.equals(that.resourceGroupId, resourceGroupId) &&
                    that.includeQueryInfo == includeQueryInfo &&
                    that.summarizeSubGroups == summarizeSubGroups &&
                    that.includeStaticSubgroupsOnly == includeStaticSubgroupsOnly;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resourceGroupId, includeQueryInfo, summarizeSubGroups, includeStaticSubgroupsOnly);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("resourceGroupId", resourceGroupId)
                    .add("includeQueryInfo", includeQueryInfo)
                    .add("summarizeSubGroups", summarizeSubGroups)
                    .add("includeStaticSubgroupsOnly", includeStaticSubgroupsOnly)
                    .toString();
        }
    }

    private final ResourceGroupManager<?> resourceGroupManager;
    private final boolean resourceManagerEnabled;
    private final InternalNodeManager internalNodeManager;
    private final Optional<ResourceManagerProxy> proxyHelper;
    private final Map<ResourceGroupStateInfoKey, Supplier<ResourceGroupInfo>> resourceGroupStateInfoKeySupplierMap;
    private final Supplier<List<ResourceGroupInfo>> rootResourceGroupInfoSupplier;
    private final Duration expirationDuration;

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
        this.resourceGroupStateInfoKeySupplierMap = new HashMap<>();
        this.expirationDuration = requireNonNull(serverConfig, "serverConfig is null").getClusterResourceGroupStateInfoExpirationDuration();
        this.rootResourceGroupInfoSupplier = expirationDuration.getValue() > 0 ?
                memoizeWithExpiration(() -> resourceGroupManager.getRootResourceGroups(), expirationDuration.toMillis(), MILLISECONDS) :
                () -> resourceGroupManager.getRootResourceGroups();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Encoded
    @Path("{resourceGroupId: .*}")
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
        try {
            if (isNullOrEmpty(resourceGroupIdString)) {
                // return root groups if no group id is specified
                asyncResponse.resume(Response.ok().entity(rootResourceGroupInfoSupplier.get()).build());
            }
            else {
                ResourceGroupId resourceGroupId = getResourceGroupId(resourceGroupIdString);

                ResourceGroupStateInfoKey resourceGroupStateInfoKey = new ResourceGroupStateInfoKey(resourceGroupId, includeQueryInfo, summarizeSubgroups, includeStaticSubgroupsOnly);

                Supplier<ResourceGroupInfo> resourceGroupInfoSupplier = resourceGroupStateInfoKeySupplierMap.getOrDefault(resourceGroupStateInfoKey, expirationDuration.getValue() > 0 ?
                        Suppliers.memoizeWithExpiration(() -> getResourceGroupInfo(resourceGroupId, includeQueryInfo, summarizeSubgroups, includeStaticSubgroupsOnly), expirationDuration.toMillis(), MILLISECONDS) :
                        () -> getResourceGroupInfo(resourceGroupId, includeQueryInfo, summarizeSubgroups, includeStaticSubgroupsOnly));

                resourceGroupStateInfoKeySupplierMap.putIfAbsent(resourceGroupStateInfoKey, resourceGroupInfoSupplier);

                asyncResponse.resume(Response.ok().entity(resourceGroupInfoSupplier.get()).build());
            }
        }
        catch (NoSuchElementException | IllegalArgumentException e) {
            asyncResponse.resume(Response.status(NOT_FOUND).build());
        }
    }

    private ResourceGroupInfo getResourceGroupInfo(ResourceGroupId resourceGroupId, boolean includeQueryInfo, boolean summarizeSubgroups, boolean includeStaticSubgroupsOnly)
    {
        return resourceGroupManager.getResourceGroupInfo(
                resourceGroupId,
                includeQueryInfo,
                summarizeSubgroups,
                includeStaticSubgroupsOnly);
    }

    private ResourceGroupId getResourceGroupId(String resourceGroupIdString)
    {
        return new ResourceGroupId(
                Arrays.stream(resourceGroupIdString.split("/"))
                        .map(ResourceGroupStateInfoResource::urlDecode)
                        .collect(toImmutableList()));
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
