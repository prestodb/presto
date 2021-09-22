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

import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourcemanager.ResourceManagerProxy;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
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

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.server.QueryStateInfo.createQueryStateInfo;
import static com.facebook.presto.server.QueryStateInfo.createQueuedQueryStateInfo;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

@Path("/v1/queryState")
@RolesAllowed({ADMIN, USER})
public class QueryStateInfoResource
{
    private final DispatchManager dispatchManager;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final boolean resourceManagerEnabled;
    private final InternalNodeManager internalNodeManager;
    private final Optional<ResourceManagerProxy> proxyHelper;

    @Inject
    public QueryStateInfoResource(
            DispatchManager dispatchManager,
            ResourceGroupManager<?> resourceGroupManager,
            InternalNodeManager internalNodeManager,
            ServerConfig serverConfig,
            Optional<ResourceManagerProxy> proxyHelper)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.resourceManagerEnabled = requireNonNull(serverConfig, "serverConfig is null").isResourceManagerEnabled();
        this.proxyHelper = requireNonNull(proxyHelper, "proxyHelper is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryStateInfos(@QueryParam("user") String user,
            @QueryParam("includeLocalQueryOnly") @DefaultValue("false") boolean isIncludeLocalQueryOnly,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (resourceManagerEnabled && !isIncludeLocalQueryOnly) {
            proxyQueryStateInfo(servletRequest, asyncResponse, xForwardedProto, uriInfo);
        }
        else {
            List<BasicQueryInfo> queryInfos = dispatchManager.getQueries();

            if (!isNullOrEmpty(user)) {
                queryInfos = queryInfos.stream()
                        .filter(queryInfo -> Pattern.matches(user, queryInfo.getSession().getUser()))
                        .collect(toImmutableList());
            }

            asyncResponse.resume(Response.ok(queryInfos.stream()
                    .filter(queryInfo -> !queryInfo.getState().isDone())
                    .map(this::getQueryStateInfo)
                    .collect(toImmutableList())).build());
        }
    }

    private QueryStateInfo getQueryStateInfo(BasicQueryInfo queryInfo)
    {
        Optional<ResourceGroupId> groupId = queryInfo.getResourceGroupId();
        if (queryInfo.getState() == QUEUED) {
            return createQueuedQueryStateInfo(
                    queryInfo,
                    groupId,
                    groupId.map(resourceGroupManager::getPathToRoot));
        }
        return createQueryStateInfo(queryInfo, groupId);
    }

    @GET
    @Path("{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryStateInfo(@PathParam("queryId") String queryId, @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
            throws WebApplicationException
    {
        try {
            QueryId queryID = new QueryId(queryId);
            if (resourceManagerEnabled && !dispatchManager.isQueryPresent(queryID)) {
                proxyQueryStateInfo(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            }
            else {
                BasicQueryInfo queryInfo = dispatchManager.getQueryInfo(queryID);
                asyncResponse.resume(Response.ok(getQueryStateInfo(queryInfo)).build());
            }
        }
        catch (NoSuchElementException e) {
            asyncResponse.resume(Response.status(NOT_FOUND).build());
        }
    }

    //TODO the pattern of this function is similar with ClusterStatsResource and QueryResource, we can move it to a common place and re-use.
    private void proxyQueryStateInfo(HttpServletRequest servletRequest, AsyncResponse asyncResponse, String xForwardedProto, UriInfo uriInfo)
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
