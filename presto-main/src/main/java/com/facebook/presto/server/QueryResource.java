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
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourcemanager.ResourceManagerProxy;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.connector.system.KillQueryProcedure.createKillQueryException;
import static com.facebook.presto.connector.system.KillQueryProcedure.createPreemptQueryException;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
@RolesAllowed({USER, ADMIN})
public class QueryResource
{
    // Sort returned queries: RUNNING - first, then QUEUED, then other non-completed, then FAILED and in each group we sort by create time.
    private static final Comparator<BasicQueryInfo> QUERIES_ORDERING = Ordering.<BasicQueryInfo>from((o1, o2) -> Boolean.compare(o1.getState() == QueryState.RUNNING, o2.getState() == QueryState.RUNNING))
            .compound((o1, o2) -> Boolean.compare(o1.getState() == QueryState.QUEUED, o2.getState() == QueryState.QUEUED))
            .compound((o1, o2) -> Boolean.compare(!o1.getState().isDone(), !o2.getState().isDone()))
            .compound((o1, o2) -> Boolean.compare(o1.getState() == QueryState.FAILED, o2.getState() == QueryState.FAILED))
            .compound(Comparator.comparing(item -> item.getQueryStats().getCreateTime()));

    // TODO There should be a combined interface for this
    private final boolean resourceManagerEnabled;
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;
    private final InternalNodeManager internalNodeManager;
    private final Optional<ResourceManagerProxy> proxyHelper;

    @Inject
    public QueryResource(
            ServerConfig serverConfig,
            DispatchManager dispatchManager,
            QueryManager queryManager,
            InternalNodeManager internalNodeManager,
            Optional<ResourceManagerProxy> proxyHelper)
    {
        this.resourceManagerEnabled = requireNonNull(serverConfig, "serverConfig is null").isResourceManagerEnabled();
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.proxyHelper = requireNonNull(proxyHelper, "proxyHelper is null");
    }

    @GET
    public void getAllQueryInfo(
            @QueryParam("state") String stateFilter,
            @QueryParam("limit") Integer limitFilter,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (resourceManagerEnabled) {
            proxyResponse(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            return;
        }

        int limit = firstNonNull(limitFilter, Integer.MAX_VALUE);
        if (limit <= 0) {
            throw new WebApplicationException(Response
                    .status(BAD_REQUEST)
                    .type(MediaType.TEXT_PLAIN)
                    .entity(format("Parameter 'limit' for getAllQueryInfo must be positive. Got %d.", limit))
                    .build());
        }

        List<BasicQueryInfo> queries = new ArrayList<>(dispatchManager.getQueries());

        // Filter list by the query state (if specified).
        if (stateFilter != null) {
            QueryState expectedState = QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));
            queries.removeIf(item -> item.getState() == expectedState);
        }

        // If limit is smaller than number of queries, then ensure that the more recent items are at the front.
        if (limit < queries.size()) {
            queries.sort(QUERIES_ORDERING);
        }
        else {
            limit = queries.size();
        }

        ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
        builder.addAll(queries.subList(0, limit));
        asyncResponse.resume(Response.ok(builder.build()).build());
    }

    @GET
    @Path("{queryId}")
    public void getQueryInfo(
            @PathParam("queryId") QueryId queryId,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(queryId, "queryId is null");

        if (resourceManagerEnabled && !dispatchManager.isQueryPresent(queryId)) {
            proxyResponse(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            return;
        }

        try {
            QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
            asyncResponse.resume(Response.ok(queryInfo).build());
        }
        catch (NoSuchElementException e) {
            try {
                BasicQueryInfo basicQueryInfo = dispatchManager.getQueryInfo(queryId);
                asyncResponse.resume(Response.ok(basicQueryInfo).build());
            }
            catch (NoSuchElementException ex) {
                asyncResponse.resume(Response.status(Status.GONE).build());
            }
        }
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(queryId, "queryId is null");
        if (resourceManagerEnabled && !dispatchManager.isQueryPresent(queryId)) {
            proxyResponse(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            return;
        }
        dispatchManager.cancelQuery(queryId);
        asyncResponse.resume(Response.status(NO_CONTENT).build());
    }

    @PUT
    @Path("{queryId}/killed")
    public void killQuery(
            @PathParam("queryId") QueryId queryId,
            String message,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (resourceManagerEnabled && !dispatchManager.isQueryPresent(queryId)) {
            proxyResponse(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            return;
        }
        asyncResponse.resume(failQuery(queryId, createKillQueryException(message)));
    }

    @PUT
    @Path("{queryId}/preempted")
    public void preemptQuery(
            @PathParam("queryId") QueryId queryId,
            String message,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (!dispatchManager.isQueryPresent(queryId)) {
            proxyResponse(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            return;
        }
        asyncResponse.resume(failQuery(queryId, createPreemptQueryException(message)));
    }

    private Response failQuery(QueryId queryId, PrestoException queryException)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            BasicQueryInfo state = dispatchManager.getQueryInfo(queryId);

            // check before killing to provide the proper error code (this is racy)
            if (state.getState().isDone()) {
                return Response.status(Status.CONFLICT).build();
            }

            dispatchManager.failQuery(queryId, queryException);

            // verify if the query was failed (if not, we lost the race)
            if (!queryException.getErrorCode().equals(dispatchManager.getQueryInfo(queryId).getErrorCode())) {
                return Response.status(Status.CONFLICT).build();
            }

            return Response.status(Status.OK).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("stage/{stageId}")
    public void cancelStage(
            @PathParam("stageId") StageId stageId,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(stageId, "stageId is null");
        if (!dispatchManager.isQueryPresent(stageId.getQueryId())) {
            proxyResponse(servletRequest, asyncResponse, xForwardedProto, uriInfo);
            return;
        }
        queryManager.cancelStage(stageId);
        asyncResponse.resume(Response.ok().build());
    }

    private void proxyResponse(HttpServletRequest servletRequest, AsyncResponse asyncResponse, String xForwardedProto, UriInfo uriInfo)
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
