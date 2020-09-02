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
import com.facebook.presto.spi.PrestoException;
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
import javax.ws.rs.core.Response.Status;

import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

import static com.facebook.presto.connector.system.KillQueryProcedure.createKillQueryException;
import static com.facebook.presto.connector.system.KillQueryProcedure.createPreemptQueryException;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static java.util.Objects.requireNonNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
@RolesAllowed({USER, ADMIN})
public class QueryResource
{
    // TODO There should be a combined interface for this
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;

    @Inject
    public QueryResource(DispatchManager dispatchManager, QueryManager queryManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @GET
    public List<BasicQueryInfo> getAllQueryInfo(@QueryParam("state") String stateFilter)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));
        ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
        for (BasicQueryInfo queryInfo : dispatchManager.getQueries()) {
            if (stateFilter == null || queryInfo.getState() == expectedState) {
                builder.add(queryInfo);
            }
        }
        return builder.build();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
            return Response.ok(queryInfo).build();
        }
        catch (NoSuchElementException e) {
            try {
                BasicQueryInfo basicQueryInfo = dispatchManager.getQueryInfo(queryId);
                return Response.ok(basicQueryInfo).build();
            }
            catch (NoSuchElementException ex) {
                return Response.status(Status.GONE).build();
            }
        }
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        dispatchManager.cancelQuery(queryId);
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        return failQuery(queryId, createKillQueryException(message));
    }

    @PUT
    @Path("{queryId}/preempted")
    public Response preemptQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        return failQuery(queryId, createPreemptQueryException(message));
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
    public void cancelStage(@PathParam("stageId") StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");
        queryManager.cancelStage(stageId);
    }
}
