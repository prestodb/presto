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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.ImmutableList;

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

import static com.facebook.presto.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static java.util.Objects.requireNonNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
public class QueryResource
{
    private final QueryManager queryManager;

    @Inject
    public QueryResource(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @GET
    public List<BasicQueryInfo> getAllQueryInfo(@QueryParam("state") String queryState)
    {
        ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
        for (QueryInfo queryInfo : queryManager.getAllQueryInfo()) {
            if (queryState == null || queryInfo.getState().equals(QueryState.valueOf(queryState.toUpperCase(Locale.ENGLISH)))) {
                builder.add(new BasicQueryInfo(queryInfo));
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
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            return Response.ok(queryInfo).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        queryManager.cancelQuery(queryId);
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String reason)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);

            // If the query was already done, we don't need to do kill it.
            if (queryInfo.getState().isDone()) {
                return Response.status(Status.CONFLICT).build();
            }

            // Kill the query
            queryManager.failQuery(queryId, new PrestoException(ADMINISTRATIVELY_KILLED, reason));

            // Verify if the query was killed successfully.
            if (queryInfo.getState() == QueryState.FAILED && queryInfo.getErrorCode() == StandardErrorCode.ADMINISTRATIVELY_KILLED.toErrorCode()) {
                return Response.status(Status.OK).build();
            }
            else {
                return Response.status(Status.CONFLICT).build();
            }
        }
        catch (NoSuchElementException e) {
            // The query was not killed because it does not exist.
            return Response.status(Status.CONFLICT).build();
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
