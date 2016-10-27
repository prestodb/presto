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

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;

import static com.facebook.presto.server.ResourceUtil.assertRequest;
import static com.facebook.presto.server.ResourceUtil.createSessionForRequest;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
public class QueryResource
{
    private final QueryManager queryManager;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final QueryIdGenerator queryIdGenerator;

    @Inject
    public QueryResource(
            QueryManager queryManager,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            QueryIdGenerator queryIdGenerator)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
    }

    @GET
    public List<BasicQueryInfo> getAllQueryInfo()
    {
        return extractBasicQueryInfo(queryManager.getAllQueryInfo());
    }

    private static List<BasicQueryInfo> extractBasicQueryInfo(List<QueryInfo> allQueryInfo)
    {
        ImmutableList.Builder<BasicQueryInfo> basicQueryInfo = ImmutableList.builder();
        for (QueryInfo queryInfo : allQueryInfo) {
            basicQueryInfo.add(new BasicQueryInfo(queryInfo));
        }
        return basicQueryInfo.build();
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

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createQuery(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo)
    {
        assertRequest(!isNullOrEmpty(statement), "SQL statement is empty");

        Session session = createSessionForRequest(servletRequest, transactionManager, accessControl, sessionPropertyManager, queryIdGenerator.createNextQueryId());

        QueryInfo queryInfo = queryManager.createQuery(session, statement);
        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(queryInfo.getQueryId().toString()).build();
        return Response.created(pagesUri).entity(queryInfo).build();
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        queryManager.cancelQuery(queryId);
    }

    @DELETE
    @Path("stage/{stageId}")
    public void cancelStage(@PathParam("stageId") StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");
        queryManager.cancelStage(stageId);
    }
}
