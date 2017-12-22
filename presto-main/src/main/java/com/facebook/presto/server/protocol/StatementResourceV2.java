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
package com.facebook.presto.server.protocol;

import com.facebook.presto.client.CreateQueryRequest;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v2/statement")
public class StatementResourceV2
{
    private static final Duration MAX_WAIT_TIME = new Duration(5, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();

    private final QueryManager queryManager;
    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("query-purger-v2"));

    @Inject
    public StatementResourceV2(
            QueryManager queryManager,
            @ForStatementResource BoundedExecutor responseExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");

        queryPurger.scheduleWithFixedDelay(new PurgeQueriesRunnable(queries, queryManager), 200, 200, MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void createQuery(
            CreateQueryRequest createQueryRequest,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        if (createQueryRequest == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity(ImmutableMap.of("error", "Cannot parse create query request"))
                    .build());
        }
        SessionContext sessionContext = new QueryRequestSessionContext(createQueryRequest.getSession(), servletRequest);
        Query query = Query.createV2(
                sessionContext,
                createQueryRequest.getQuery(),
                queryManager,
                responseExecutor,
                timeoutExecutor);
        queries.put(query.getQueryId(), query);

        asyncQueryResults(query, OptionalLong.empty(), new Duration(1, MILLISECONDS), uriInfo, asyncResponse);
    }

    @GET
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = queries.get(queryId);
        if (query == null) {
            asyncResponse.resume(Response.status(Status.NOT_FOUND).build());
            return;
        }
        asyncQueryResults(query, OptionalLong.of(token), maxWait, uriInfo, asyncResponse);
    }

    @DELETE
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token)
    {
        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        query.cancel();
        return Response.noContent().build();
    }

    private void asyncQueryResults(Query query, OptionalLong token, Duration maxWait, UriInfo uriInfo, AsyncResponse asyncResponse)
    {
        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        ListenableFuture<QueryResults> queryResultsFuture = query.waitForResults(token, uriInfo, wait);

        ListenableFuture<Response> response = Futures.transform(queryResultsFuture, StatementResourceV2::toResponse);

        bindAsyncResponse(asyncResponse, response, responseExecutor);
    }

    private static Response toResponse(QueryResults queryResults)
    {
        return Response.ok(queryResults).build();
    }
}
