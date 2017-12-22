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

import com.facebook.presto.client.QueryActions;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.server.HttpRequestSessionContext;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/statement")
public class StatementResource
{
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();

    private final QueryManager queryManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;
    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("query-purger"));

    @Inject
    public StatementResource(
            QueryManager queryManager,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClientSupplier exchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            @ForStatementResource BoundedExecutor responseExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
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
    @Produces(MediaType.APPLICATION_JSON)
    public void createQuery(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        if (isNullOrEmpty(statement)) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .type(MediaType.TEXT_PLAIN)
                    .entity("SQL statement is empty")
                    .build());
        }
        SessionContext sessionContext = new HttpRequestSessionContext(servletRequest);
        Query query = Query.createV1(
                sessionContext,
                statement,
                queryManager,
                sessionPropertyManager,
                exchangeClientSupplier,
                responseExecutor,
                timeoutExecutor,
                blockEncodingSerde);
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

    private void asyncQueryResults(Query query, OptionalLong token, Duration maxWait, UriInfo uriInfo, AsyncResponse asyncResponse)
    {
        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        ListenableFuture<QueryResults> queryResultsFuture = query.waitForResults(token, uriInfo, wait);

        ListenableFuture<Response> response = Futures.transform(queryResultsFuture, StatementResource::toResponse);

        bindAsyncResponse(asyncResponse, response, responseExecutor);
    }

    private static Response toResponse(QueryResults queryResults)
    {
        // TODO: think if it's fine to keep this section for V1 protocol
        QueryResults withoutActions = queryResults.clearActions();
        Response.ResponseBuilder response = Response.ok(withoutActions);

        QueryActions actions = queryResults.getActions();
        if (actions == null) {
            return response.build();
        }

        // add set catalog and schema
        if (actions.getSetCatalog() != null) {
            response.header(PRESTO_SET_CATALOG, actions.getSetCatalog());
        }
        if (actions.getSetSchema() != null) {
            response.header(PRESTO_SET_SCHEMA, actions.getSetSchema());
        }

        // add set session properties
        if (actions.getSetSessionProperties() != null) {
            actions.getSetSessionProperties().entrySet()
                    .forEach(entry -> response.header(PRESTO_SET_SESSION, entry.getKey() + '=' + entry.getValue()));
        }

        // add clear session properties
        if (actions.getClearSessionProperties() != null) {
            actions.getClearSessionProperties()
                    .forEach(name -> response.header(PRESTO_CLEAR_SESSION, name));
        }

        // add added prepare statements
        if (actions.getAddedPreparedStatements() != null) {
            for (Map.Entry<String, String> entry : actions.getAddedPreparedStatements().entrySet()) {
                String encodedKey = urlEncode(entry.getKey());
                String encodedValue = urlEncode(entry.getValue());
                response.header(PRESTO_ADDED_PREPARE, encodedKey + '=' + encodedValue);
            }
        }

        // add deallocated prepare statements
        if (actions.getDeallocatedPreparedStatements() != null) {
            for (String name : actions.getDeallocatedPreparedStatements()) {
                response.header(PRESTO_DEALLOCATED_PREPARE, urlEncode(name));
            }
        }

        // add new transaction ID
        if (actions.getStartedTransactionId() != null) {
            response.header(PRESTO_STARTED_TRANSACTION_ID, actions.getStartedTransactionId());
        }

        // add clear transaction ID directive
        if (actions.isClearTransactionId() != null && actions.isClearTransactionId()) {
            response.header(PRESTO_CLEAR_TRANSACTION_ID, true);
        }

        return response.build();
    }

    @DELETE
    @Path("{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelQuery(@PathParam("queryId") QueryId queryId,
            @PathParam("token") long token)
    {
        Query query = queries.get(queryId);
        if (query == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        query.cancel();
        return Response.noContent().build();
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
