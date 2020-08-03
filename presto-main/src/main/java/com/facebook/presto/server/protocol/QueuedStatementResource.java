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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.dispatcher.DispatchExecutor;
import com.facebook.presto.dispatcher.DispatchInfo;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.server.HttpRequestSessionContext;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/")
@RolesAllowed(USER)
public class QueuedStatementResource
{
    private static final Logger log = Logger.get(QueuedStatementResource.class);
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final DataSize TARGET_RESULT_SIZE = new DataSize(1, MEGABYTE);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final Duration NO_DURATION = new Duration(0, MILLISECONDS);

    private final DispatchManager dispatchManager;
    private final LocalQueryProvider queryResultsProvider;

    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("dispatch-query-purger"));

    @Inject
    public QueuedStatementResource(
            DispatchManager dispatchManager,
            DispatchExecutor executor,
            LocalQueryProvider queryResultsProvider)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryResultsProvider = queryResultsProvider;

        requireNonNull(dispatchManager, "dispatchManager is null");
        this.responseExecutor = requireNonNull(executor, "responseExecutor is null").getExecutor();
        this.timeoutExecutor = requireNonNull(executor, "timeoutExecutor is null").getScheduledExecutor();

        queryPurger.scheduleWithFixedDelay(
                () -> {
                    try {
                        // snapshot the queries before checking states to avoid registration race
                        for (Entry<QueryId, Query> entry : ImmutableSet.copyOf(queries.entrySet())) {
                            if (!entry.getValue().isSubmissionFinished()) {
                                continue;
                            }

                            // forget about this query if the query manager is no longer tracking it
                            if (!dispatchManager.isQueryPresent(entry.getKey())) {
                                queries.remove(entry.getKey());
                            }
                        }
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error removing old queries");
                    }
                },
                200,
                200,
                MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    @POST
    @Path("/v1/statement")
    @Produces(APPLICATION_JSON)
    public Response postStatement(
            String statement,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }

        SessionContext sessionContext = new HttpRequestSessionContext(servletRequest);
        Query query = new Query(statement, sessionContext, dispatchManager, queryResultsProvider, timeoutExecutor);
        queries.put(query.getQueryId(), query);

        return Response.ok(query.getInitialQueryResults(uriInfo, xForwardedProto)).build();
    }

    @GET
    @Path("/v1/statement/queued/{queryId}/{token}")
    @Produces(APPLICATION_JSON)
    public void getStatus(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("slug") String slug,
            @QueryParam("maxWait") Duration maxWait,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = getQuery(queryId, slug);

        // wait for query to be dispatched, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                query.waitForDispatched(),
                () -> null,
                WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait),
                timeoutExecutor);

        // when state changes, fetch the next result
        ListenableFuture<Response> queryResultsFuture = transformAsync(
                futureStateChange,
                ignored -> query.toResponse(token, uriInfo, xForwardedProto, WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait)),
                responseExecutor);
        bindAsyncResponse(asyncResponse, queryResultsFuture, responseExecutor);
    }

    @DELETE
    @Path("/v1/statement/queued/{queryId}/{token}")
    @Produces(APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("slug") String slug)
    {
        getQuery(queryId, slug).cancel();
        return Response.noContent().build();
    }

    private Query getQuery(QueryId queryId, String slug)
    {
        Query query = queries.get(queryId);
        if (query == null || !query.getSlug().equals(slug)) {
            throw badRequest(NOT_FOUND, "Query not found");
        }
        return query;
    }

    private static URI getQueryHtmlUri(QueryId queryId, UriInfo uriInfo, String xForwardedProto)
    {
        return uriInfo.getRequestUriBuilder()
                .scheme(getScheme(xForwardedProto, uriInfo))
                .replacePath("ui/query.html")
                .replaceQuery(queryId.toString())
                .build();
    }

    private static URI getQueuedUri(QueryId queryId, String slug, long token, UriInfo uriInfo, String xForwardedProto)
    {
        return uriInfo.getBaseUriBuilder()
                .scheme(getScheme(xForwardedProto, uriInfo))
                .replacePath("/v1/statement/queued/")
                .path(queryId.toString())
                .path(String.valueOf(token))
                .replaceQuery("")
                .queryParam("slug", slug)
                .build();
    }

    private static String getScheme(String xForwardedProto, @Context UriInfo uriInfo)
    {
        return isNullOrEmpty(xForwardedProto) ? uriInfo.getRequestUri().getScheme() : xForwardedProto;
    }

    private static QueryResults createQueryResults(
            QueryId queryId,
            URI nextUri,
            Optional<QueryError> queryError,
            UriInfo uriInfo,
            String xForwardedProto,
            Duration elapsedTime,
            Duration queuedTime)
    {
        QueryState state = queryError.map(error -> FAILED).orElse(QUEUED);
        return new QueryResults(
                queryId.toString(),
                getQueryHtmlUri(queryId, uriInfo, xForwardedProto),
                null,
                nextUri,
                null,
                null,
                StatementStats.builder()
                        .setState(state.toString())
                        .setQueued(state == QUEUED)
                        .setElapsedTimeMillis(elapsedTime.toMillis())
                        .setQueuedTimeMillis(queuedTime.toMillis())
                        .build(),
                queryError.orElse(null),
                ImmutableList.of(),
                null,
                null);
    }

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private static final class Query
    {
        private final String query;
        private final SessionContext sessionContext;
        private final DispatchManager dispatchManager;
        private final LocalQueryProvider queryProvider;
        private final ScheduledExecutorService timeoutExecutor;
        private final QueryId queryId;
        private final String slug = "x" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        private final AtomicLong lastToken = new AtomicLong();

        @GuardedBy("this")
        private ListenableFuture<?> querySubmissionFuture;

        public Query(String query, SessionContext sessionContext, DispatchManager dispatchManager, LocalQueryProvider queryResultsProvider, ScheduledExecutorService timeoutExecutor)
        {
            this.query = requireNonNull(query, "query is null");
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
            this.queryProvider = requireNonNull(queryResultsProvider, "queryExecutor is null");
            this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
            this.queryId = dispatchManager.createQueryId();
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public String getSlug()
        {
            return slug;
        }

        public long getLastToken()
        {
            return lastToken.get();
        }

        public synchronized boolean isSubmissionFinished()
        {
            return querySubmissionFuture != null && querySubmissionFuture.isDone();
        }

        private ListenableFuture<?> waitForDispatched()
        {
            // if query query submission has not finished, wait for it to finish
            synchronized (this) {
                if (querySubmissionFuture == null) {
                    querySubmissionFuture = dispatchManager.createQuery(queryId, slug, sessionContext, query);
                }
                if (!querySubmissionFuture.isDone()) {
                    return querySubmissionFuture;
                }
            }

            // otherwise, wait for the query to finish
            return dispatchManager.waitForDispatched(queryId);
        }

        public synchronized QueryResults getInitialQueryResults(UriInfo uriInfo, String xForwardedProto)
        {
            verify(lastToken.get() == 0);
            verify(querySubmissionFuture == null);
            return createQueryResults(
                    1,
                    uriInfo,
                    xForwardedProto,
                    DispatchInfo.queued(NO_DURATION, NO_DURATION));
        }

        public ListenableFuture<Response> toResponse(long token, UriInfo uriInfo, String xForwardedProto, Duration maxWait)
        {
            long lastToken = this.lastToken.get();
            // token should be the last token or the next token
            if (token != lastToken && token != lastToken + 1) {
                throw new WebApplicationException(Response.Status.GONE);
            }
            // advance (or stay at) the token
            this.lastToken.compareAndSet(lastToken, token);

            synchronized (this) {
                // if query submission has not finished, return simple empty result
                if (querySubmissionFuture == null || !querySubmissionFuture.isDone()) {
                    QueryResults queryResults = createQueryResults(
                            token + 1,
                            uriInfo,
                            xForwardedProto,
                            DispatchInfo.queued(NO_DURATION, NO_DURATION));
                    return immediateFuture(Response.ok(queryResults).build());
                }
            }

            Optional<DispatchInfo> dispatchInfo = dispatchManager.getDispatchInfo(queryId);
            if (!dispatchInfo.isPresent()) {
                // query should always be found, but it may have just been determined to be abandoned
                return immediateFailedFuture(new WebApplicationException(Response
                        .status(NOT_FOUND)
                        .build()));
            }

            if (!waitForDispatched().isDone()) {
                return immediateFuture(Response.ok(createQueryResults(token + 1, uriInfo, xForwardedProto, dispatchInfo.get())).build());
            }

            com.facebook.presto.server.protocol.Query query;
            try {
                query = queryProvider.getQuery(queryId, slug);
            }
            catch (WebApplicationException e) {
                return immediateFuture(Response.ok(createQueryResults(token + 1, uriInfo, xForwardedProto, dispatchInfo.get())).build());
            }
            // If this future completes successfully, the next URI will redirect to the executing statement endpoint.
            // Hence it is safe to hardcode the token to be 0.
            return transform(
                    query.waitForResults(0, uriInfo, getScheme(xForwardedProto, uriInfo), maxWait, TARGET_RESULT_SIZE),
                    results -> QueryResourceUtil.toResponse(query, results),
                    directExecutor());
        }

        public synchronized void cancel()
        {
            querySubmissionFuture.addListener(() -> dispatchManager.cancelQuery(queryId), directExecutor());
        }

        private QueryResults createQueryResults(long token, UriInfo uriInfo, String xForwardedProto, DispatchInfo dispatchInfo)
        {
            URI nextUri = getNextUri(token, uriInfo, xForwardedProto, dispatchInfo);

            Optional<QueryError> queryError = dispatchInfo.getFailureInfo()
                    .map(this::toQueryError);

            return QueuedStatementResource.createQueryResults(
                    queryId,
                    nextUri,
                    queryError,
                    uriInfo,
                    xForwardedProto,
                    dispatchInfo.getElapsedTime(),
                    dispatchInfo.getQueuedTime());
        }

        private URI getNextUri(long token, UriInfo uriInfo, String xForwardedProto, DispatchInfo dispatchInfo)
        {
            // if failed, query is complete
            if (dispatchInfo.getFailureInfo().isPresent()) {
                return null;
            }
            return getQueuedUri(queryId, slug, token, uriInfo, xForwardedProto);
        }

        private QueryError toQueryError(ExecutionFailureInfo executionFailureInfo)
        {
            ErrorCode errorCode;
            if (executionFailureInfo.getErrorCode() != null) {
                errorCode = executionFailureInfo.getErrorCode();
            }
            else {
                errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
                log.warn("Failed query %s has no error code", queryId);
            }

            return new QueryError(
                    firstNonNull(executionFailureInfo.getMessage(), "Internal error"),
                    null,
                    errorCode.getCode(),
                    errorCode.getName(),
                    errorCode.getType().toString(),
                    executionFailureInfo.getErrorLocation(),
                    executionFailureInfo.toFailureInfo());
        }
    }
}
