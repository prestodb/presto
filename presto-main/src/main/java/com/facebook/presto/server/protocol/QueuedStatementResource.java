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
import com.facebook.airlift.stats.TimeStat;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.dispatcher.DispatchExecutor;
import com.facebook.presto.dispatcher.DispatchInfo;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.HttpRequestSessionContext;
import com.facebook.presto.server.RetryUrlValidator;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tracing.TracerProviderManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.CacheControl;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.UriInfo;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREFIX_URL;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_RETRY_QUERY;
import static com.facebook.presto.server.protocol.QueryResourceUtil.NO_DURATION;
import static com.facebook.presto.server.protocol.QueryResourceUtil.abortIfPrefixUrlInvalid;
import static com.facebook.presto.server.protocol.QueryResourceUtil.createQueuedQueryResults;
import static com.facebook.presto.server.protocol.QueryResourceUtil.getCacheControlMaxAge;
import static com.facebook.presto.server.protocol.QueryResourceUtil.getQueuedUri;
import static com.facebook.presto.server.protocol.QueryResourceUtil.getScheme;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.RETRY_QUERY_NOT_FOUND;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.CONFLICT;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/")
@RolesAllowed(USER)
public class QueuedStatementResource
{
    private static final Logger log = Logger.get(QueuedStatementResource.class);
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final DataSize TARGET_RESULT_SIZE = new DataSize(1, MEGABYTE);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();

    private final DispatchManager dispatchManager;
    private final ExecutingQueryResponseProvider executingQueryResponseProvider;

    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final Map<QueryId, Query> queries = new ConcurrentHashMap<>();          // a mapping from current query id to current query
    private final Map<QueryId, Query> retriedQueries = new ConcurrentHashMap<>();   // a mapping from old to-be-retried query id to the current retry query
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("dispatch-query-purger"));
    private final boolean compressionEnabled;
    private final boolean nestedDataSerializationEnabled;

    private final SqlParserOptions sqlParserOptions;
    private final TracerProviderManager tracerProviderManager;
    private final SessionPropertyManager sessionPropertyManager;     // We may need some system default session property values at early query stage even before session is created.

    private final QueryBlockingRateLimiter queryRateLimiter;
    private final RetryUrlValidator retryUrlValidator;

    @Inject
    public QueuedStatementResource(
            DispatchManager dispatchManager,
            DispatchExecutor executor,
            ExecutingQueryResponseProvider executingQueryResponseProvider,
            SqlParserOptions sqlParserOptions,
            ServerConfig serverConfig,
            TracerProviderManager tracerProviderManager,
            SessionPropertyManager sessionPropertyManager,
            QueryBlockingRateLimiter queryRateLimiter,
            RetryUrlValidator retryUrlValidator)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.executingQueryResponseProvider = requireNonNull(executingQueryResponseProvider, "executingQueryResponseProvider is null");
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
        this.compressionEnabled = requireNonNull(serverConfig, "serverConfig is null").isQueryResultsCompressionEnabled();
        this.nestedDataSerializationEnabled = requireNonNull(serverConfig, "serverConfig is null").isNestedDataSerializationEnabled();

        this.responseExecutor = requireNonNull(executor, "responseExecutor is null").getExecutor();
        this.timeoutExecutor = requireNonNull(executor, "timeoutExecutor is null").getScheduledExecutor();
        this.tracerProviderManager = requireNonNull(tracerProviderManager, "tracerProviderManager is null");
        this.sessionPropertyManager = sessionPropertyManager;

        this.queryRateLimiter = requireNonNull(queryRateLimiter, "queryRateLimiter is null");
        this.retryUrlValidator = requireNonNull(retryUrlValidator, "retryUrlValidator is null");

        queryPurger.scheduleWithFixedDelay(
                () -> {
                    try {
                        // snapshot the queries before checking states to avoid registration race
                        purgeQueries(queries);
                        purgeQueries(retriedQueries);
                    }
                    catch (Throwable e) {
                        log.error(e, "Error removing old queries");
                    }
                },
                200,
                200,
                MILLISECONDS);
    }

    @Managed
    @Nested
    public TimeStat getRateLimiterBlockTime()
    {
        return queryRateLimiter.getRateLimiterBlockTime();
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    /**
     * HTTP endpoint for submitting queries to the Presto Coordinator
     * Presto performs lazy execution. The submission of a query returns
     * a placeholder for the result set, but the query gets
     * scheduled/dispatched only when the client polls for results
     * @param statement The statement or sql query string submitted
     * @param xForwardedProto Forwarded protocol (http or https)
     * @param servletRequest The http request
     * @param uriInfo {@link jakarta.ws.rs.core.UriInfo}
     * @return {@link jakarta.ws.rs.core.Response} HTTP response code
     */
    @POST
    @Path("/v1/statement")
    @Produces(APPLICATION_JSON)
    public Response postStatement(
            String statement,
            @DefaultValue("false") @QueryParam("binaryResults") boolean binaryResults,
            @QueryParam("retryUrl") String retryUrlString,
            @QueryParam("retryExpirationInSeconds") Long retryExpirationInSeconds,
            @HeaderParam(PRESTO_RETRY_QUERY) String isRetryQueryHeader,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @HeaderParam(PRESTO_PREFIX_URL) String xPrestoPrefixUrl,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }

        abortIfPrefixUrlInvalid(xPrestoPrefixUrl);

        // Parse retry query header
        boolean isRetryQuery = parseBoolean(isRetryQueryHeader);

        // Validate retry URL if provided
        Optional<URI> retryUrl = Optional.empty();
        OptionalLong retryExpirationEpochTime = OptionalLong.empty();
        if ((retryUrlString != null && !retryUrlString.isEmpty()) || retryExpirationInSeconds != null) {
            if (retryUrlString == null || retryUrlString.isEmpty() || retryExpirationInSeconds == null || retryExpirationInSeconds < 1) {
                throw badRequest(BAD_REQUEST, format("Invalid retry parameters: retryUrl=%s, retryExpiration=%s", retryUrlString, retryExpirationInSeconds));
            }
            retryUrl = Optional.of(getRetryUrl(retryUrlString));
            retryExpirationEpochTime = OptionalLong.of(currentTimeMillis() + SECONDS.toMillis(retryExpirationInSeconds));
            String currentHost = uriInfo.getBaseUri().getHost();
            if (!retryUrlValidator.isValidRetryUrl(retryUrl.get(), currentHost)) {
                throw badRequest(BAD_REQUEST, "Invalid retry URL");
            }
        }

        // TODO: For future cases we may want to start tracing from client. Then continuation of tracing
        //       will be needed instead of creating a new trace here.
        SessionContext sessionContext = new HttpRequestSessionContext(
                servletRequest,
                sqlParserOptions,
                tracerProviderManager.getTracerProvider(),
                Optional.of(sessionPropertyManager));
        QueryId newQueryId = dispatchManager.createQueryId();
        Query query = new Query(
                statement,
                sessionContext,
                dispatchManager,
                executingQueryResponseProvider,
                0,
                newQueryId,
                createSlug(),
                isRetryQuery,
                retryUrl,
                retryExpirationEpochTime);

        queries.put(query.getQueryId(), query);

        return withCompressionConfiguration(Response.ok(query.getInitialQueryResults(uriInfo, xForwardedProto, xPrestoPrefixUrl, binaryResults)), compressionEnabled)
                .cacheControl(query.getDefaultCacheControl())
                .build();
    }

    private static URI getRetryUrl(String urlEncodedUrl)
    {
        try {
            String decodedUrl = URLDecoder.decode(urlEncodedUrl, UTF_8.toString());
            return URI.create(decodedUrl);
        }
        catch (UnsupportedEncodingException | IllegalArgumentException e) {
            throw badRequest(BAD_REQUEST, "Retry URL invalid");
        }
    }

    /**
     * HTTP endpoint for submitting queries to the Presto Coordinator.
     * Presto performs lazy execution. The submission of a query returns
     * a placeholder for the result set, but the query gets
     * scheduled/dispatched only when the client polls for results.
     * This endpoint accepts a pre-minted queryId and slug, instead of
     * generating it.
     *
     * @param statement The statement or sql query string submitted
     * @param queryId Pre-minted query ID to associate with this query
     * @param slug Pre-minted slug to protect this query
     * @param xForwardedProto Forwarded protocol (http or https)
     * @param servletRequest The http request
     * @param uriInfo {@link jakarta.ws.rs.core.UriInfo}
     * @return {@link jakarta.ws.rs.core.Response} HTTP response code
     */
    @PUT
    @Path("/v1/statement/{queryId}")
    @Produces(APPLICATION_JSON)
    public Response putStatement(
            String statement,
            @PathParam("queryId") QueryId queryId,
            @QueryParam("slug") String slug,
            @DefaultValue("false") @QueryParam("binaryResults") boolean binaryResults,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @HeaderParam(PRESTO_PREFIX_URL) String xPrestoPrefixUrl,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }

        abortIfPrefixUrlInvalid(xPrestoPrefixUrl);

        // TODO: For future cases we may want to start tracing from client. Then continuation of tracing
        //       will be needed instead of creating a new trace here.
        SessionContext sessionContext = new HttpRequestSessionContext(
                servletRequest,
                sqlParserOptions,
                tracerProviderManager.getTracerProvider(),
                Optional.of(sessionPropertyManager));
        Query attemptedQuery = new Query(statement, sessionContext, dispatchManager, executingQueryResponseProvider, 0, queryId, slug);
        Query query = queries.computeIfAbsent(queryId, unused -> attemptedQuery);

        if (attemptedQuery != query && !attemptedQuery.getSlug().equals(query.getSlug()) || query.getLastToken() != 0) {
            throw badRequest(CONFLICT, "Query already exists");
        }

        return withCompressionConfiguration(Response.ok(query.getInitialQueryResults(uriInfo, xForwardedProto, xPrestoPrefixUrl, binaryResults)), compressionEnabled)
                .cacheControl(query.getDefaultCacheControl())
                .build();
    }

    /**
     * HTTP endpoint for re-processing a failed query
     * @param queryId Query Identifier of the query to be retried
     * @param xForwardedProto Forwarded protocol (http or https)
     * @param uriInfo {@link jakarta.ws.rs.core.UriInfo}
     * @return {@link jakarta.ws.rs.core.Response} HTTP response code
     */
    @GET
    @Path("/v1/statement/queued/retry/{queryId}")
    @Produces(APPLICATION_JSON)
    public Response retryFailedQuery(
            @PathParam("queryId") QueryId queryId,
            @DefaultValue("false") @QueryParam("binaryResults") boolean binaryResults,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @HeaderParam(PRESTO_PREFIX_URL) String xPrestoPrefixUrl,
            @Context UriInfo uriInfo)
    {
        abortIfPrefixUrlInvalid(xPrestoPrefixUrl);

        Query failedQuery = queries.get(queryId);

        if (failedQuery == null) {
            // TODO: purge retryable queries slower than normal ones
            throw new PrestoException(RETRY_QUERY_NOT_FOUND, "failed to find the query to retry with ID " + queryId);
        }

        if (dispatchManager.isQueryPresent(queryId) &&
                dispatchManager.getQueryInfo(queryId).getFailureInfo() == null &&
                !failedQuery.isRetryQuery()) {
            throw badRequest(CONFLICT, "Query with ID " + queryId + " has not failed and cannot be retried");
        }

        int retryCount = failedQuery.getRetryCount() + 1;
        Query query = new Query(
                "-- retry query " + queryId + "; attempt: " + retryCount + "\n" + failedQuery.getQuery(),
                failedQuery.getSessionContext(),
                dispatchManager,
                executingQueryResponseProvider,
                retryCount);

        retriedQueries.putIfAbsent(queryId, query);
        synchronized (retriedQueries.get(queryId)) {
            // Retry queries should never be processed except through this endpoint
            if (failedQuery.isRetryQuery() && failedQuery.getLastToken() != 0) {
                throw badRequest(CONFLICT, "Query with ID " + queryId + " has already been processed and cannot be retried");
            }

            if (retriedQueries.get(queryId).getQueryId().equals(query.getQueryId())) {
                queries.put(query.getQueryId(), query);
            }
            else {
                // other thread has already created the new retry query
                // use the existing one
                query = retriedQueries.get(queryId);
            }
        }

        return withCompressionConfiguration(Response.ok(query.getInitialQueryResults(uriInfo, xForwardedProto, xPrestoPrefixUrl, binaryResults)), compressionEnabled)
                .cacheControl(query.getDefaultCacheControl())
                .build();
    }

    /**
     * HTTP endpoint for retrieving the status of a submitted query
     * @param queryId Query Identifier of query whose status is polled
     * @param token Monotonically increasing token that identifies the next batch of query results
     * @param slug Unique security token generated for each query that controls access to that query's results
     * @param maxWait Time to wait for the query to be dispatched
     * @param xForwardedProto Forwarded protocol (http or https)
     * @param uriInfo {@link jakarta.ws.rs.core.UriInfo}
     * @param asyncResponse
     */
    @GET
    @Path("/v1/statement/queued/{queryId}/{token}")
    @Produces(APPLICATION_JSON)
    public void getStatus(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("slug") String slug,
            @QueryParam("maxWait") Duration maxWait,
            @DefaultValue("false") @QueryParam("binaryResults") boolean binaryResults,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @HeaderParam(PRESTO_PREFIX_URL) String xPrestoPrefixUrl,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        abortIfPrefixUrlInvalid(xPrestoPrefixUrl);

        Query query = getQuery(queryId, slug);

        if (query.isRetryQuery()) {
            throw badRequest(
                    CONFLICT,
                    format("Query with ID %s is a retry query and cannot be polled directly. Use /v1/statement/queued/retry/%s endpoint to retry it.",
                            queryId, queryId));
        }

        ListenableFuture<Double> acquirePermitAsync = queryRateLimiter.acquire(queryId);
        ListenableFuture<?> waitForDispatchedAsync = transformAsync(
                acquirePermitAsync,
                acquirePermitTimeSeconds -> {
                    queryRateLimiter.addRateLimiterBlockTime(new Duration(acquirePermitTimeSeconds, SECONDS));
                    return query.waitForDispatched();
                },
                responseExecutor);
        // wait for query to be dispatched, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                waitForDispatchedAsync,
                () -> null,
                WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait),
                timeoutExecutor);
        // when state changes, fetch the next result
        ListenableFuture<Response> queryResultsFuture = transformAsync(
                futureStateChange,
                ignored -> query.toResponse(token, uriInfo, xForwardedProto, xPrestoPrefixUrl, WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait), compressionEnabled, nestedDataSerializationEnabled, binaryResults),
                responseExecutor);
        bindAsyncResponse(asyncResponse, queryResultsFuture, responseExecutor);
    }

    /**
     * HTTP endpoint to cancel execution of a query in flight
     * @param queryId Query Identifier of query to be canceled
     * @param token Monotonically increasing token that identifies the next batch of query results
     * @param slug Unique security token generated for each query that controls access to that query's results
     * @return {@link jakarta.ws.rs.core.Response} HTTP response code
     */
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

    private void purgeQueries(Map<QueryId, Query> queries)
    {
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

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private static Response.ResponseBuilder withCompressionConfiguration(Response.ResponseBuilder builder, boolean compressionEnabled)
    {
        if (!compressionEnabled) {
            builder.encoding("identity");
        }
        return builder;
    }

    private static String createSlug()
    {
        return "x" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
    }

    private static final class Query
    {
        private static final int CACHE_CONTROL_MAX_AGE_SEC = 60;
        private final String query;
        private final SessionContext sessionContext;
        private final DispatchManager dispatchManager;
        private final ExecutingQueryResponseProvider executingQueryResponseProvider;
        private final QueryId queryId;
        private final String slug;
        private final AtomicLong lastToken = new AtomicLong();
        private final int retryCount;
        private final long expirationTime;
        private final boolean isRetryQuery;
        private final Optional<URI> retryUrl;
        private final OptionalLong retryExpirationEpochTime;

        @GuardedBy("this")
        private ListenableFuture<?> querySubmissionFuture;

        public Query(String query, SessionContext sessionContext, DispatchManager dispatchManager, ExecutingQueryResponseProvider executingQueryResponseProvider, int retryCount)
        {
            this(query, sessionContext, dispatchManager, executingQueryResponseProvider, retryCount, dispatchManager.createQueryId(), createSlug(), false, Optional.empty(), OptionalLong.empty());
        }

        public Query(
                String query,
                SessionContext sessionContext,
                DispatchManager dispatchManager,
                ExecutingQueryResponseProvider executingQueryResponseProvider,
                int retryCount,
                QueryId queryId,
                String slug)
        {
            this(query, sessionContext, dispatchManager, executingQueryResponseProvider, retryCount, queryId, slug, false, Optional.empty(), OptionalLong.empty());
        }

        public Query(
                String query,
                SessionContext sessionContext,
                DispatchManager dispatchManager,
                ExecutingQueryResponseProvider executingQueryResponseProvider,
                int retryCount,
                QueryId queryId,
                String slug,
                boolean isRetryQuery,
                Optional<URI> retryUrl,
                OptionalLong retryExpirationEpochTime)
        {
            this.query = requireNonNull(query, "query is null");
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
            this.executingQueryResponseProvider = requireNonNull(executingQueryResponseProvider, "executingQueryResponseProvider is null");
            this.retryCount = retryCount;
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.slug = requireNonNull(slug, "slug is null");
            this.expirationTime = currentTimeMillis() + SECONDS.toMillis(CACHE_CONTROL_MAX_AGE_SEC);
            this.isRetryQuery = isRetryQuery;
            this.retryUrl = requireNonNull(retryUrl, "retryUrl is null");
            this.retryExpirationEpochTime = requireNonNull(retryExpirationEpochTime, "retryExpirationEpochTime is null");
        }

        /**
         * Returns the unique identifier of a query
         */
        public QueryId getQueryId()
        {
            return queryId;
        }

        /**
         * Returns the query string
         */
        public String getQuery()
        {
            return query;
        }

        /**
         * Returns the session context of the query
         */
        public SessionContext getSessionContext()
        {
            return sessionContext;
        }

        /**
         * Returns the secure slug associated with the query
         */
        public String getSlug()
        {
            return slug;
        }

        /**
         * Returns the last token of the result set
         */
        public long getLastToken()
        {
            return lastToken.get();
        }

        /**
         * Returns whether or not this query was stored as a retry query
         */
        public boolean isRetryQuery()
        {
            return isRetryQuery;
        }

        /**
         * Returns the retry attempt of the query
         */
        public int getRetryCount()
        {
            return retryCount;
        }

        /**
         * Returns a cache control with the default max age value
         */
        public CacheControl getDefaultCacheControl()
        {
            long maxAgeMillis = Math.max(0, expirationTime - currentTimeMillis());
            return CacheControl.valueOf("max-age=" + MILLISECONDS.toSeconds(maxAgeMillis));
        }

        /**
         * Checks whether the query has been processed by the dispatchManager
         */
        public synchronized boolean isSubmissionFinished()
        {
            return querySubmissionFuture != null && querySubmissionFuture.isDone();
        }

        /**
         * Submit query to dispatchManager, if required, and for the dispatchManager to process the query
         */
        private ListenableFuture<?> waitForDispatched()
        {
            // if query submission has not finished, wait for it to finish
            synchronized (this) {
                if (querySubmissionFuture == null) {
                    querySubmissionFuture = dispatchManager.createQuery(queryId, slug, retryCount, sessionContext, query);
                }
                if (!querySubmissionFuture.isDone()) {
                    return querySubmissionFuture;
                }
            }
            // otherwise, wait for the query to finish
            return dispatchManager.waitForDispatched(queryId);
        }

        /**
         * Returns a placeholder for query results for the client to poll
         * @param uriInfo {@link jakarta.ws.rs.core.UriInfo}
         * @param xForwardedProto Forwarded protocol (http or https)
         * @return {@link com.facebook.presto.client.QueryResults}
         */
        public synchronized QueryResults getInitialQueryResults(UriInfo uriInfo, String xForwardedProto, String xPrestoPrefixUrl, boolean binaryResults)
        {
            verify(lastToken.get() == 0);
            verify(querySubmissionFuture == null);
            return createQueryResults(
                    1,
                    uriInfo,
                    xForwardedProto,
                    xPrestoPrefixUrl,
                    DispatchInfo.waitingForPrerequisites(NO_DURATION, NO_DURATION),
                    binaryResults);
        }

        public ListenableFuture<Response> toResponse(
                long token,
                UriInfo uriInfo,
                String xForwardedProto,
                String xPrestoPrefixUrl,
                Duration maxWait,
                boolean compressionEnabled,
                boolean nestedDataSerializationEnabled,
                boolean binaryResults)
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
                            xPrestoPrefixUrl,
                            DispatchInfo.waitingForPrerequisites(NO_DURATION, NO_DURATION),
                            binaryResults);

                    return immediateFuture(withCompressionConfiguration(Response.ok(queryResults), compressionEnabled)
                            .cacheControl(getDefaultCacheControl())
                            .build());
                }
            }

            Optional<DispatchInfo> dispatchInfo = dispatchManager.getDispatchInfo(queryId);
            if (!dispatchInfo.isPresent()) {
                // query should always be found, but it may have just been determined to be abandoned
                return immediateFailedFuture(new WebApplicationException(Response
                        .status(NOT_FOUND)
                        .build()));
            }
            long durationUntilExpirationMs = dispatchManager.getDurationUntilExpirationInMillis(queryId);

            if (waitForDispatched().isDone()) {
                Optional<ListenableFuture<Response>> executingQueryResponse = executingQueryResponseProvider.waitForExecutingResponse(
                        queryId,
                        slug,
                        dispatchInfo.get(),
                        uriInfo,
                        xPrestoPrefixUrl,
                        getScheme(xForwardedProto, uriInfo),
                        maxWait,
                        TARGET_RESULT_SIZE,
                        compressionEnabled,
                        nestedDataSerializationEnabled,
                        binaryResults,
                        durationUntilExpirationMs,
                        retryUrl,
                        retryExpirationEpochTime,
                        isRetryQuery);

                if (executingQueryResponse.isPresent()) {
                    return executingQueryResponse.get();
                }
            }

            return immediateFuture(withCompressionConfiguration(Response.ok(
                    createQueryResults(token + 1, uriInfo, xForwardedProto, xPrestoPrefixUrl, dispatchInfo.get(), binaryResults)), compressionEnabled)
                    .cacheControl(getCacheControlMaxAge(durationUntilExpirationMs))
                    .build());
        }

        public synchronized void cancel()
        {
            querySubmissionFuture.addListener(() -> dispatchManager.cancelQuery(queryId), directExecutor());
        }

        private QueryResults createQueryResults(long token, UriInfo uriInfo, String xForwardedProto, String xPrestoPrefixUrl, DispatchInfo dispatchInfo, boolean binaryResults)
        {
            URI nextUri = getNextUri(token, uriInfo, xForwardedProto, xPrestoPrefixUrl, dispatchInfo, binaryResults);

            Optional<QueryError> queryError = dispatchInfo.getFailureInfo()
                    .map(this::toQueryError);

            return createQueuedQueryResults(
                    queryId,
                    nextUri,
                    queryError,
                    uriInfo,
                    xForwardedProto,
                    xPrestoPrefixUrl,
                    dispatchInfo.getElapsedTime(),
                    dispatchInfo.getQueuedTime(),
                    dispatchInfo.getWaitingForPrerequisitesTime());
        }

        private static String createSlug()
        {
            return "x" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        }

        private URI getNextUri(long token, UriInfo uriInfo, String xForwardedProto, String xPrestoPrefixUrl, DispatchInfo dispatchInfo, boolean binaryResults)
        {
            // if failed, query is complete
            if (dispatchInfo.getFailureInfo().isPresent()) {
                return null;
            }
            return getQueuedUri(queryId, slug, token, uriInfo, xForwardedProto, xPrestoPrefixUrl, binaryResults);
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
                    errorCode.isRetriable(),
                    executionFailureInfo.getErrorLocation(),
                    executionFailureInfo.toFailureInfo());
        }
    }
}
