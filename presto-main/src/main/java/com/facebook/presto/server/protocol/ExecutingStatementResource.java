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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREFIX_URL;
import static com.facebook.presto.server.protocol.QueryResourceUtil.abortIfPrefixUrlInvalid;
import static com.facebook.presto.server.protocol.QueryResourceUtil.toResponse;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/")
@RolesAllowed(USER)
public class ExecutingStatementResource
{
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final DataSize DEFAULT_TARGET_RESULT_SIZE = new DataSize(1, MEGABYTE);
    private static final DataSize MAX_TARGET_RESULT_SIZE = new DataSize(128, MEGABYTE);

    private final BoundedExecutor responseExecutor;
    private final LocalQueryProvider queryProvider;
    private final QueryManager queryManager;
    private final boolean compressionEnabled;
    private final boolean nestedDataSerializationEnabled;
    private final QueryBlockingRateLimiter queryRateLimiter;

    @Inject
    public ExecutingStatementResource(
            @ForStatementResource BoundedExecutor responseExecutor,
            LocalQueryProvider queryProvider,
            QueryManager queryManager,
            ServerConfig serverConfig,
            QueryBlockingRateLimiter queryRateLimiter)
    {
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.queryProvider = requireNonNull(queryProvider, "queryProvider is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.compressionEnabled = requireNonNull(serverConfig, "serverConfig is null").isQueryResultsCompressionEnabled();
        this.nestedDataSerializationEnabled = requireNonNull(serverConfig, "serverConfig is null").isNestedDataSerializationEnabled();
        this.queryRateLimiter = requireNonNull(queryRateLimiter, "queryRateLimiter is null");
    }

    @Managed
    @Nested
    public TimeStat getRateLimiterBlockTime()
    {
        return queryRateLimiter.getRateLimiterBlockTime();
    }

    @GET
    @Path("/v1/statement/executing/{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("slug") String slug,
            @QueryParam("maxWait") Duration maxWait,
            @QueryParam("targetResultSize") DataSize targetResultSize,
            @DefaultValue("false") @QueryParam("binaryResults") boolean binaryResults,
            @HeaderParam(X_FORWARDED_PROTO) String proto,
            @HeaderParam(PRESTO_PREFIX_URL) String xPrestoPrefixUrl,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        if (targetResultSize == null) {
            targetResultSize = DEFAULT_TARGET_RESULT_SIZE;
        }
        else {
            targetResultSize = Ordering.natural().min(targetResultSize, MAX_TARGET_RESULT_SIZE);
        }
        if (isNullOrEmpty(proto)) {
            proto = uriInfo.getRequestUri().getScheme();
        }

        abortIfPrefixUrlInvalid(xPrestoPrefixUrl);

        Query query = queryProvider.getQuery(queryId, slug);
        ListenableFuture<Double> acquirePermitAsync = queryRateLimiter.acquire(queryId);
        String effectiveFinalProto = proto;
        DataSize effectiveFinalTargetResultSize = targetResultSize;
        ListenableFuture<QueryResults> waitForResultsAsync = transformAsync(
                acquirePermitAsync,
                acquirePermitTimeSeconds -> {
                    queryRateLimiter.addRateLimiterBlockTime(new Duration(acquirePermitTimeSeconds, SECONDS));
                    return query.waitForResults(token, uriInfo, effectiveFinalProto, wait, effectiveFinalTargetResultSize, binaryResults);
                },
                responseExecutor);
        long durationUntilExpirationMs = queryManager.getDurationUntilExpirationInMillis(queryId);
        ListenableFuture<Response> queryResultsFuture = transform(
                waitForResultsAsync,
                results -> toResponse(query, results, xPrestoPrefixUrl, compressionEnabled, nestedDataSerializationEnabled, durationUntilExpirationMs),
                directExecutor());
        bindAsyncResponse(asyncResponse, queryResultsFuture, responseExecutor);
    }

    @DELETE
    @Path("/v1/statement/executing/{queryId}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @PathParam("token") long token,
            @QueryParam("slug") String slug)
    {
        queryProvider.cancel(queryId, slug);
        return Response.noContent().build();
    }
}
