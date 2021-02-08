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
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.facebook.presto.server.protocol.QueryResourceUtil.toResponse;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
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
    private final boolean compressionEnabled;

    @Inject
    public ExecutingStatementResource(
            @ForStatementResource BoundedExecutor responseExecutor,
            LocalQueryProvider queryProvider,
            ServerConfig serverConfig)
    {
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.queryProvider = requireNonNull(queryProvider, "queryProvider is null");
        this.compressionEnabled = requireNonNull(serverConfig, "serverConfig is null").isQueryResultsCompressionEnabled();
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
            @HeaderParam(X_FORWARDED_PROTO) String proto,
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

        Query query = queryProvider.getQuery(queryId, slug);
        ListenableFuture<Response> queryResultsFuture = transform(
                query.waitForResults(token, uriInfo, proto, wait, targetResultSize),
                results -> toResponse(query, results, compressionEnabled),
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
