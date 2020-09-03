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
import com.facebook.presto.Session;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.dispatcher.DispatchQueryFactory;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.warnings.WarningCollectorFactory;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.server.HttpRequestSessionContext;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.SessionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.facebook.presto.SystemSessionProperties.getWarningHandlingLevel;
import static com.facebook.presto.server.protocol.QueryResourceUtil.toResponse;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.StatementUtils.getQueryType;
import static com.facebook.presto.util.StatementUtils.isTransactionControlStatement;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/")
@RolesAllowed(USER)
public class ExecutingStatementResource
{
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final DataSize DEFAULT_TARGET_RESULT_SIZE = new DataSize(1, MEGABYTE);
    private static final DataSize MAX_TARGET_RESULT_SIZE = new DataSize(128, MEGABYTE);

    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final Map<Class<? extends Statement>, QueryExecution.QueryExecutionFactory<?>> executionFactories;

    private final QueryPreparer queryPreparer;
    private WarningCollectorFactory warningCollectorFactory;
    private SessionSupplier sessionSupplier;
    private final LocationFactory locationFactory;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;

    private final QueryProvider queryProvider;
    private QueryManager queryManager;
    private DispatchManager dispatchManager;
    private final DispatchQueryFactory dispatchQueryFactory;

    @Inject
    public ExecutingStatementResource(
            @ForStatementResource BoundedExecutor responseExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor,
            Map<Class<? extends Statement>, QueryExecution.QueryExecutionFactory<?>> executionFactories,
            QueryPreparer queryPreparer,
            WarningCollectorFactory warningCollectorFactory,
            SessionSupplier sessionSupplier,
            LocationFactory locationFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            QueryProvider queryProvider,
            QueryManager queryManager,
            DispatchQueryFactory dispatchQueryFactory)
    {
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");

        this.executionFactories = executionFactories;
        this.queryPreparer = queryPreparer;
        this.warningCollectorFactory = warningCollectorFactory;
        this.sessionSupplier = sessionSupplier;
        this.locationFactory = locationFactory;
        this.transactionManager = transactionManager;
        this.accessControl = accessControl;
        this.metadata = metadata;

        this.queryProvider = requireNonNull(queryProvider, "queryProvider is null");
        this.queryManager = queryManager;
        this.dispatchQueryFactory = dispatchQueryFactory;
    }

    @POST
    @Path("/v1/statement/executing/{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postQueryResults(
            @PathParam("queryId") QueryId queryId,
            @QueryParam("slug") String slug,
            @QueryParam("resourceGroupId") ResourceGroupId resourceGroupId,
            String statement,
            @HeaderParam(X_FORWARDED_PROTO) String xForwardedProto,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(statement)) {
            throw badRequest("SQL statement is empty");
        }

        SessionContext sessionContext = new HttpRequestSessionContext(servletRequest);

        Session session = sessionSupplier.createSession(queryId, sessionContext);

        WarningCollector warningCollector = warningCollectorFactory.create(getWarningHandlingLevel(session));

        QueryPreparer.PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, statement, warningCollector);
        Optional<QueryType> queryType = getQueryType(preparedQuery.getStatement().getClass());

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                statement,
                session,
                locationFactory.createQueryLocation(session.getQueryId()),
                new ResourceGroupId("global"),
                queryType,
                isTransactionControlStatement(preparedQuery.getStatement()),
                transactionManager,
                accessControl,
                responseExecutor,
                metadata,
                warningCollector);

        QueryExecution.QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(preparedQuery.getStatement().getClass());
        if (queryExecutionFactory == null) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + preparedQuery.getStatement().getClass().getSimpleName());
        }

        QueryExecution execution = queryExecutionFactory.createQueryExecution(preparedQuery, stateMachine, slug, warningCollector, queryType);
        queryManager.createQuery(execution);

        return Response.ok().build();
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
                query.waitForResults(token, uriInfo, proto, Optional.empty(), wait, targetResultSize),
                results -> toResponse(query, results),
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

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(Response
                .status(BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(message)
                .build());
    }

    private static WebApplicationException notFound(String message)
    {
        throw new WebApplicationException(
                Response.status(NOT_FOUND)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
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
