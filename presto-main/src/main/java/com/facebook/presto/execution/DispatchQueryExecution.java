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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Joiner;
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.core.UriBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_QUERY_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.execution.QueryState.ACCEPTED;
import static com.facebook.presto.execution.QueryState.ACKNOWLEDGED;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.SUBMITTING;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_FORWARD_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@ThreadSafe
public final class DispatchQueryExecution
        extends AbstractQueryExecution
{
    private static final String UNKNOWN_FIELD = "unknown";
    private static final Duration ZERO_DURATION = new Duration(0, SECONDS);
    private static final Logger log = Logger.get(DispatchQueryExecution.class);

    private final Ticker ticker;
    private final HttpClient httpClient;
    private final StateMachine<QueryState> queryStateMachine;

    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final URI self;

    // stats
    private final DateTime createTime = DateTime.now();
    private final long createNanos;
    private final AtomicLong endNanos = new AtomicLong();
    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicReference<DateTime> endTime = new AtomicReference<>();
    private final AtomicReference<Duration> queuedTime = new AtomicReference<>();

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();
    private final AtomicReference<ResourceGroupId> resourceGroup = new AtomicReference<>();

    private final AtomicReference<QueryResults> redirectResults = new AtomicReference<>();
    private final AtomicReference<URI> coordinatorUri = new AtomicReference<>();
    private final Queue<Consumer<QueryResults>> redirectResultsListeners = new ConcurrentLinkedQueue<>();

    public DispatchQueryExecution(
            QueryId queryId,
            String query,
            Session session,
            URI self,
            Statement statement,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            ExecutorService queryExecutor,
            HttpClient httpClient,
            QueryExplainer queryExplainer,
            List<Expression> parameters)
    {
        super(statement);

        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            this.createNanos = tickerNanos();
            this.ticker = Ticker.systemTicker();
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
            this.queryStateMachine = new StateMachine<>("query " + query, queryExecutor, QUEUED, TERMINAL_QUERY_STATES);
            queryStateMachine.addStateChangeListener(newState -> {
                log.debug("Query %s is %s", queryId, newState);
            });

            this.queryId = requireNonNull(queryId, "queryId is null");
            this.query = requireNonNull(query, "query is null");
            this.session = requireNonNull(session, "session is null");
            this.self = requireNonNull(self, "self is null");

            // analyze query to capture early failure
            new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(queryExplainer), parameters).analyze(statement);
        }
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support getMemoryPool");
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support setMemoryPool");
    }

    @Override
    public long getUserMemoryReservation()
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support setMemoryPool");
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return ZERO_DURATION;
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            try {
                // transition to submitting state
                queuedTime.compareAndSet(null, succinctNanos(tickerNanos() - createNanos).convertToMostSuccinctTimeUnit());
                if (!queryStateMachine.compareAndSet(QUEUED, SUBMITTING)) {
                    // query already submitted or done
                    return;
                }

                // TODO: make this async
                // TODO: add backoff
                URI coordinatorUri = this.coordinatorUri.get();
                checkState(coordinatorUri != null);
                redirectResults.compareAndSet(
                        null,
                        httpClient.execute(
                                buildQueryRequest(UriBuilder.fromUri(coordinatorUri).replacePath("v1/statement").build(), session, query),
                                new QueryResultsResponseHandler()));

                queryStateMachine.compareAndSet(SUBMITTING, ACCEPTED);
                log.debug(format("query %s has been forwarded to coordinator %s", queryId, coordinatorUri));

                for (Consumer<QueryResults> listener : redirectResultsListeners) {
                    listener.accept(redirectResults.get());
                }

                // TODO: periodically pull data from coordinator
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            queryStateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support addFinalQueryInfoListener");
    }

    @Override
    public void cancelQuery()
    {
        recordEndTime();
        failureCause.compareAndSet(null, toFailure(new PrestoException(USER_CANCELED, "Query was canceled")));
        queryStateMachine.setIf(FAILED, currentState -> !currentState.isDone());
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support cancelStage");
    }

    @Override
    public void fail(Throwable cause)
    {
        recordEndTime();
        failureCause.compareAndSet(null, toFailure(requireNonNull(cause, "throwable is null")));
        queryStateMachine.setIf(FAILED, currentState -> !currentState.isDone());
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support addOutputInfoListener");
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return queryStateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        lastHeartbeat.set(DateTime.now());
    }

    @Override
    public void pruneInfo()
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support pruneInfo");
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support getQueryInfo");
    }

    @Override
    public QueryState getState()
    {
        return queryStateMachine.get();
    }

    @Override
    public Optional<ResourceGroupId> getResourceGroup()
    {
        return Optional.ofNullable(resourceGroup.get());
    }

    @Override
    public void setResourceGroup(ResourceGroupId resourceGroupId)
    {
        resourceGroup.compareAndSet(null, requireNonNull(resourceGroupId, "resourceGroupId is null"));
    }

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support getQueryPlan");
    }

    @Override
    public void setTargetCoordinator(URI uri)
    {
        coordinatorUri.compareAndSet(null, requireNonNull(uri, "uri is null"));
    }

    @Override
    public void addRedirectResultsListner(Consumer<QueryResults> listener)
    {
        redirectResultsListeners.add(requireNonNull(listener, "listener is null"));
    }

    private static Request buildQueryRequest(URI uri, Session session, String query)
    {
        Request.Builder builder = preparePost()
                .addHeader(PRESTO_QUERY_ID, session.getQueryId().toString())
                .addHeader(PRESTO_USER, session.getUser())
                .addHeader(USER_AGENT, session.getUserAgent().orElse(UNKNOWN_FIELD))
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8));

        if (session.getSource() != null) {
            builder.addHeader(PRESTO_SOURCE, session.getSource().orElse(UNKNOWN_FIELD));
        }
        if (session.getClientTags() != null && !session.getClientTags().isEmpty()) {
            builder.addHeader(PRESTO_CLIENT_TAGS, Joiner.on(",").join(session.getClientTags()));
        }
        if (session.getClientInfo() != null) {
            builder.addHeader(PRESTO_CLIENT_INFO, session.getClientInfo().orElse(UNKNOWN_FIELD));
        }
        if (session.getCatalog() != null) {
            builder.addHeader(PRESTO_CATALOG, session.getCatalog().orElse(UNKNOWN_FIELD));
        }
        if (session.getSchema() != null) {
            builder.addHeader(PRESTO_SCHEMA, session.getSchema().orElse(UNKNOWN_FIELD));
        }
        builder.addHeader(PRESTO_TIME_ZONE, session.getTimeZoneKey().getId());
        if (session.getLocale() != null) {
            builder.addHeader(PRESTO_LANGUAGE, session.getLocale().toLanguageTag());
        }

        Map<String, String> property = session.getSystemProperties();
        for (Map.Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PRESTO_SESSION, entry.getKey() + "=" + entry.getValue());
        }

        Map<String, String> statements = session.getPreparedStatements();
        for (Map.Entry<String, String> entry : statements.entrySet()) {
            builder.addHeader(PRESTO_PREPARED_STATEMENT, urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
        }

        // TODO: add transaction
        builder.addHeader(PRESTO_TRANSACTION_ID, "NONE");

        return builder.build();
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

    private boolean transitionToAcknowledged()
    {
        recordEndTime();
        return queryStateMachine.compareAndSet(ACCEPTED, ACKNOWLEDGED);
    }

    private void recordEndTime()
    {
        endTime.compareAndSet(null, DateTime.now());
        endNanos.compareAndSet(0, tickerNanos());
    }

    private long tickerNanos()
    {
        return ticker.read();
    }

    public static class DispatchQueryExecutionFactory
            implements QueryExecutionFactory<DispatchQueryExecution>
    {
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final SqlParser sqlParser;
        private final LocationFactory locationFactory;
        private final ExecutorService queryExecutor;
        private final HttpClient httpClient;
        private final QueryExplainer queryExplainer;

        @Inject
        DispatchQueryExecutionFactory(
                Metadata metadata,
                AccessControl accessControl,
                SqlParser sqlParser,
                LocationFactory locationFactory,
                @ForQueryExecution ExecutorService queryExecutor,
                @ForQueryExecution HttpClient httpClient,
                QueryExplainer queryExplainer)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        }

        @Override
        public DispatchQueryExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement, List<Expression> parameters)
        {
            return new DispatchQueryExecution(
                    queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    statement,
                    metadata,
                    accessControl,
                    sqlParser,
                    queryExecutor,
                    httpClient,
                    queryExplainer,
                    parameters);
        }
    }

    private static class QueryResultsResponseHandler
            implements ResponseHandler<QueryResults, RuntimeException>
    {
        private static final JsonCodec<QueryResults> CODEC = jsonCodec(QueryResults.class);
        private final FullJsonResponseHandler<QueryResults> handler;

        QueryResultsResponseHandler()
        {
            handler = createFullJsonResponseHandler(CODEC);
        }

        @Override
        public QueryResults handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public QueryResults handle(Request request, Response response)
        {
            FullJsonResponseHandler.JsonResponse<QueryResults> jsonResponse;
            try {
                jsonResponse = handler.handle(request, response);
            }
            catch (RuntimeException e) {
                throw new PrestoException(QUERY_FORWARD_ERROR, format("Error fetching %s: %s", request.getUri().toASCIIString(), e.getMessage()), e);
            }

            if (response.getStatusCode() != HttpStatus.OK.code()) {
                throw new PrestoException(QUERY_FORWARD_ERROR, format(
                        "Expected response code to be 200, but was %s %s:%n%s",
                        response.getStatusCode(),
                        response.getStatusMessage(),
                        response.toString()));
            }

            // invalid content type can happen when an error page is returned, but is unlikely given the above 200
            String contentType = response.getHeader(CONTENT_TYPE);
            if (contentType == null) {
                throw new PrestoException(QUERY_FORWARD_ERROR, format("%s header is not set: %s", CONTENT_TYPE, response));
            }
            if (!contentType.equals(APPLICATION_JSON)) {
                throw new PrestoException(QUERY_FORWARD_ERROR, format("Expected %s response from server but got %s", APPLICATION_JSON, contentType));
            }

            return jsonResponse.getValue();
        }
    }
}
