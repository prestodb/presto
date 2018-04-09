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
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.server.remotetask.Backoff;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.core.UriBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_QUERY_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.execution.QueryState.ACKNOWLEDGED;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.SUBMITTED;
import static com.facebook.presto.execution.QueryState.SUBMITTING;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.spi.StandardErrorCode.GET_QUERY_STATS_FAILED;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_FORWARD_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_FORWARD_TIMEOUT;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@ThreadSafe
public final class DispatchQueryExecution
        implements QueryExecution<QueryResults>
{
    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final JsonCodec<BasicQueryInfo> QUERY_INFO_CODEC = jsonCodec(BasicQueryInfo.class);
    private static final Duration MAX_ERROR_DURATION = new Duration(30, SECONDS);
    private static final String UNKNOWN_FIELD = "unknown";
    private static final Duration ZERO_DURATION = new Duration(0, SECONDS);
    private static final DataSize ZERO_DATA_SIZE = new DataSize(0, BYTE);
    private static final Logger log = Logger.get(DispatchQueryExecution.class);

    private final ExecutorService dispatchExecutor;
    private final ScheduledExecutorService scheduler;
    private final HttpClient httpClient;
    private final DispatchStateMachine dispatchStateMachine;
    private final Backoff backoff;
    private final QueryId queryId;
    private final Statement statement;
    private final String query;
    private final Session session;
    private final URI self;
    private final String updateType;

    private final AtomicReference<ScheduledFuture<?>> acknowledgeQueryFuture = new AtomicReference<>();
    private final AtomicReference<QueryResults> redirectResults = new AtomicReference<>();
    private final AtomicReference<URI> coordinatorUri = new AtomicReference<>();

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();
    private final AtomicReference<ResourceGroupId> resourceGroup = new AtomicReference<>();
    private final AtomicReference<QueryInfo> finalQueryInfo = new AtomicReference<>();

    private final AtomicReference<Optional<String>> setCatalog = new AtomicReference<>();
    private final AtomicReference<Optional<String>> setSchema = new AtomicReference<>();
    private final AtomicReference<Map<String, String>> setSessionProperties = new AtomicReference<>();
    private final AtomicReference<Set<String>> resetSessionProperties = new AtomicReference<>();
    private final AtomicReference<Map<String, String>> addedPreparedStatements = new AtomicReference<>();
    private final AtomicReference<Set<String>> deallocatedPreparedStatements = new AtomicReference<>();
    private final AtomicReference<Optional<TransactionId>> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();

    private final AtomicBoolean isTransactionActive = new AtomicBoolean();
    private final AtomicBoolean shouldSendFailure = new AtomicBoolean();
    private final AtomicReference<BasicQueryInfo> basicQueryInfo = new AtomicReference<>();

    DispatchQueryExecution(
            QueryId queryId,
            String query,
            Session session,
            URI self,
            Statement statement,
            Metadata metadata,
            TransactionManager ignoredTransactionManager,
            SessionPropertyManager sessionPropertyManager,
            AccessControl accessControl,
            SqlParser sqlParser,
            ExecutorService dispatchExecutor,
            ScheduledExecutorService scheduler,
            HttpClient httpClient,
            QueryExplainer queryExplainer,
            List<Expression> parameters)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            Ticker ticker = Ticker.systemTicker();
            requireNonNull(ignoredTransactionManager, "ignoreTransactionManager is null");
            requireNonNull(sessionPropertyManager, "ignoreTransactionManager is null");
            this.dispatchExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null");
            this.scheduler = requireNonNull(scheduler, "scheduler is null");
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
            this.dispatchStateMachine = new DispatchStateMachine(queryId, dispatchExecutor, ticker);
            this.backoff = new Backoff(MAX_ERROR_DURATION, ticker);
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.statement = requireNonNull(statement, "statement is null");
            this.query = requireNonNull(query, "query is null");
            this.session = requireNonNull(session, "session is null");
            this.self = requireNonNull(self, "self is null");

            addStateChangeListener(newState -> {
                if (newState.isDone()) {
                    finish();
                }
                if (newState == SUBMITTED) {
                    shouldSendFailure.set(true);
                }
                if (newState.isDone() && isTransactionActive.compareAndSet(true, false)) {
                    // TODO: set local tx manager to inactive
                }
            });

            // analyze query to capture early failure
            TransactionId transactionId = ignoredTransactionManager.beginTransaction(true);
            Session ignoredSession = new Session(
                    session.getQueryId(),
                    Optional.empty(),   // ignore any transaction id in the current session
                    true,
                    session.getIdentity(),
                    session.getSource(),
                    session.getCatalog(),
                    session.getSchema(),
                    session.getTimeZoneKey(),
                    session.getLocale(),
                    session.getRemoteUserAddress(),
                    session.getUserAgent(),
                    session.getClientInfo(),
                    session.getClientTags(),
                    session.getResourceEstimates(),
                    session.getStartTime(),
                    session.getSystemProperties(),
                    session.getConnectorProperties(),
                    session.getUnprocessedCatalogProperties(),
                    sessionPropertyManager,
                    session.getPreparedStatements());
            ignoredSession = ignoredSession.beginTransactionId(transactionId, ignoredTransactionManager, accessControl);
            Analysis analysis = new Analyzer(ignoredSession, metadata, sqlParser, accessControl, Optional.of(queryExplainer), parameters).analyze(statement);
            this.updateType = analysis.getUpdateType();

            if (session.getTransactionId().isPresent()) {
                // TODO: set local tx manager to active
                isTransactionActive.set(true);
            }
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
        // no-op
    }

    @Override
    public long getUserMemoryReservation()
    {
        BasicQueryInfo queryInfo = basicQueryInfo.get();
        return queryInfo == null ? 0 : queryInfo.getQueryStats().getUserMemoryReservation().toBytes();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        BasicQueryInfo queryInfo = basicQueryInfo.get();
        return queryInfo == null ? ZERO_DURATION : queryInfo.getQueryStats().getTotalCpuTime();
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public void start()
    {
        // TODO: set this from resource group
        setCoordinator(URI.create("http://127.0.0.1:8081"));

        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            try {
                if (!dispatchStateMachine.transitionToSubmitting()) {
                    // query already submitted or done
                    return;
                }

                checkState(coordinatorUri.get() != null);
                log.debug(format("query %s is being submitted to coordinator %s", queryId, coordinatorUri.get()));
                forwardQuery();
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
            dispatchStateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        listenOrFire(
                () -> getState().isDone(),
                () -> {
                    if (finalQueryInfo.get() == null) {
                        finalQueryInfo.compareAndSet(null, getQueryInfo());
                    }
                    stateChangeListener.stateChanged(finalQueryInfo.get());
                });
    }

    @Override
    public void addOutputListener(Consumer<QueryResults> listener)
    {
        listenOrFire(
                () -> redirectResults.get() != null,
                () -> listener.accept(redirectResults.get()));
    }

    /**
     * {@param event} will be fired upon {@param condition} is satisfied.
     * {@param event} will be fired once and only once.
     */
    private void listenOrFire(BooleanSupplier condition, Runnable event)
    {
        AtomicBoolean done = new AtomicBoolean();
        Runnable fireOnceListener = () -> {
            if (condition.getAsBoolean() && done.compareAndSet(false, true)) {
                event.run();
            }
        };

        dispatchStateMachine.addStateChangeListener(newState -> fireOnceListener.run());
        fireOnceListener.run();
    }

    public void setCoordinator(URI uri)
    {
        coordinatorUri.compareAndSet(null, requireNonNull(uri, "uri is null"));
    }

    @Override
    public void cancelQuery()
    {
        failureCause.compareAndSet(null, toFailure(new PrestoException(USER_CANCELED, "Query was canceled")));
        dispatchStateMachine.transitionToFailed();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support cancelStage");
    }

    @Override
    public void fail(Throwable cause)
    {
        failureCause.compareAndSet(null, toFailure(requireNonNull(cause, "throwable is null")));
        dispatchStateMachine.transitionToFailed();
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return dispatchStateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        dispatchStateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        // no-op
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        BasicQueryInfo queryInfo = basicQueryInfo.get();
        boolean hasInfo = queryInfo != null;

        // headers
        Optional<String> setCatalog = this.setCatalog.get();
        Optional<String> setSchema = this.setSchema.get();
        Map<String, String> setSessionProperties = this.setSessionProperties.get();
        Set<String> resetSessionProperties = this.resetSessionProperties.get();
        Map<String, String> addedPreparedStatements = this.addedPreparedStatements.get();
        Set<String> deallocatedPreparedStatements = this.deallocatedPreparedStatements.get();

        Optional<TransactionId> startedTransactionId = this.startedTransactionId.get();
        if (startedTransactionId == null) {
            startedTransactionId = hasInfo ? queryInfo.getStartedTransactionId() : Optional.empty();
        }

        boolean clearTransactionId = this.clearTransactionId.get() || (hasInfo && queryInfo.isClearTransactionId());

        // errorCode
        ExecutionFailureInfo cause = failureCause.get();
        ErrorCode errorCode = null;
        if (cause != null) {
            errorCode = cause.getErrorCode();
        }
        else if (hasInfo) {
            errorCode = queryInfo.getErrorCode();
        }

        // query stats
        QueryStats queryStats = dispatchStateMachine.getQueryStats();
        if (hasInfo) {
            BasicQueryStats basicQueryStats = queryInfo.getQueryStats();

            queryStats = new QueryStats(
                    queryStats.getCreateTime(),
                    queryStats.getExecutionStartTime(),
                    queryStats.getLastHeartbeat(),
                    basicQueryStats.getEndTime(),
                    queryStats.getElapsedTime(),
                    queryStats.getQueuedTime(),
                    queryStats.getAnalysisTime(),
                    queryStats.getDistributedPlanningTime(),
                    queryStats.getTotalPlanningTime(),
                    queryStats.getFinishingTime(),
                    queryStats.getTotalTasks(),
                    queryStats.getRunningTasks(),
                    queryStats.getCompletedTasks(),
                    basicQueryStats.getTotalDrivers(),
                    basicQueryStats.getQueuedDrivers(),
                    basicQueryStats.getRunningDrivers(),
                    queryStats.getBlockedDrivers(),
                    basicQueryStats.getCompletedDrivers(),
                    basicQueryStats.getCumulativeUserMemory(),
                    basicQueryStats.getUserMemoryReservation(),
                    basicQueryStats.getPeakUserMemoryReservation(),
                    queryStats.getPeakTotalMemoryReservation(),
                    queryStats.getPeakTaskTotalMemory(),
                    true,
                    queryStats.getTotalScheduledTime(),
                    basicQueryStats.getTotalCpuTime(),
                    queryStats.getTotalUserTime(),
                    queryStats.getTotalBlockedTime(),
                    basicQueryStats.isFullyBlocked(),
                    basicQueryStats.getBlockedReasons(),
                    queryStats.getRawInputDataSize(),
                    queryStats.getRawInputPositions(),
                    queryStats.getProcessedInputDataSize(),
                    queryStats.getRawInputPositions(),
                    queryStats.getOutputDataSize(),
                    queryStats.getOutputPositions(),
                    queryStats.getPhysicalWrittenDataSize(),
                    queryStats.getStageGcStatistics(),
                    queryStats.getOperatorSummaries());
        }

        return new QueryInfo(
                queryId,
                session.toSessionRepresentation(),
                hasInfo ? queryInfo.getState() : getState(),
                hasInfo ? queryInfo.getMemoryPool() : GENERAL_POOL,
                hasInfo && queryInfo.isScheduled(),
                hasInfo ? queryInfo.getSelf() : self,
                ImmutableList.of(),
                query,
                queryStats,
                setCatalog == null ? Optional.empty() : setCatalog,
                setSchema == null ? Optional.empty() : setSchema,
                setSessionProperties == null ? ImmutableMap.of() : setSessionProperties,
                resetSessionProperties == null ? ImmutableSet.of() : resetSessionProperties,
                addedPreparedStatements == null ? ImmutableMap.of() : addedPreparedStatements,
                deallocatedPreparedStatements == null ? ImmutableSet.of() : deallocatedPreparedStatements,
                startedTransactionId == null ? Optional.empty() : startedTransactionId,
                clearTransactionId,
                updateType,
                Optional.empty(),
                cause == null ? null : cause.toFailureInfo(),
                cause == null ? null : errorCode,
                ImmutableSet.of(),
                Optional.empty(),
                true,
                getResourceGroup().map(ResourceGroupId::toString));
    }

    @Override
    public QueryState getState()
    {
        return dispatchStateMachine.getState();
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

    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException("DispatchQueryExecution does not support getQueryPlan");
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

        builder.addHeader(PRESTO_TRANSACTION_ID, session.getTransactionId().isPresent() ? session.getTransactionId().get().toString() : "NONE");

        return builder.build();
    }

    private void forwardQuery()
    {
        checkState(coordinatorUri.get() != null);
        URI forwardUri = UriBuilder.fromUri(coordinatorUri.get()).replacePath("v1/statement").build();
        ListenableFuture<QueryResults> resultFuture = httpClient.executeAsync(buildQueryRequest(forwardUri, session, query), new QueryResultsResponseHandler());

        Futures.addCallback(resultFuture, new FutureCallback<QueryResults>()
        {
            @Override
            public void onSuccess(QueryResults queryResults)
            {
                try {
                    recordHeartbeat();
                    backoff.success();
                    if (!dispatchStateMachine.transitionToSubmitted()) {
                        // either finished or another thread has already received the response from the coordinator
                        checkState(getState().isDone() || getState() == SUBMITTED);
                        return;
                    }
                    log.debug(format("query %s has been forwarded to coordinator %s", queryId, coordinatorUri));

                    // send request to the coordinator to decide when to finish the execution
                    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(DispatchQueryExecution.this::updateQueryInfo, 0, 1, SECONDS);
                    if (!acknowledgeQueryFuture.compareAndSet(null, future)) {
                        // some other request has scheduled a request
                        future.cancel(false);
                    }
                }
                catch (Exception e) {
                    PrestoException exception = new PrestoException(QUERY_FORWARD_ERROR, e);
                    fail(exception);
                    throw exception;
                }
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                log.debug("Forwarding query %s to %s failed %s", queryId, forwardUri, throwable);

                if (throwable instanceof PrestoException) {
                    fail(throwable);
                    throwIfInstanceOf(throwable, Error.class);
                }
                else if (backoff.failure()) {
                    String message = format("Encountered too many errors talking to a coordinator node. (%s - %s failures, failure duration %s, total failed request time %s)",
                            forwardUri,
                            backoff.getFailureCount(),
                            backoff.getFailureDuration().convertTo(SECONDS),
                            backoff.getFailureRequestTimeTotal().convertTo(SECONDS));
                    throwable = new PrestoException(QUERY_FORWARD_TIMEOUT, message, throwable);

                    fail(throwable);
                    throwIfInstanceOf(throwable, Error.class);
                }
                // back off and try again
                scheduler.schedule(DispatchQueryExecution.this::forwardQuery, backoff.getBackoffDelayNanos(), NANOSECONDS);
            }
        }, dispatchExecutor);
    }

    /**
     * Periodically send request to /v1/query/{querId}/dispatch/{done} to the get the query info on the coordinator.
     * flag "done" denotes if we can stop sending request to get the query info so that the coordinator can expire the query info.
     */
    private void updateQueryInfo()
    {
        checkState(coordinatorUri.get() != null);
        URI uri = UriBuilder.fromUri(coordinatorUri.get()).replacePath("v1/query").path(queryId.toString()).path("basic").build();

        ListenableFuture<JsonResponse<BasicQueryInfo>> future = httpClient.executeAsync(prepareGet().setUri(uri).build(), createFullJsonResponseHandler(QUERY_INFO_CODEC));
        Futures.addCallback(future, new FutureCallback<JsonResponse<BasicQueryInfo>>()
        {
            @Override
            public void onSuccess(JsonResponse<BasicQueryInfo> jsonResponse)
            {
                try {
                    recordHeartbeat();
                    if (jsonResponse.getStatusCode() == HttpStatus.GONE.code()) {
                        PrestoException exception = new PrestoException(GET_QUERY_STATS_FAILED, format("query %s is gone on coordinator", queryId));
                        fail(exception);
                        throw exception;
                    }
                    basicQueryInfo.set(jsonResponse.getValue());
                    if (basicQueryInfo.get().getState() == FINISHED) {
                        shouldSendFailure.set(false);
                        dispatchStateMachine.transitionToAcknowledged();
                    }
                    else if (basicQueryInfo.get().getState() == FAILED) {
                        shouldSendFailure.set(false);
                        PrestoException exception = new PrestoException(
                                () -> basicQueryInfo.get().getErrorCode(),
                                format("query %s failed on %s", queryId, coordinatorUri.get()));
                        fail(exception);
                        throw exception;
                    }
                }
                catch (Exception exception) {
                    if (exception instanceof PrestoException) {
                        fail(exception);
                        throw exception;
                    }
                    log.debug("checking status of dispatched query %s to %s failed %s", queryId, uri, exception);
                }
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                log.debug("checking status of dispatched query %s to %s failed %s", queryId, uri, throwable);
                // ignore failure
            }
        }, dispatchExecutor);
    }

    private void finish()
    {
        // stop sending the acknowledgement checking
        ScheduledFuture<?> future = acknowledgeQueryFuture.get();
        if (future != null) {
            future.cancel(false);
            acknowledgeQueryFuture.set(null);
        }

        BasicQueryInfo queryInfo = basicQueryInfo.get();
        if (queryInfo == null) {
            return;
        }

        // set transaction info if applicable
        Optional<TransactionId> currentTransactionId = session.getTransactionId();
        if (queryInfo.isClearTransactionId()) {
            checkState(currentTransactionId.isPresent());
            // TODO: remove tx
        }
        else if (queryInfo.getStartedTransactionId().isPresent()) {
            checkState(!queryInfo.isClearTransactionId());
            // TODO: add tx
        }

        // fail the query on the coordinator if necessary
        if (failureCause.get() != null && shouldSendFailure.get()) {
            // TODO: fail query on coordinator
        }
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

    private static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    @ThreadSafe
    private static class DispatchStateMachine
    {
        private final Ticker ticker;
        private final StateMachine<QueryState> queryStateMachine;

        // stats
        private final long createNanos;
        private final DateTime createTime = DateTime.now();
        private final AtomicLong endNanos = new AtomicLong();
        private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
        private final AtomicReference<Duration> queuedTime = new AtomicReference<>();
        private final AtomicReference<DateTime> submitTime = new AtomicReference<>();
        private final AtomicReference<DateTime> endTime = new AtomicReference<>();

        DispatchStateMachine(QueryId queryId, Executor dispatchExecutor, Ticker ticker)
        {
            this.ticker = requireNonNull(ticker, "ticker is null");
            this.queryStateMachine = new StateMachine<>("query " + queryId, dispatchExecutor, QUEUED, TERMINAL_QUERY_STATES);
            this.createNanos = tickerNanos();

            queryStateMachine.addStateChangeListener(newState -> log.debug("Query %s is %s", queryId, newState));
        }

        boolean transitionToSubmitting()
        {
            queuedTime.compareAndSet(null, succinctNanos(tickerNanos() - createNanos).convertToMostSuccinctTimeUnit());
            submitTime.compareAndSet(null, DateTime.now());
            return queryStateMachine.compareAndSet(QUEUED, SUBMITTING);
        }

        boolean transitionToSubmitted()
        {
            return queryStateMachine.compareAndSet(SUBMITTING, SUBMITTED);
        }

        boolean transitionToAcknowledged()
        {
            recordEnd();
            return queryStateMachine.compareAndSet(SUBMITTED, ACKNOWLEDGED);
        }

        boolean transitionToFailed()
        {
            recordEnd();
            return queryStateMachine.setIf(FAILED, currentState -> !currentState.isDone());
        }

        QueryState getState()
        {
            return queryStateMachine.get();
        }

        void recordHeartbeat()
        {
            lastHeartbeat.set(DateTime.now());
        }

        void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
        {
            queryStateMachine.addStateChangeListener(stateChangeListener);
        }

        ListenableFuture<QueryState> getStateChange(QueryState currentState)
        {
            return queryStateMachine.getStateChange(currentState);
        }

        QueryStats getQueryStats()
        {
            Duration elapsedTime;
            if (endNanos.get() != 0) {
                elapsedTime = new Duration(endNanos.get() - createNanos, NANOSECONDS);
            }
            else {
                elapsedTime = succinctNanos(tickerNanos() - createNanos);
            }

            return new QueryStats(
                    createTime,
                    submitTime.get(),
                    lastHeartbeat.get(),
                    endTime.get(),
                    elapsedTime.convertToMostSuccinctTimeUnit(),
                    queuedTime.get(),
                    ZERO_DURATION,
                    ZERO_DURATION,
                    ZERO_DURATION,
                    ZERO_DURATION,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0.0,
                    ZERO_DATA_SIZE,
                    ZERO_DATA_SIZE,
                    ZERO_DATA_SIZE,
                    ZERO_DATA_SIZE,
                    false,
                    ZERO_DURATION,
                    ZERO_DURATION,
                    ZERO_DURATION,
                    ZERO_DURATION,
                    false,
                    ImmutableSet.of(),
                    ZERO_DATA_SIZE,
                    0,
                    ZERO_DATA_SIZE,
                    0,
                    ZERO_DATA_SIZE,
                    0,
                    ZERO_DATA_SIZE,
                    ImmutableList.of(),
                    ImmutableList.of());
        }

        private void recordEnd()
        {
            queuedTime.compareAndSet(null, succinctNanos(tickerNanos() - createNanos).convertToMostSuccinctTimeUnit());
            submitTime.compareAndSet(null, DateTime.now());
            endTime.compareAndSet(null, DateTime.now());
            endNanos.compareAndSet(0, tickerNanos());
        }

        private long tickerNanos()
        {
            return ticker.read();
        }
    }

    private class QueryResultsResponseHandler
            implements ResponseHandler<QueryResults, RuntimeException>
    {
        private final FullJsonResponseHandler<QueryResults> handler;

        QueryResultsResponseHandler()
        {
            handler = createFullJsonResponseHandler(QUERY_RESULTS_CODEC);
        }

        @Override
        public QueryResults handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public QueryResults handle(Request request, Response response)
        {
            JsonResponse<QueryResults> jsonResponse;
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

            setRedirectInfo(jsonResponse);
            return jsonResponse.getValue();
        }

        /**
         * This method update the header and body of the response that should be fetched by the client
         */
        private void setRedirectInfo(JsonResponse<QueryResults> jsonResponse)
        {
            // Set redirect response.
            // At this point we can safely transition the state to SUBMITTED so that the client can fetch the results.
            // Both query info and query results are up to date.

            // Set the headers
            DispatchQueryExecution.this.setCatalog.compareAndSet(null, Optional.ofNullable(jsonResponse.getHeader(PRESTO_SET_CATALOG)));
            DispatchQueryExecution.this.setSchema.compareAndSet(null, Optional.ofNullable(jsonResponse.getHeader(PRESTO_SET_SCHEMA)));

            ImmutableMap.Builder<String, String> setSessionProperties = ImmutableMap.builder();
            ImmutableSet.Builder<String> resetSessionProperties = ImmutableSet.builder();
            for (String setSession : jsonResponse.getHeaders(PRESTO_SET_SESSION)) {
                List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setSession);
                if (keyValue.size() != 2) {
                    continue;
                }
                setSessionProperties.put(keyValue.get(0), keyValue.get(1));
            }
            resetSessionProperties.addAll(jsonResponse.getHeaders(PRESTO_CLEAR_SESSION));
            DispatchQueryExecution.this.setSessionProperties.compareAndSet(null, setSessionProperties.build());
            DispatchQueryExecution.this.resetSessionProperties.compareAndSet(null, resetSessionProperties.build());

            ImmutableMap.Builder<String, String> addedPreparedStatements = ImmutableMap.builder();
            ImmutableSet.Builder<String> deallocatedPreparedStatements = ImmutableSet.builder();
            for (String entry : jsonResponse.getHeaders(PRESTO_ADDED_PREPARE)) {
                List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(entry);
                if (keyValue.size() != 2) {
                    continue;
                }
                addedPreparedStatements.put(urlDecode(keyValue.get(0)), urlDecode(keyValue.get(1)));
            }
            for (String entry : jsonResponse.getHeaders(PRESTO_DEALLOCATED_PREPARE)) {
                deallocatedPreparedStatements.add(urlDecode(entry));
            }
            DispatchQueryExecution.this.addedPreparedStatements.compareAndSet(null, addedPreparedStatements.build());
            DispatchQueryExecution.this.deallocatedPreparedStatements.compareAndSet(null, deallocatedPreparedStatements.build());

            DispatchQueryExecution.this.startedTransactionId.compareAndSet(
                    null,
                    Optional.ofNullable(jsonResponse.getHeader(PRESTO_STARTED_TRANSACTION_ID)).map(TransactionId::valueOf));
            DispatchQueryExecution.this.clearTransactionId.set(jsonResponse.getHeader(PRESTO_CLEAR_TRANSACTION_ID) != null);

            // Set the body
            redirectResults.compareAndSet(null, jsonResponse.getValue());
        }
    }

    public static class DispatchQueryExecutionFactory
            implements QueryExecutionFactory<DispatchQueryExecution>
    {
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final SqlParser sqlParser;
        private final LocationFactory locationFactory;
        private final TransactionManager transactionManager;
        private final SessionPropertyManager sessionPropertyManager;
        private final ExecutorService dispatchExecutor;
        private final ScheduledExecutorService scheduler;
        private final HttpClient httpClient;
        private final QueryExplainer queryExplainer;

        @Inject
        public DispatchQueryExecutionFactory(
                Metadata metadata,
                AccessControl accessControl,
                SqlParser sqlParser,
                LocationFactory locationFactory,
                TransactionManager transactionManager,
                SessionPropertyManager sessionPropertyManager,
                @ForQueryExecution ExecutorService dispatchExecutor,
                @ForQueryExecution ScheduledExecutorService scheduler,
                @ForQueryExecution HttpClient httpClient,
                QueryExplainer queryExplainer)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
            this.dispatchExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null");
            this.scheduler = requireNonNull(scheduler, "scheduler is null");
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
                    transactionManager,
                    sessionPropertyManager,
                    accessControl,
                    sqlParser,
                    dispatchExecutor,
                    scheduler,
                    httpClient,
                    queryExplainer,
                    parameters);
        }
    }
}
