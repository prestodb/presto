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
import com.facebook.presto.execution.DispatchQueryExecution.DispatchQueryExecutionFactory;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.SUBMITTING;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public final class LazyQueryExecution
        implements QueryExecution<LazyOutput>
{
    private static final Duration ZERO_DURATION = new Duration(0, SECONDS);
    private static final DataSize ZERO_DATA_SIZE = new DataSize(0, BYTE);

    private final ExecutorService lazyExecutor;
    private final QueryExecutionFactory<? extends QueryExecution<QueryOutputInfo>> sqlQueryExecutionFactory;
    private final DispatchQueryExecutionFactory dispatchQueryExecutionFactory;

    private final URI self;
    private final QueryId queryId;
    private final Statement statement;
    private final String query;
    private final Session session;
    private final List<Expression> parameters;

    private final LazyStateMachine lazyStateMachine;

    private final AtomicReference<QueryExecution<?>> delegate = new AtomicReference<>();
    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();
    private final AtomicReference<URI> coordinatorUri = new AtomicReference<>();
    private final AtomicReference<VersionedMemoryPoolId> versionedMemoryPoolId = new AtomicReference<>(new VersionedMemoryPoolId(GENERAL_POOL, 0));
    private final AtomicReference<ResourceGroupId> resourceGroupId = new AtomicReference<>();
    private final AtomicBoolean isRemote = new AtomicBoolean();

    private final AtomicReference<QueryExecution<QueryResults>> dispatchQueryExecution = new AtomicReference<>();
    private final AtomicReference<QueryExecution<QueryOutputInfo>> sqlQueryExecution = new AtomicReference<>();

    LazyQueryExecution(
            URI self,
            QueryId queryId,
            String query,
            Session session,
            Statement statement,
            List<Expression> parameters,
            ExecutorService lazyExecutor,
            QueryExecutionFactory<? extends QueryExecution<QueryOutputInfo>> sqlQueryExecutionFactory,
            DispatchQueryExecutionFactory dispatchQueryExecutionFactory)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            this.lazyExecutor = requireNonNull(lazyExecutor, "lazyExecutor is null");
            this.sqlQueryExecutionFactory = requireNonNull(sqlQueryExecutionFactory, "sqlQueryExecutionFactory is null");
            this.dispatchQueryExecutionFactory = requireNonNull(dispatchQueryExecutionFactory, "dispatchQueryExecutionFactory is null");
            this.self = UriBuilder.fromUri(requireNonNull(self, "self is null")).replacePath("").build();

            this.queryId = requireNonNull(queryId, "queryId is null");
            this.statement = requireNonNull(statement, "statement is null");
            this.query = requireNonNull(query, "query is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");

            this.lazyStateMachine = new LazyStateMachine(queryId, lazyExecutor, Ticker.systemTicker());

            // TODO set and reset tx
        }
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return versionedMemoryPoolId.get();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        versionedMemoryPoolId.set(poolId);
        synchronized (this) {
            if (delegate.get() != null) {
                delegate.get().setMemoryPool(poolId);
            }
        }
    }

    @Override
    public long getUserMemoryReservation()
    {
        if (delegate.get() == null) {
            return 0;
        }
        return delegate.get().getUserMemoryReservation();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        if (delegate.get() == null) {
            return ZERO_DURATION;
        }
        return delegate.get().getTotalCpuTime();
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
        setCoordinator(URI.create("http://127.0.0.1:8080"));

        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            try {
                // set delegate before transitioning the state
                isRemote.set(!coordinatorUri.get().equals(self));

                // TODO: set coodinator
                isRemote.set(false);
                if (!isRemote.get()) {
                    sqlQueryExecution.compareAndSet(null, sqlQueryExecutionFactory.createQueryExecution(queryId, query, session, statement, parameters));
                    delegate.set(sqlQueryExecution.get());
                }
                else {
                    dispatchQueryExecution.compareAndSet(null, dispatchQueryExecutionFactory.createQueryExecution(queryId, query, session, statement, parameters));
                    delegate.set(dispatchQueryExecution.get());
                    // TODO: set uri
                }

                if (!lazyStateMachine.transitionToFinished()) {
                    checkState(lazyStateMachine.getState() == FAILED);
                    checkState(failureCause.get() != null);
                    delegate.get().fail(failureCause.get().toException());
                    return;
                }
                delegate.get().start();
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void addOutputListener(Consumer<LazyOutput> listener)
    {
        listenOrFire(
                () -> {
                    // Set an empty result to trigger the delegation of LazyQuery.
                    // This is in case some query execution's addOutputListener is a no-op
                    listener.accept(new LazyOutput(false, isRemote.get(), Optional.empty(), Optional.empty()));

                    // then set the actual output based on the output listener
                    if (isRemote.get()) {
                        checkState(dispatchQueryExecution.get() != null);
                        dispatchQueryExecution.get().addOutputListener(
                                queryResults -> listener.accept(
                                        new LazyOutput(false, true, Optional.empty(), Optional.of(queryResults))));
                    }
                    else {
                        checkState(sqlQueryExecution.get() != null);
                        sqlQueryExecution.get().addOutputListener(
                                queryOutputInfo -> listener.accept(
                                        new LazyOutput(false, false, Optional.of(queryOutputInfo), Optional.empty())));
                    }
                },
                () -> listener.accept(new LazyOutput(true, isRemote.get(), Optional.empty(), Optional.empty())));
    }

    @Override
    public Plan getQueryPlan()
    {
        if (delegate.get() == null) {
            throw new UnsupportedOperationException("cannot get query plan while queued");
        }
        return delegate.get().getQueryPlan();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        listenOrFire(
                () -> delegate.get().addStateChangeListener(stateChangeListener),
                () -> stateChangeListener.stateChanged(FAILED));
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        listenOrFire(
                () -> delegate.get().addFinalQueryInfoListener(stateChangeListener),
                () -> stateChangeListener.stateChanged(getQueryInfo()));
    }

    private void listenOrFire(Runnable delegateEvent, Runnable failedEvent)
    {
        AtomicBoolean done = new AtomicBoolean();
        Runnable fireOnceListener = () -> {
            if (delegate.get() != null && done.compareAndSet(false, true)) {
                delegateEvent.run();
            }
        };

        lazyStateMachine.addStateChangeListener(newState -> {
            if (delegate.get() != null) {
                checkState(newState == FINISHED);
                fireOnceListener.run();
                return;
            }
            checkState(newState == FAILED);
            failedEvent.run();
        });
        fireOnceListener.run();
    }

    public void setCoordinator(URI uri)
    {
        coordinatorUri.compareAndSet(null, requireNonNull(uri, "uri is null"));
    }

    @Override
    public void cancelQuery()
    {
        internalFail(new PrestoException(USER_CANCELED, "Query was canceled"));
    }

    @Override
    public synchronized void cancelStage(StageId stageId)
    {
        if (delegate.get() == null) {
            return;
        }
        delegate.get().cancelStage(stageId);
    }

    @Override
    public void fail(Throwable cause)
    {
        internalFail(cause);
    }

    @Override
    public synchronized ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        if (delegate.get() != null) {
            return delegate.get().getStateChange(currentState);
        }
        return Futures.transform(
                lazyStateMachine.getStateChange(currentState),
                state -> {
                    if (state == FINISHED) {
                        // TODO: This is very hacky
                        checkState(delegate.get() != null);
                        if (sqlQueryExecution.get() != null) {
                            return RUNNING;
                        }
                        else {
                            checkState(dispatchQueryExecution.get() != null);
                            return SUBMITTING;
                        }
                    }
                    return state;
                },
                lazyExecutor);
    }

    @Override
    public void recordHeartbeat()
    {
        if (delegate.get() == null) {
            lazyStateMachine.recordHeartbeat();
            return;
        }
        delegate.get().recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        if (delegate.get() == null) {
            return;
        }
        delegate.get().pruneInfo();
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        QueryInfo delegateQueryInfo = null;
        if (delegate.get() != null) {
            delegateQueryInfo = delegate.get().getQueryInfo();
        }

        QueryInfo lazyQueryInfo = new QueryInfo(
                queryId,
                session.toSessionRepresentation(),
                getState(),
                getMemoryPool().getId(),
                false,
                self,
                ImmutableList.of(),
                query,
                lazyStateMachine.getQueryStats(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "", // TODO
                Optional.empty(),
                failureCause.get() == null ? null : failureCause.get().toFailureInfo(),
                failureCause.get() == null ? null : failureCause.get().getErrorCode(),
                ImmutableSet.of(),
                Optional.empty(),
                getState() == FAILED,
                getResourceGroup().map(ResourceGroupId::toString));

        if (delegateQueryInfo == null) {
            return lazyQueryInfo;
        }

        QueryStats lazyQueryStats = lazyQueryInfo.getQueryStats();
        QueryStats delegateQueryStats = delegateQueryInfo.getQueryStats();

        QueryStats queryStats = new QueryStats(
                lazyQueryStats.getCreateTime(),
                delegateQueryStats.getExecutionStartTime(),
                delegateQueryStats.getLastHeartbeat(),
                delegateQueryStats.getEndTime(),
                new Duration(lazyQueryStats.getElapsedTime().toMillis() + delegateQueryStats.getElapsedTime().toMillis(), MILLISECONDS).convertToMostSuccinctTimeUnit(),
                lazyQueryStats.getQueuedTime(),
                delegateQueryStats.getAnalysisTime(),
                delegateQueryStats.getDistributedPlanningTime(),
                delegateQueryStats.getTotalPlanningTime(),
                delegateQueryStats.getFinishingTime(),
                delegateQueryStats.getTotalTasks(),
                delegateQueryStats.getRunningTasks(),
                delegateQueryStats.getCompletedTasks(),
                delegateQueryStats.getTotalDrivers(),
                delegateQueryStats.getQueuedDrivers(),
                delegateQueryStats.getRunningDrivers(),
                delegateQueryStats.getBlockedDrivers(),
                delegateQueryStats.getCompletedDrivers(),
                delegateQueryStats.getCumulativeUserMemory(),
                delegateQueryStats.getUserMemoryReservation(),
                delegateQueryStats.getPeakUserMemoryReservation(),
                delegateQueryStats.getPeakTotalMemoryReservation(),
                delegateQueryStats.getPeakTaskTotalMemory(),
                delegateQueryStats.isScheduled(),
                delegateQueryStats.getTotalScheduledTime(),
                delegateQueryStats.getTotalCpuTime(),
                delegateQueryStats.getTotalUserTime(),
                delegateQueryStats.getTotalBlockedTime(),
                delegateQueryStats.isFullyBlocked(),
                delegateQueryStats.getBlockedReasons(),
                delegateQueryStats.getRawInputDataSize(),
                delegateQueryStats.getRawInputPositions(),
                delegateQueryStats.getProcessedInputDataSize(),
                delegateQueryStats.getRawInputPositions(),
                delegateQueryStats.getOutputDataSize(),
                delegateQueryStats.getOutputPositions(),
                delegateQueryStats.getPhysicalWrittenDataSize(),
                delegateQueryStats.getStageGcStatistics(),
                delegateQueryStats.getOperatorSummaries());

        return new QueryInfo(
                queryId,
                delegateQueryInfo.getSession(),
                delegateQueryInfo.getState(),
                lazyQueryInfo.getMemoryPool(),
                delegateQueryInfo.isScheduled(),
                delegateQueryInfo.getSelf(),
                delegateQueryInfo.getFieldNames(),
                delegateQueryInfo.getQuery(),
                queryStats,
                delegateQueryInfo.getSetCatalog(),
                delegateQueryInfo.getSetSchema(),
                delegateQueryInfo.getSetSessionProperties(),
                delegateQueryInfo.getResetSessionProperties(),
                delegateQueryInfo.getAddedPreparedStatements(),
                delegateQueryInfo.getDeallocatedPreparedStatements(),
                delegateQueryInfo.getStartedTransactionId(),
                delegateQueryInfo.isClearTransactionId(),
                delegateQueryInfo.getUpdateType(),
                delegateQueryInfo.getOutputStage(),
                delegateQueryInfo.getFailureInfo(),
                delegateQueryInfo.getErrorCode(),
                delegateQueryInfo.getInputs(),
                delegateQueryInfo.getOutput(),
                delegateQueryInfo.isCompleteInfo(),
                lazyQueryInfo.getResourceGroupName());
    }

    @Override
    public QueryState getState()
    {
        if (delegate.get() == null) {
            QueryState queryState = lazyStateMachine.getState();
            if (queryState == FINISHED) {
                // this can happen due to race
                return QUEUED;
            }
            return queryState;
        }
        return delegate.get().getState();
    }

    @Override
    public Optional<ResourceGroupId> getResourceGroup()
    {
        return Optional.ofNullable(resourceGroupId.get());
    }

    @Override
    public void setResourceGroup(ResourceGroupId resourceGroupId)
    {
        this.resourceGroupId.set(resourceGroupId);
        synchronized (this) {
            if (delegate.get() != null) {
                delegate.get().setResourceGroup(resourceGroupId);
            }
        }
    }

    private void internalFail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");
        if (!failureCause.compareAndSet(null, toFailure(cause))) {
            return;
        }

        if (!lazyStateMachine.transitionToFailed()) {
            checkState(lazyStateMachine.getState() == FINISHED);
            checkState(delegate.get() != null);
            delegate.get().fail(cause);
        }
    }

    @ThreadSafe
    private static class LazyStateMachine
    {
        private final Ticker ticker;
        private final StateMachine<QueryState> stateMachine;

        // stats
        private final long createNanos;
        private final DateTime createTime = DateTime.now();
        private final AtomicLong endNanos = new AtomicLong();
        private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
        private final AtomicReference<DateTime> delegateTime = new AtomicReference<>();
        private final AtomicReference<Duration> queuedTime = new AtomicReference<>();
        private final AtomicReference<DateTime> endTime = new AtomicReference<>();

        LazyStateMachine(QueryId queryId, Executor lazyExecutor, Ticker ticker)
        {
            this.ticker = requireNonNull(ticker, "ticker is null");
            this.stateMachine = new StateMachine<>("query " + queryId, lazyExecutor, QUEUED, TERMINAL_QUERY_STATES);
            this.createNanos = tickerNanos();
        }

        boolean transitionToFinished()
        {
            recordEnd();
            return stateMachine.compareAndSet(QUEUED, FINISHED);
        }

        boolean transitionToFailed()
        {
            recordEnd();
            return stateMachine.compareAndSet(QUEUED, FAILED);
        }

        QueryState getState()
        {
            return stateMachine.get();
        }

        void recordHeartbeat()
        {
            lastHeartbeat.set(DateTime.now());
        }

        void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
        {
            stateMachine.addStateChangeListener(stateChangeListener);
        }

        ListenableFuture<QueryState> getStateChange(QueryState currentState)
        {
            return stateMachine.getStateChange(currentState);
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
                    delegateTime.get(),
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
            delegateTime.compareAndSet(null, DateTime.now());
            queuedTime.compareAndSet(null, succinctNanos(tickerNanos() - createNanos).convertToMostSuccinctTimeUnit());
            endTime.compareAndSet(null, DateTime.now());
            endNanos.compareAndSet(0, tickerNanos());
        }

        private long tickerNanos()
        {
            return ticker.read();
        }
    }

    public static class LazyQueryExecutionFactory
            implements QueryExecutionFactory<LazyQueryExecution>
    {
        private final LocationFactory locationFactory;
        private final ExecutorService lazyExecutor;
        private final Map<Class<? extends Statement>, QueryExecutionFactory<? extends QueryExecution<QueryOutputInfo>>> sqlQueryExecutionFactories;
        private final DispatchQueryExecutionFactory dispatchQueryExecutionFactory;

        @Inject
        public LazyQueryExecutionFactory(
                LocationFactory locationFactory,
                @ForQueryExecution ExecutorService lazyExecutor,
                Map<Class<? extends Statement>, QueryExecutionFactory<? extends QueryExecution<QueryOutputInfo>>> sqlQueryExecutionFactories,
                DispatchQueryExecutionFactory dispatchQueryExecutionFactory)
        {
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.lazyExecutor = requireNonNull(lazyExecutor, "lazyExecutor is null");
            this.sqlQueryExecutionFactories = requireNonNull(sqlQueryExecutionFactories, "sqlQueryExecutionFactories is null");
            this.dispatchQueryExecutionFactory = requireNonNull(dispatchQueryExecutionFactory, "dispatchQueryExecutionFactory is null");
        }

        @Override
        public LazyQueryExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement, List<Expression> parameters)
        {
            QueryExecutionFactory<? extends QueryExecution<QueryOutputInfo>> sqlQueryExecutionFactory = sqlQueryExecutionFactories.get(statement.getClass());
            if (sqlQueryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + statement.getClass().getSimpleName());
            }
            return new LazyQueryExecution(
                    locationFactory.createQueryLocation(queryId),
                    queryId,
                    query,
                    session,
                    statement,
                    parameters,
                    lazyExecutor,
                    sqlQueryExecutionFactory,
                    dispatchQueryExecutionFactory);
        }
    }
}
