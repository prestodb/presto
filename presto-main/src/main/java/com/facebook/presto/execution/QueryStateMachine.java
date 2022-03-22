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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.QueryExecution.QueryOutputInfo;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.facebook.presto.execution.BasicStageExecutionStats.EMPTY_STAGE_STATS;
import static com.facebook.presto.execution.QueryState.DISPATCHING;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.FINISHING;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.STARTING;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_PREREQUISITES;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_RESOURCES;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryStateMachine
{
    private static final Logger QUERY_STATE_LOG = Logger.get(QueryStateMachine.class);

    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final URI self;
    private final Optional<QueryType> queryType;
    private final ResourceGroupId resourceGroup;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final QueryOutputManager outputManager;

    private final AtomicReference<VersionedMemoryPoolId> memoryPool = new AtomicReference<>(new VersionedMemoryPoolId(GENERAL_POOL, 0));

    private final AtomicLong currentUserMemory = new AtomicLong();
    private final AtomicLong peakUserMemory = new AtomicLong();

    // peak of the user + system memory reservation
    private final AtomicLong currentTotalMemory = new AtomicLong();
    private final AtomicLong peakTotalMemory = new AtomicLong();

    private final AtomicLong peakTaskUserMemory = new AtomicLong();
    private final AtomicLong peakTaskTotalMemory = new AtomicLong();
    private final AtomicLong peakNodeTotalMemory = new AtomicLong();

    private final AtomicInteger currentRunningTaskCount = new AtomicInteger();
    private final AtomicInteger peakRunningTaskCount = new AtomicInteger();

    private final QueryStateTimer queryStateTimer;

    private final StateMachine<QueryState> queryState;

    private final AtomicReference<String> setCatalog = new AtomicReference<>();
    private final AtomicReference<String> setSchema = new AtomicReference<>();

    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();

    private final Map<String, SelectedRole> setRoles = new ConcurrentHashMap<>();

    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();

    private final AtomicReference<TransactionId> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();

    private final AtomicReference<String> updateType = new AtomicReference<>();

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<Set<Input>> inputs = new AtomicReference<>(ImmutableSet.of());
    private final AtomicReference<Optional<Output>> output = new AtomicReference<>(Optional.empty());
    private final StateMachine<Optional<QueryInfo>> finalQueryInfo;
    private final AtomicReference<Optional<String>> expandedQuery = new AtomicReference<>(Optional.empty());

    private final Map<SqlFunctionId, SqlInvokedFunction> addedSessionFunctions = new ConcurrentHashMap<>();
    private final Set<SqlFunctionId> removedSessionFunctions = Sets.newConcurrentHashSet();

    private final WarningCollector warningCollector;

    private QueryStateMachine(
            String query,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            Optional<QueryType> queryType,
            TransactionManager transactionManager,
            Executor executor,
            Ticker ticker,
            Metadata metadata,
            WarningCollector warningCollector)
    {
        this.query = requireNonNull(query, "query is null");
        this.session = requireNonNull(session, "session is null");
        this.queryId = session.getQueryId();
        this.self = requireNonNull(self, "self is null");
        this.resourceGroup = requireNonNull(resourceGroup, "resourceGroup is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.queryStateTimer = new QueryStateTimer(ticker);
        this.metadata = requireNonNull(metadata, "metadata is null");

        this.queryState = new StateMachine<>("query " + query, executor, WAITING_FOR_PREREQUISITES, TERMINAL_QUERY_STATES);
        this.finalQueryInfo = new StateMachine<>("finalQueryInfo-" + queryId, executor, Optional.empty());
        this.outputManager = new QueryOutputManager(executor);
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    /**
     * Created QueryStateMachines must be transitioned to terminal states to clean up resources.
     */
    public static QueryStateMachine begin(
            String query,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            Optional<QueryType> queryType,
            boolean transactionControl,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Executor executor,
            Metadata metadata,
            WarningCollector warningCollector)
    {
        return beginWithTicker(
                query,
                session,
                self,
                resourceGroup,
                queryType,
                transactionControl,
                transactionManager,
                accessControl,
                executor,
                Ticker.systemTicker(),
                metadata,
                warningCollector);
    }

    static QueryStateMachine beginWithTicker(
            String query,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            Optional<QueryType> queryType,
            boolean transactionControl,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Executor executor,
            Ticker ticker,
            Metadata metadata,
            WarningCollector warningCollector)
    {
        // If there is not an existing transaction, begin an auto commit transaction
        if (!session.getTransactionId().isPresent() && !transactionControl) {
            // TODO: make autocommit isolation level a session parameter
            TransactionId transactionId = transactionManager.beginTransaction(true);
            session = session.beginTransactionId(transactionId, transactionManager, accessControl);
        }

        QueryStateMachine queryStateMachine = new QueryStateMachine(
                query,
                session,
                self,
                resourceGroup,
                queryType,
                transactionManager,
                executor,
                ticker,
                metadata,
                warningCollector);

        queryStateMachine.addStateChangeListener(newState -> {
            QUERY_STATE_LOG.debug("Query %s is %s", queryStateMachine.getQueryId(), newState);
            // mark finished or failed transaction as inactive
            if (newState.isDone()) {
                queryStateMachine.getSession().getTransactionId().ifPresent(transactionManager::trySetInactive);
            }
        });
        return queryStateMachine;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public Session getSession()
    {
        return session;
    }

    public long getPeakUserMemoryInBytes()
    {
        return peakUserMemory.get();
    }

    public long getPeakTotalMemoryInBytes()
    {
        return peakTotalMemory.get();
    }

    public long getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory.get();
    }

    public long getPeakTaskUserMemory()
    {
        return peakTaskUserMemory.get();
    }

    public long getPeakNodeTotalMemory()
    {
        return peakNodeTotalMemory.get();
    }

    public int getCurrentRunningTaskCount()
    {
        return currentRunningTaskCount.get();
    }

    public int incrementCurrentRunningTaskCount()
    {
        int runningTaskCount = currentRunningTaskCount.incrementAndGet();
        peakRunningTaskCount.accumulateAndGet(runningTaskCount, Math::max);
        return runningTaskCount;
    }

    public int decrementCurrentRunningTaskCount()
    {
        return currentRunningTaskCount.decrementAndGet();
    }

    public int getPeakRunningTaskCount()
    {
        return peakRunningTaskCount.get();
    }

    public WarningCollector getWarningCollector()
    {
        return warningCollector;
    }

    public void updateMemoryUsage(
            long deltaUserMemoryInBytes,
            long deltaTotalMemoryInBytes,
            long taskUserMemoryInBytes,
            long taskTotalMemoryInBytes,
            long peakNodeTotalMemoryInBytes)
    {
        currentUserMemory.addAndGet(deltaUserMemoryInBytes);
        currentTotalMemory.addAndGet(deltaTotalMemoryInBytes);
        peakUserMemory.updateAndGet(currentPeakValue -> Math.max(currentUserMemory.get(), currentPeakValue));
        peakTotalMemory.updateAndGet(currentPeakValue -> Math.max(currentTotalMemory.get(), currentPeakValue));
        peakTaskUserMemory.accumulateAndGet(taskUserMemoryInBytes, Math::max);
        peakTaskTotalMemory.accumulateAndGet(taskTotalMemoryInBytes, Math::max);
        peakNodeTotalMemory.accumulateAndGet(peakNodeTotalMemoryInBytes, Math::max);
    }

    public BasicQueryInfo getBasicQueryInfo(Optional<BasicStageExecutionStats> rootStage)
    {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        BasicStageExecutionStats stageStats = rootStage.orElse(EMPTY_STAGE_STATS);
        BasicQueryStats queryStats = new BasicQueryStats(
                queryStateTimer.getCreateTime(),
                getEndTime().orElse(null),
                queryStateTimer.getWaitingForPrerequisitesTime(),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getElapsedTime(),
                queryStateTimer.getExecutionTime(),

                getCurrentRunningTaskCount(),
                getPeakRunningTaskCount(),

                stageStats.getTotalDrivers(),
                stageStats.getQueuedDrivers(),
                stageStats.getRunningDrivers(),
                stageStats.getCompletedDrivers(),

                stageStats.getRawInputDataSize(),
                stageStats.getRawInputPositions(),

                stageStats.getCumulativeUserMemory(),
                stageStats.getCumulativeTotalMemory(),
                stageStats.getUserMemoryReservation(),
                stageStats.getTotalMemoryReservation(),
                succinctBytes(getPeakUserMemoryInBytes()),
                succinctBytes(getPeakTotalMemoryInBytes()),
                succinctBytes(getPeakTaskTotalMemory()),
                succinctBytes(getPeakNodeTotalMemory()),

                stageStats.getTotalCpuTime(),
                stageStats.getTotalScheduledTime(),

                stageStats.isFullyBlocked(),
                stageStats.getBlockedReasons(),

                stageStats.getTotalAllocation(),

                stageStats.getProgressPercentage());

        return new BasicQueryInfo(
                queryId,
                session.toSessionRepresentation(),
                Optional.of(resourceGroup),
                state,
                memoryPool.get().getId(),
                stageStats.isScheduled(),
                self,
                query,
                queryStats,
                failureCause.get(),
                queryType,
                warningCollector.getWarnings());
    }

    public QueryInfo getQueryInfo(Optional<StageInfo> rootStage)
    {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        ExecutionFailureInfo failureCause = null;
        ErrorCode errorCode = null;
        if (state == QueryState.FAILED) {
            failureCause = this.failureCause.get();
            if (failureCause != null) {
                errorCode = failureCause.getErrorCode();
            }
        }

        boolean completeInfo = getAllStages(rootStage).stream().allMatch(StageInfo::isFinalStageInfo);
        Optional<List<TaskId>> failedTasks;
        // Traversing all tasks is expensive, thus only construct failedTasks list when query finished.
        if (state.isDone()) {
            failedTasks = Optional.of(getAllStages(rootStage).stream()
                    .flatMap(stageInfo -> Streams.concat(ImmutableList.of(stageInfo.getLatestAttemptExecutionInfo()).stream(), stageInfo.getPreviousAttemptsExecutionInfos().stream()))
                    .flatMap(execution -> execution.getTasks().stream())
                    .filter(taskInfo -> taskInfo.getTaskStatus().getState() == TaskState.FAILED)
                    .map(TaskInfo::getTaskId)
                    .collect(toImmutableList()));
        }
        else {
            failedTasks = Optional.empty();
        }

        List<StageId> runtimeOptimizedStages = getAllStages(rootStage).stream().filter(StageInfo::isRuntimeOptimized).map(StageInfo::getStageId).collect(toImmutableList());
        QueryStats queryStats = getQueryStats(rootStage);
        return new QueryInfo(
                queryId,
                session.toSessionRepresentation(),
                state,
                memoryPool.get().getId(),
                queryStats.isScheduled(),
                self,
                outputManager.getQueryOutputInfo().map(QueryOutputInfo::getColumnNames).orElse(ImmutableList.of()),
                query,
                expandedQuery.get(),
                queryStats,
                Optional.ofNullable(setCatalog.get()),
                Optional.ofNullable(setSchema.get()),
                setSessionProperties,
                resetSessionProperties,
                setRoles,
                addedPreparedStatements,
                deallocatedPreparedStatements,
                Optional.ofNullable(startedTransactionId.get()),
                clearTransactionId.get(),
                updateType.get(),
                rootStage,
                failureCause,
                errorCode,
                warningCollector.getWarnings(),
                inputs.get(),
                output.get(),
                completeInfo,
                Optional.of(resourceGroup),
                queryType,
                failedTasks,
                runtimeOptimizedStages.isEmpty() ? Optional.empty() : Optional.of(runtimeOptimizedStages),
                addedSessionFunctions,
                removedSessionFunctions);
    }

    private QueryStats getQueryStats(Optional<StageInfo> rootStage)
    {
        return QueryStats.create(
                queryStateTimer,
                rootStage,
                getPeakRunningTaskCount(),
                succinctBytes(getPeakUserMemoryInBytes()),
                succinctBytes(getPeakTotalMemoryInBytes()),
                succinctBytes(getPeakTaskUserMemory()),
                succinctBytes(getPeakTaskTotalMemory()),
                succinctBytes(getPeakNodeTotalMemory()),
                session.getRuntimeStats());
    }

    public VersionedMemoryPoolId getMemoryPool()
    {
        return memoryPool.get();
    }

    public void setMemoryPool(VersionedMemoryPoolId memoryPool)
    {
        this.memoryPool.set(requireNonNull(memoryPool, "memoryPool is null"));
    }

    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        outputManager.addOutputInfoListener(listener);
    }

    public void setColumns(List<String> columnNames, List<Type> columnTypes)
    {
        outputManager.setColumns(columnNames, columnTypes);
    }

    public void updateOutputLocations(Map<URI, TaskId> newExchangeLocations, boolean noMoreExchangeLocations)
    {
        outputManager.updateOutputLocations(newExchangeLocations, noMoreExchangeLocations);
    }

    public void setInputs(List<Input> inputs)
    {
        requireNonNull(inputs, "inputs is null");
        this.inputs.set(ImmutableSet.copyOf(inputs));
    }

    public void setOutput(Optional<Output> output)
    {
        requireNonNull(output, "output is null");
        this.output.set(output);
    }

    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    public void setSetCatalog(String catalog)
    {
        setCatalog.set(requireNonNull(catalog, "catalog is null"));
    }

    public void setSetSchema(String schema)
    {
        setSchema.set(requireNonNull(schema, "schema is null"));
    }

    public void addSetSessionProperties(String key, String value)
    {
        setSessionProperties.put(requireNonNull(key, "key is null"), requireNonNull(value, "value is null"));
    }

    public void addSetRole(String catalog, SelectedRole role)
    {
        setRoles.put(requireNonNull(catalog, "catalog is null"), requireNonNull(role, "role is null"));
    }

    public Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    public void addResetSessionProperties(String name)
    {
        resetSessionProperties.add(requireNonNull(name, "name is null"));
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    public Map<SqlFunctionId, SqlInvokedFunction> getAddedSessionFunctions()
    {
        return addedSessionFunctions;
    }

    public Set<SqlFunctionId> getRemovedSessionFunctions()
    {
        return removedSessionFunctions;
    }

    public void addPreparedStatement(String key, String value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        addedPreparedStatements.put(key, value);
    }

    public void removePreparedStatement(String key)
    {
        requireNonNull(key, "key is null");

        if (!session.getPreparedStatements().containsKey(key)) {
            throw new PrestoException(NOT_FOUND, "Prepared statement not found: " + key);
        }
        deallocatedPreparedStatements.add(key);
    }

    public void addSessionFunction(SqlFunctionId signature, SqlInvokedFunction function)
    {
        requireNonNull(signature, "signature is null");
        requireNonNull(function, "function is null");

        if (session.getSessionFunctions().containsKey(signature) || addedSessionFunctions.putIfAbsent(signature, function) != null) {
            throw new PrestoException(ALREADY_EXISTS, format("Session function %s has already been defined", signature));
        }
    }

    public void removeSessionFunction(SqlFunctionId signature, boolean suppressNotFoundException)
    {
        requireNonNull(signature, "signature is null");

        if (!session.getSessionFunctions().containsKey(signature)) {
            if (!suppressNotFoundException) {
                throw new PrestoException(NOT_FOUND, format("Session function %s not found", signature.getFunctionName()));
            }
        }
        else {
            removedSessionFunctions.add(signature);
        }
    }

    public void setStartedTransactionId(TransactionId startedTransactionId)
    {
        checkArgument(!clearTransactionId.get(), "Cannot start and clear transaction ID in the same request");
        this.startedTransactionId.set(startedTransactionId);
    }

    public void clearTransactionId()
    {
        checkArgument(startedTransactionId.get() == null, "Cannot start and clear transaction ID in the same request");
        clearTransactionId.set(true);
    }

    public void setUpdateType(String updateType)
    {
        this.updateType.set(updateType);
    }

    public void setExpandedQuery(Optional<String> expandedQuery)
    {
        this.expandedQuery.set(expandedQuery);
    }

    public QueryState getQueryState()
    {
        return queryState.get();
    }

    public boolean isDone()
    {
        return queryState.get().isDone();
    }

    public boolean transitionToQueued()
    {
        queryStateTimer.beginQueued();
        return queryState.setIf(QUEUED, currentState -> currentState.ordinal() < QUEUED.ordinal());
    }

    public boolean transitionToWaitingForResources()
    {
        queryStateTimer.beginWaitingForResources();
        return queryState.setIf(WAITING_FOR_RESOURCES, currentState -> currentState.ordinal() < WAITING_FOR_RESOURCES.ordinal());
    }

    public void beginSemanticAnalyzing()
    {
        queryStateTimer.beginSemanticAnalyzing();
    }

    public void beginColumnAccessPermissionChecking()
    {
        queryStateTimer.beginColumnAccessPermissionChecking();
    }

    public void endColumnAccessPermissionChecking()
    {
        queryStateTimer.endColumnAccessPermissionChecking();
    }

    public boolean transitionToDispatching()
    {
        queryStateTimer.beginDispatching();
        return queryState.setIf(DISPATCHING, currentState -> currentState.ordinal() < DISPATCHING.ordinal());
    }

    public boolean transitionToPlanning()
    {
        queryStateTimer.beginPlanning();
        return queryState.setIf(PLANNING, currentState -> currentState.ordinal() < PLANNING.ordinal());
    }

    public boolean transitionToStarting()
    {
        queryStateTimer.beginStarting();
        return queryState.setIf(STARTING, currentState -> currentState.ordinal() < STARTING.ordinal());
    }

    public boolean transitionToRunning()
    {
        queryStateTimer.beginRunning();
        return queryState.setIf(RUNNING, currentState -> currentState.ordinal() < RUNNING.ordinal());
    }

    public boolean transitionToFinishing()
    {
        queryStateTimer.beginFinishing();

        if (!queryState.setIf(FINISHING, currentState -> currentState != FINISHING && !currentState.isDone())) {
            return false;
        }

        Optional<TransactionInfo> transaction = session.getTransactionId()
                .flatMap(transactionManager::getOptionalTransactionInfo);
        if (transaction.isPresent() && transaction.get().isAutoCommitContext()) {
            ListenableFuture<?> commitFuture = transactionManager.asyncCommit(transaction.get().getTransactionId());
            Futures.addCallback(commitFuture, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(@Nullable Object result)
                {
                    transitionToFinished();
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    transitionToFailed(throwable, currentState -> !currentState.isDone());
                }
            }, directExecutor());
        }
        else {
            transitionToFinished();
        }
        return true;
    }

    private void transitionToFinished()
    {
        cleanupQueryQuietly();
        queryStateTimer.endQuery();

        queryState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        // When the state enters FINISHING, the only thing remaining is to commit
        // the transaction. It should only be failed if the transaction commit fails.
        return transitionToFailed(throwable, currentState -> currentState != FINISHING && !currentState.isDone());
    }

    private boolean transitionToFailed(Throwable throwable, Predicate<QueryState> predicate)
    {
        QueryState currentState = queryState.get();
        if (!predicate.test(currentState)) {
            QUERY_STATE_LOG.debug(throwable, "Failure is ignored as the query %s is the %s state, ", queryId, currentState);
            return false;
        }

        cleanupQueryQuietly();
        queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        requireNonNull(throwable, "throwable is null");
        failureCause.compareAndSet(null, toFailure(throwable));

        boolean failed = queryState.setIf(QueryState.FAILED, predicate);
        if (failed) {
            QUERY_STATE_LOG.debug(throwable, "Query %s failed", queryId);
            // if the transaction is already gone, do nothing
            session.getTransactionId().flatMap(transactionManager::getOptionalTransactionInfo).ifPresent(transaction -> {
                if (transaction.isAutoCommitContext()) {
                    transactionManager.asyncAbort(transaction.getTransactionId());
                }
                else {
                    transactionManager.fail(transaction.getTransactionId());
                }
            });
        }
        else {
            QUERY_STATE_LOG.debug(throwable, "Failure after query %s finished", queryId);
        }

        return failed;
    }

    public boolean transitionToCanceled()
    {
        cleanupQueryQuietly();
        queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        failureCause.compareAndSet(null, toFailure(new PrestoException(USER_CANCELED, "Query was canceled")));

        boolean canceled = queryState.setIf(QueryState.FAILED, currentState -> !currentState.isDone());
        if (canceled) {
            // if the transaction is already gone, do nothing
            session.getTransactionId().flatMap(transactionManager::getOptionalTransactionInfo).ifPresent(transaction -> {
                if (transaction.isAutoCommitContext()) {
                    transactionManager.asyncAbort(transaction.getTransactionId());
                }
                else {
                    transactionManager.fail(transaction.getTransactionId());
                }
            });
        }

        return canceled;
    }

    private void cleanupQueryQuietly()
    {
        try {
            metadata.cleanupQuery(session);
        }
        catch (Throwable t) {
            QUERY_STATE_LOG.error("Error cleaning up query: %s", t);
        }
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    /**
     * Add a listener for the final query info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    public void addQueryInfoStateChangeListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<QueryInfo>> fireOnceStateChangeListener = finalQueryInfo -> {
            if (finalQueryInfo.isPresent() && done.compareAndSet(false, true)) {
                stateChangeListener.stateChanged(finalQueryInfo.get());
            }
        };
        finalQueryInfo.addStateChangeListener(fireOnceStateChangeListener);
    }

    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return queryState.getStateChange(currentState);
    }

    public void recordHeartbeat()
    {
        queryStateTimer.recordHeartbeat();
    }

    public void beginAnalysis()
    {
        queryStateTimer.beginAnalyzing();
    }

    public void endAnalysis()
    {
        queryStateTimer.endAnalysis();
    }

    public DateTime getCreateTime()
    {
        return queryStateTimer.getCreateTime();
    }

    public Optional<DateTime> getExecutionStartTime()
    {
        return queryStateTimer.getExecutionStartTime();
    }

    public DateTime getLastHeartbeat()
    {
        return queryStateTimer.getLastHeartbeat();
    }

    public Optional<DateTime> getEndTime()
    {
        return queryStateTimer.getEndTime();
    }

    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        if (queryState.get() != QueryState.FAILED) {
            return Optional.empty();
        }
        return Optional.ofNullable(this.failureCause.get());
    }

    public Optional<QueryInfo> getFinalQueryInfo()
    {
        return finalQueryInfo.get();
    }

    public QueryInfo updateQueryInfo(Optional<StageInfo> stageInfo)
    {
        QueryInfo queryInfo = getQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            finalQueryInfo.compareAndSet(Optional.empty(), Optional.of(queryInfo));
        }
        return queryInfo;
    }

    public void pruneQueryInfo()
    {
        Optional<QueryInfo> finalInfo = finalQueryInfo.get();
        if (!finalInfo.isPresent() || !finalInfo.get().getOutputStage().isPresent()) {
            return;
        }

        QueryInfo queryInfo = finalInfo.get();
        Optional<StageInfo> prunedOutputStage = queryInfo.getOutputStage().map(outputStage -> new StageInfo(
                outputStage.getStageId(),
                outputStage.getSelf(),
                Optional.empty(), // Remove the plan
                pruneStageExecutionInfo(outputStage.getLatestAttemptExecutionInfo()),
                ImmutableList.of(), // Remove failed attempts
                ImmutableList.of(),
                outputStage.isRuntimeOptimized())); // Remove the substages

        QueryInfo prunedQueryInfo = new QueryInfo(
                queryInfo.getQueryId(),
                queryInfo.getSession(),
                queryInfo.getState(),
                getMemoryPool().getId(),
                queryInfo.isScheduled(),
                queryInfo.getSelf(),
                queryInfo.getFieldNames(),
                queryInfo.getQuery(),
                queryInfo.getExpandedQuery(),
                pruneQueryStats(queryInfo.getQueryStats()),
                queryInfo.getSetCatalog(),
                queryInfo.getSetSchema(),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                queryInfo.getSetRoles(),
                queryInfo.getAddedPreparedStatements(),
                queryInfo.getDeallocatedPreparedStatements(),
                queryInfo.getStartedTransactionId(),
                queryInfo.isClearTransactionId(),
                queryInfo.getUpdateType(),
                prunedOutputStage,
                queryInfo.getFailureInfo(),
                queryInfo.getErrorCode(),
                queryInfo.getWarnings(),
                queryInfo.getInputs(),
                queryInfo.getOutput(),
                queryInfo.isCompleteInfo(),
                queryInfo.getResourceGroupId(),
                queryInfo.getQueryType(),
                queryInfo.getFailedTasks(),
                queryInfo.getRuntimeOptimizedStages(),
                queryInfo.getAddedSessionFunctions(),
                queryInfo.getRemovedSessionFunctions());
        finalQueryInfo.compareAndSet(finalInfo, Optional.of(prunedQueryInfo));
    }

    private static StageExecutionInfo pruneStageExecutionInfo(StageExecutionInfo info)
    {
        return new StageExecutionInfo(
                info.getState(),
                info.getStats(),
                // Remove the tasks
                ImmutableList.of(),
                info.getFailureCause());
    }

    private static QueryStats pruneQueryStats(QueryStats queryStats)
    {
        return new QueryStats(
                queryStats.getCreateTime(),
                queryStats.getExecutionStartTime(),
                queryStats.getLastHeartbeat(),
                queryStats.getEndTime(),
                queryStats.getElapsedTime(),
                queryStats.getWaitingForPrerequisitesTime(),
                queryStats.getQueuedTime(),
                queryStats.getResourceWaitingTime(),
                queryStats.getSemanticAnalyzingTime(),
                queryStats.getColumnAccessPermissionCheckingTime(),
                queryStats.getDispatchingTime(),
                queryStats.getExecutionTime(),
                queryStats.getAnalysisTime(),
                queryStats.getTotalPlanningTime(),
                queryStats.getFinishingTime(),
                queryStats.getTotalTasks(),
                queryStats.getRunningTasks(),
                queryStats.getCompletedTasks(),
                queryStats.getPeakRunningTasks(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getBlockedDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getCumulativeTotalMemory(),
                queryStats.getUserMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getPeakTaskUserMemory(),
                queryStats.getPeakTaskTotalMemory(),
                queryStats.getPeakNodeTotalMemory(),
                queryStats.isScheduled(),
                queryStats.getTotalScheduledTime(),
                queryStats.getTotalCpuTime(),
                queryStats.getRetriedCpuTime(),
                queryStats.getTotalBlockedTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
                queryStats.getTotalAllocation(),
                queryStats.getRawInputDataSize(),
                queryStats.getRawInputPositions(),
                queryStats.getProcessedInputDataSize(),
                queryStats.getProcessedInputPositions(),
                queryStats.getOutputDataSize(),
                queryStats.getOutputPositions(),
                queryStats.getWrittenOutputPositions(),
                queryStats.getWrittenOutputLogicalDataSize(),
                queryStats.getWrittenOutputPhysicalDataSize(),
                queryStats.getWrittenIntermediatePhysicalDataSize(),
                queryStats.getStageGcStatistics(),
                ImmutableList.of(), // Remove the operator summaries as OperatorInfo (especially ExchangeClientStatus) can hold onto a large amount of memory
                queryStats.getRuntimeStats());
    }

    public static class QueryOutputManager
    {
        private final Executor executor;

        @GuardedBy("this")
        private final List<Consumer<QueryOutputInfo>> outputInfoListeners = new ArrayList<>();

        @GuardedBy("this")
        private List<String> columnNames;
        @GuardedBy("this")
        private List<Type> columnTypes;
        @GuardedBy("this")
        private final Map<URI, TaskId> exchangeLocations = new LinkedHashMap<>();
        @GuardedBy("this")
        private boolean noMoreExchangeLocations;

        public QueryOutputManager(Executor executor)
        {
            this.executor = requireNonNull(executor, "executor is null");
        }

        public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
        {
            requireNonNull(listener, "listener is null");

            Optional<QueryOutputInfo> queryOutputInfo;
            synchronized (this) {
                outputInfoListeners.add(listener);
                queryOutputInfo = getQueryOutputInfo();
            }
            queryOutputInfo.ifPresent(info -> executor.execute(() -> listener.accept(info)));
        }

        public void setColumns(List<String> columnNames, List<Type> columnTypes)
        {
            requireNonNull(columnNames, "columnNames is null");
            requireNonNull(columnTypes, "columnTypes is null");
            checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes must be the same size");

            Optional<QueryOutputInfo> queryOutputInfo;
            List<Consumer<QueryOutputInfo>> outputInfoListeners;
            synchronized (this) {
                checkState(this.columnNames == null && this.columnTypes == null, "output fields already set");
                this.columnNames = ImmutableList.copyOf(columnNames);
                this.columnTypes = ImmutableList.copyOf(columnTypes);

                queryOutputInfo = getQueryOutputInfo();
                outputInfoListeners = ImmutableList.copyOf(this.outputInfoListeners);
            }
            queryOutputInfo.ifPresent(info -> fireStateChanged(info, outputInfoListeners));
        }

        public void updateOutputLocations(Map<URI, TaskId> newExchangeLocations, boolean noMoreExchangeLocations)
        {
            requireNonNull(newExchangeLocations, "newExchangeLocations is null");

            Optional<QueryOutputInfo> queryOutputInfo;
            List<Consumer<QueryOutputInfo>> outputInfoListeners;
            synchronized (this) {
                if (this.noMoreExchangeLocations) {
                    checkArgument(this.exchangeLocations.keySet().containsAll(newExchangeLocations.keySet()), "New locations added after no more locations set");
                    return;
                }

                this.exchangeLocations.putAll(newExchangeLocations);
                this.noMoreExchangeLocations = noMoreExchangeLocations;
                queryOutputInfo = getQueryOutputInfo();
                outputInfoListeners = ImmutableList.copyOf(this.outputInfoListeners);
            }
            queryOutputInfo.ifPresent(info -> fireStateChanged(info, outputInfoListeners));
        }

        private synchronized Optional<QueryOutputInfo> getQueryOutputInfo()
        {
            if (columnNames == null || columnTypes == null) {
                return Optional.empty();
            }
            return Optional.of(new QueryOutputInfo(columnNames, columnTypes, exchangeLocations, noMoreExchangeLocations));
        }

        private void fireStateChanged(QueryOutputInfo queryOutputInfo, List<Consumer<QueryOutputInfo>> outputInfoListeners)
        {
            for (Consumer<QueryOutputInfo> outputInfoListener : outputInfoListeners) {
                executor.execute(() -> outputInfoListener.accept(queryOutputInfo));
            }
        }
    }
}
