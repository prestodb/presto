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
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.QueryExecution.QueryOutputInfo;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.FINISHING;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.STARTING;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_RESOURCES;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class QueryStateMachine
{
    private static final Logger log = Logger.get(QueryStateMachine.class);

    private final DateTime createTime = DateTime.now();
    private final long createNanos;
    private final AtomicLong endNanos = new AtomicLong();

    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final URI self;
    private final boolean autoCommit;
    private final TransactionManager transactionManager;
    private final Ticker ticker;
    private final Metadata metadata;
    private final QueryOutputManager outputManager;

    private final AtomicReference<VersionedMemoryPoolId> memoryPool = new AtomicReference<>(new VersionedMemoryPoolId(GENERAL_POOL, 0));

    private final AtomicLong currentUserMemory = new AtomicLong();
    private final AtomicLong peakUserMemory = new AtomicLong();

    // peak of the user + system memory reservation
    private final AtomicLong currentTotalMemory = new AtomicLong();
    private final AtomicLong peakTotalMemory = new AtomicLong();

    private final AtomicLong peakTaskTotalMemory = new AtomicLong();

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> endTime = new AtomicReference<>();

    private final AtomicReference<Duration> queuedTime = new AtomicReference<>();
    private final AtomicReference<Duration> analysisTime = new AtomicReference<>();
    private final AtomicReference<Duration> distributedPlanningTime = new AtomicReference<>();

    private final AtomicReference<Long> finishingStartNanos = new AtomicReference<>();
    private final AtomicReference<Duration> finishingTime = new AtomicReference<>();

    private final AtomicReference<Long> totalPlanningStartNanos = new AtomicReference<>();
    private final AtomicReference<Duration> totalPlanningTime = new AtomicReference<>();

    private final AtomicReference<Long> resourceWaitingStartNanos = new AtomicReference<>();
    private final AtomicReference<Duration> resourceWaitingTime = new AtomicReference<>();

    private final StateMachine<QueryState> queryState;

    private final AtomicReference<String> setCatalog = new AtomicReference<>();
    private final AtomicReference<String> setSchema = new AtomicReference<>();
    private final AtomicReference<String> setPath = new AtomicReference<>();

    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();

    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();

    private final AtomicReference<TransactionId> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();

    private final AtomicReference<String> updateType = new AtomicReference<>();

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<Set<Input>> inputs = new AtomicReference<>(ImmutableSet.of());
    private final AtomicReference<Optional<Output>> output = new AtomicReference<>(Optional.empty());
    private final StateMachine<Optional<QueryInfo>> finalQueryInfo;

    private final AtomicReference<ResourceGroupId> resourceGroup = new AtomicReference<>();

    private QueryStateMachine(QueryId queryId, String query, Session session, URI self, boolean autoCommit, TransactionManager transactionManager, Executor executor, Ticker ticker, Metadata metadata)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.query = requireNonNull(query, "query is null");
        this.session = requireNonNull(session, "session is null");
        this.self = requireNonNull(self, "self is null");
        this.autoCommit = autoCommit;
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.ticker = ticker;
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.createNanos = tickerNanos();

        this.queryState = new StateMachine<>("query " + query, executor, QUEUED, TERMINAL_QUERY_STATES);
        this.finalQueryInfo = new StateMachine<>("finalQueryInfo-" + queryId, executor, Optional.empty());
        this.outputManager = new QueryOutputManager(executor);
    }

    /**
     * Created QueryStateMachines must be transitioned to terminal states to clean up resources.
     */
    public static QueryStateMachine begin(
            QueryId queryId,
            String query,
            Session session,
            URI self,
            boolean transactionControl,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Executor executor,
            Metadata metadata)
    {
        return beginWithTicker(queryId, query, session, self, transactionControl, transactionManager, accessControl, executor, Ticker.systemTicker(), metadata);
    }

    static QueryStateMachine beginWithTicker(
            QueryId queryId,
            String query,
            Session session,
            URI self,
            boolean transactionControl,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Executor executor,
            Ticker ticker,
            Metadata metadata)
    {
        session.getTransactionId().ifPresent(transactionControl ? transactionManager::trySetActive : transactionManager::checkAndSetActive);

        Session querySession;
        boolean autoCommit = !session.getTransactionId().isPresent() && !transactionControl;
        if (autoCommit) {
            // TODO: make autocommit isolation level a session parameter
            TransactionId transactionId = transactionManager.beginTransaction(true);
            querySession = session.beginTransactionId(transactionId, transactionManager, accessControl);
        }
        else {
            querySession = session;
        }

        QueryStateMachine queryStateMachine = new QueryStateMachine(queryId, query, querySession, self, autoCommit, transactionManager, executor, ticker, metadata);
        queryStateMachine.addStateChangeListener(newState -> {
            log.debug("Query %s is %s", queryId, newState);
            if (newState.isDone()) {
                session.getTransactionId().ifPresent(transactionManager::trySetInactive);
            }
        });

        return queryStateMachine;
    }

    /**
     * Create a QueryStateMachine that is already in a failed state.
     */
    public static QueryStateMachine failed(QueryId queryId, String query, Session session, URI self, TransactionManager transactionManager, Executor executor, Metadata metadata, Throwable throwable)
    {
        return failedWithTicker(queryId, query, session, self, transactionManager, executor, Ticker.systemTicker(), metadata, throwable);
    }

    static QueryStateMachine failedWithTicker(
            QueryId queryId,
            String query,
            Session session,
            URI self,
            TransactionManager transactionManager,
            Executor executor,
            Ticker ticker,
            Metadata metadata,
            Throwable throwable)
    {
        QueryStateMachine queryStateMachine = new QueryStateMachine(queryId, query, session, self, false, transactionManager, executor, ticker, metadata);
        queryStateMachine.transitionToFailed(throwable);
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

    public boolean isAutoCommit()
    {
        return autoCommit;
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

    public void updateMemoryUsage(long deltaUserMemoryInBytes, long deltaTotalMemoryInBytes, long taskTotalMemoryInBytes)
    {
        currentUserMemory.addAndGet(deltaUserMemoryInBytes);
        currentTotalMemory.addAndGet(deltaTotalMemoryInBytes);
        peakUserMemory.updateAndGet(currentPeakValue -> Math.max(currentUserMemory.get(), currentPeakValue));
        peakTotalMemory.updateAndGet(currentPeakValue -> Math.max(currentTotalMemory.get(), currentPeakValue));
        peakTaskTotalMemory.accumulateAndGet(taskTotalMemoryInBytes, Math::max);
    }

    public void setResourceGroup(ResourceGroupId group)
    {
        requireNonNull(group, "group is null");
        resourceGroup.compareAndSet(null, group);
    }

    public Optional<ResourceGroupId> getResourceGroup()
    {
        return Optional.ofNullable(resourceGroup.get());
    }

    public QueryInfo getQueryInfoWithoutDetails()
    {
        return getQueryInfo(Optional.empty());
    }

    public QueryInfo getQueryInfo(Optional<StageInfo> rootStage)
    {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        Duration elapsedTime;
        if (endNanos.get() != 0) {
            elapsedTime = new Duration(endNanos.get() - createNanos, NANOSECONDS);
        }
        else {
            elapsedTime = nanosSince(createNanos);
        }

        // don't report failure info is query is marked as success
        FailureInfo failureInfo = null;
        ErrorCode errorCode = null;
        if (state == FAILED) {
            ExecutionFailureInfo failureCause = this.failureCause.get();
            if (failureCause != null) {
                failureInfo = failureCause.toFailureInfo();
                errorCode = failureCause.getErrorCode();
            }
        }

        int totalTasks = 0;
        int runningTasks = 0;
        int completedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int blockedDrivers = 0;
        int completedDrivers = 0;

        long cumulativeUserMemory = 0;
        long userMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long totalUserTime = 0;
        long totalBlockedTime = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        long physicalWrittenDataSize = 0;

        ImmutableList.Builder<StageGcStatistics> stageGcStatistics = ImmutableList.builder();

        boolean fullyBlocked = rootStage.isPresent();
        Set<BlockedReason> blockedReasons = new HashSet<>();

        ImmutableList.Builder<OperatorStats> operatorStatsSummary = ImmutableList.builder();
        boolean completeInfo = true;
        for (StageInfo stageInfo : getAllStages(rootStage)) {
            StageStats stageStats = stageInfo.getStageStats();
            totalTasks += stageStats.getTotalTasks();
            runningTasks += stageStats.getRunningTasks();
            completedTasks += stageStats.getCompletedTasks();

            totalDrivers += stageStats.getTotalDrivers();
            queuedDrivers += stageStats.getQueuedDrivers();
            runningDrivers += stageStats.getRunningDrivers();
            blockedDrivers += stageStats.getBlockedDrivers();
            completedDrivers += stageStats.getCompletedDrivers();

            cumulativeUserMemory += stageStats.getCumulativeUserMemory();
            userMemoryReservation += stageStats.getUserMemoryReservation().toBytes();
            totalMemoryReservation += stageStats.getTotalMemoryReservation().toBytes();
            totalScheduledTime += stageStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageStats.getTotalCpuTime().roundTo(MILLISECONDS);
            totalUserTime += stageStats.getTotalUserTime().roundTo(MILLISECONDS);
            totalBlockedTime += stageStats.getTotalBlockedTime().roundTo(MILLISECONDS);
            if (!stageInfo.getState().isDone()) {
                fullyBlocked &= stageStats.isFullyBlocked();
                blockedReasons.addAll(stageStats.getBlockedReasons());
            }

            PlanFragment plan = stageInfo.getPlan();
            if (plan != null && plan.getPartitionedSourceNodes().stream().anyMatch(TableScanNode.class::isInstance)) {
                rawInputDataSize += stageStats.getRawInputDataSize().toBytes();
                rawInputPositions += stageStats.getRawInputPositions();

                processedInputDataSize += stageStats.getProcessedInputDataSize().toBytes();
                processedInputPositions += stageStats.getProcessedInputPositions();
            }

            physicalWrittenDataSize += stageStats.getPhysicalWrittenDataSize().toBytes();

            stageGcStatistics.add(stageStats.getGcInfo());

            completeInfo = completeInfo && stageInfo.isCompleteInfo();
            operatorStatsSummary.addAll(stageInfo.getStageStats().getOperatorSummaries());
        }

        if (rootStage.isPresent()) {
            StageStats outputStageStats = rootStage.get().getStageStats();
            outputDataSize += outputStageStats.getOutputDataSize().toBytes();
            outputPositions += outputStageStats.getOutputPositions();
        }

        boolean isScheduled = isScheduled(rootStage);

        QueryStats queryStats = new QueryStats(
                createTime,
                executionStartTime.get(),
                lastHeartbeat.get(),
                endTime.get(),

                elapsedTime.convertToMostSuccinctTimeUnit(),
                queuedTime.get(),
                resourceWaitingTime.get(),
                analysisTime.get(),
                distributedPlanningTime.get(),
                totalPlanningTime.get(),
                finishingTime.get(),

                totalTasks,
                runningTasks,
                completedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                cumulativeUserMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),
                succinctBytes(getPeakUserMemoryInBytes()),
                succinctBytes(getPeakTotalMemoryInBytes()),
                succinctBytes(getPeakTaskTotalMemory()),

                isScheduled,

                new Duration(totalScheduledTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked,
                blockedReasons,

                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(outputDataSize),
                outputPositions,

                succinctBytes(physicalWrittenDataSize),

                stageGcStatistics.build(),

                operatorStatsSummary.build());

        return new QueryInfo(queryId,
                session.toSessionRepresentation(),
                state,
                memoryPool.get().getId(),
                isScheduled,
                self,
                outputManager.getQueryOutputInfo().map(QueryOutputInfo::getColumnNames).orElse(ImmutableList.of()),
                query,
                queryStats,
                Optional.ofNullable(setCatalog.get()),
                Optional.ofNullable(setSchema.get()),
                Optional.ofNullable(setPath.get()),
                setSessionProperties,
                resetSessionProperties,
                addedPreparedStatements,
                deallocatedPreparedStatements,
                Optional.ofNullable(startedTransactionId.get()),
                clearTransactionId.get(),
                updateType.get(),
                rootStage,
                failureInfo,
                errorCode,
                inputs.get(),
                output.get(),
                completeInfo,
                getResourceGroup());
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

    public void updateOutputLocations(Set<URI> newExchangeLocations, boolean noMoreExchangeLocations)
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

    public void setSetPath(String path)
    {
        requireNonNull(path, "path is null");
        setPath.set(path);
    }

    public String getSetPath()
    {
        return setPath.get();
    }

    public void addSetSessionProperties(String key, String value)
    {
        setSessionProperties.put(requireNonNull(key, "key is null"), requireNonNull(value, "value is null"));
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

    public QueryState getQueryState()
    {
        return queryState.get();
    }

    public boolean isDone()
    {
        return queryState.get().isDone();
    }

    public boolean transitionToWaitingForResources()
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos).convertToMostSuccinctTimeUnit());
        resourceWaitingStartNanos.compareAndSet(null, tickerNanos());
        return queryState.compareAndSet(QUEUED, WAITING_FOR_RESOURCES);
    }

    public boolean transitionToPlanning()
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos).convertToMostSuccinctTimeUnit());
        resourceWaitingStartNanos.compareAndSet(null, tickerNanos());
        resourceWaitingTime.compareAndSet(null, nanosSince(resourceWaitingStartNanos.get()).convertToMostSuccinctTimeUnit());
        totalPlanningStartNanos.compareAndSet(null, tickerNanos());
        return queryState.setIf(PLANNING, currentState -> currentState == QUEUED || currentState == WAITING_FOR_RESOURCES);
    }

    public boolean transitionToStarting()
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos).convertToMostSuccinctTimeUnit());
        resourceWaitingStartNanos.compareAndSet(null, tickerNanos());
        resourceWaitingTime.compareAndSet(null, nanosSince(resourceWaitingStartNanos.get()).convertToMostSuccinctTimeUnit());
        totalPlanningStartNanos.compareAndSet(null, tickerNanos());
        totalPlanningTime.compareAndSet(null, nanosSince(totalPlanningStartNanos.get()));

        return queryState.setIf(STARTING, currentState -> currentState == QUEUED || currentState == PLANNING);
    }

    public boolean transitionToRunning()
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos).convertToMostSuccinctTimeUnit());
        resourceWaitingStartNanos.compareAndSet(null, tickerNanos());
        resourceWaitingTime.compareAndSet(null, nanosSince(resourceWaitingStartNanos.get()).convertToMostSuccinctTimeUnit());
        totalPlanningStartNanos.compareAndSet(null, tickerNanos());
        totalPlanningTime.compareAndSet(null, nanosSince(totalPlanningStartNanos.get()));
        executionStartTime.compareAndSet(null, DateTime.now());

        return queryState.setIf(RUNNING, currentState -> currentState != RUNNING && currentState != FINISHING && !currentState.isDone());
    }

    public boolean transitionToFinishing()
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos).convertToMostSuccinctTimeUnit());
        resourceWaitingStartNanos.compareAndSet(null, tickerNanos());
        resourceWaitingTime.compareAndSet(null, nanosSince(resourceWaitingStartNanos.get()).convertToMostSuccinctTimeUnit());
        totalPlanningStartNanos.compareAndSet(null, tickerNanos());
        totalPlanningTime.compareAndSet(null, nanosSince(totalPlanningStartNanos.get()));
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        finishingStartNanos.compareAndSet(null, tickerNanos());

        if (!queryState.setIf(FINISHING, currentState -> currentState != FINISHING && !currentState.isDone())) {
            return false;
        }

        if (autoCommit) {
            ListenableFuture<?> commitFuture = transactionManager.asyncCommit(session.getTransactionId().get());
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
                    transitionToFailed(throwable);
                }
            }, directExecutor());
        }
        else {
            transitionToFinished();
        }
        return true;
    }

    private boolean transitionToFinished()
    {
        cleanupQueryQuietly();
        recordDoneStats();

        return queryState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        cleanupQueryQuietly();
        recordDoneStats();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        requireNonNull(throwable, "throwable is null");
        failureCause.compareAndSet(null, toFailure(throwable));

        boolean failed = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            log.debug(throwable, "Query %s failed", queryId);
            session.getTransactionId().ifPresent(autoCommit ? transactionManager::asyncAbort : transactionManager::fail);
        }
        else {
            log.debug(throwable, "Failure after query %s finished", queryId);
        }

        return failed;
    }

    public boolean transitionToCanceled()
    {
        cleanupQueryQuietly();
        recordDoneStats();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        failureCause.compareAndSet(null, toFailure(new PrestoException(USER_CANCELED, "Query was canceled")));

        boolean canceled = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        if (canceled) {
            session.getTransactionId().ifPresent(autoCommit ? transactionManager::asyncAbort : transactionManager::fail);
        }

        return canceled;
    }

    private void cleanupQueryQuietly()
    {
        try {
            metadata.cleanupQuery(session);
        }
        catch (Throwable t) {
            log.error("Error cleaning up query: %s", t);
        }
    }

    private void recordDoneStats()
    {
        Duration durationSinceCreation = nanosSince(createNanos).convertToMostSuccinctTimeUnit();
        queuedTime.compareAndSet(null, durationSinceCreation);
        resourceWaitingTime.compareAndSet(null, succinctNanos(0));
        totalPlanningStartNanos.compareAndSet(null, tickerNanos());
        totalPlanningTime.compareAndSet(null, nanosSince(totalPlanningStartNanos.get()));
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        finishingStartNanos.compareAndSet(null, tickerNanos());
        finishingTime.compareAndSet(null, nanosSince(finishingStartNanos.get()));
        endTime.compareAndSet(null, now);
        endNanos.compareAndSet(0, tickerNanos());
    }

    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    public void addQueryInfoStateChangeListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<QueryInfo>> fireOnceStateChangeListener = finalQueryInfo -> {
            if (finalQueryInfo.isPresent() && done.compareAndSet(false, true)) {
                stateChangeListener.stateChanged(finalQueryInfo.get());
            }
        };
        finalQueryInfo.addStateChangeListener(fireOnceStateChangeListener);
        fireOnceStateChangeListener.stateChanged(finalQueryInfo.get());
    }

    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return queryState.getStateChange(currentState);
    }

    public void recordHeartbeat()
    {
        this.lastHeartbeat.set(DateTime.now());
    }

    public void recordAnalysisTime(long analysisStart)
    {
        analysisTime.compareAndSet(null, nanosSince(analysisStart).convertToMostSuccinctTimeUnit());
    }

    public void recordDistributedPlanningTime(long distributedPlanningStart)
    {
        distributedPlanningTime.compareAndSet(null, nanosSince(distributedPlanningStart).convertToMostSuccinctTimeUnit());
    }

    private static boolean isScheduled(Optional<StageInfo> rootStage)
    {
        if (!rootStage.isPresent()) {
            return false;
        }
        return getAllStages(rootStage).stream()
                .map(StageInfo::getState)
                .allMatch(state -> (state == StageState.RUNNING) || state.isDone());
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
        StageInfo outputStage = queryInfo.getOutputStage().get();
        StageInfo prunedOutputStage = new StageInfo(
                outputStage.getStageId(),
                outputStage.getState(),
                outputStage.getSelf(),
                null, // Remove the plan
                outputStage.getTypes(),
                outputStage.getStageStats(),
                ImmutableList.of(), // Remove the tasks
                ImmutableList.of(), // Remove the substages
                outputStage.getFailureCause());

        QueryInfo prunedQueryInfo = new QueryInfo(
                queryInfo.getQueryId(),
                queryInfo.getSession(),
                queryInfo.getState(),
                getMemoryPool().getId(),
                queryInfo.isScheduled(),
                queryInfo.getSelf(),
                queryInfo.getFieldNames(),
                queryInfo.getQuery(),
                pruneQueryStats(queryInfo.getQueryStats()),
                queryInfo.getSetCatalog(),
                queryInfo.getSetSchema(),
                queryInfo.getSetPath(),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                queryInfo.getAddedPreparedStatements(),
                queryInfo.getDeallocatedPreparedStatements(),
                queryInfo.getStartedTransactionId(),
                queryInfo.isClearTransactionId(),
                queryInfo.getUpdateType(),
                Optional.of(prunedOutputStage),
                queryInfo.getFailureInfo(),
                queryInfo.getErrorCode(),
                queryInfo.getInputs(),
                queryInfo.getOutput(),
                queryInfo.isCompleteInfo(),
                queryInfo.getResourceGroupId());
        finalQueryInfo.compareAndSet(finalInfo, Optional.of(prunedQueryInfo));
    }

    private QueryStats pruneQueryStats(QueryStats queryStats)
    {
        return new QueryStats(
                queryStats.getCreateTime(),
                queryStats.getExecutionStartTime(),
                queryStats.getLastHeartbeat(),
                queryStats.getEndTime(),
                queryStats.getElapsedTime(),
                queryStats.getQueuedTime(),
                queryStats.getResourceWaitingTime(),
                queryStats.getAnalysisTime(),
                queryStats.getDistributedPlanningTime(),
                queryStats.getTotalPlanningTime(),
                queryStats.getFinishingTime(),
                queryStats.getTotalTasks(),
                queryStats.getRunningTasks(),
                queryStats.getCompletedTasks(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getBlockedDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getUserMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getPeakTaskTotalMemory(),
                queryStats.isScheduled(),
                queryStats.getTotalScheduledTime(),
                queryStats.getTotalCpuTime(),
                queryStats.getTotalUserTime(),
                queryStats.getTotalBlockedTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
                queryStats.getRawInputDataSize(),
                queryStats.getRawInputPositions(),
                queryStats.getProcessedInputDataSize(),
                queryStats.getProcessedInputPositions(),
                queryStats.getOutputDataSize(),
                queryStats.getOutputPositions(),
                queryStats.getPhysicalWrittenDataSize(),
                queryStats.getStageGcStatistics(),
                ImmutableList.of()); // Remove the operator summaries as OperatorInfo (especially ExchangeClientStatus) can hold onto a large amount of memory
    }

    private long tickerNanos()
    {
        return ticker.read();
    }

    private Duration nanosSince(long start)
    {
        return succinctNanos(tickerNanos() - start);
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
        private final Set<URI> exchangeLocations = new LinkedHashSet<>();
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

        public void updateOutputLocations(Set<URI> newExchangeLocations, boolean noMoreExchangeLocations)
        {
            requireNonNull(newExchangeLocations, "newExchangeLocations is null");

            Optional<QueryOutputInfo> queryOutputInfo;
            List<Consumer<QueryOutputInfo>> outputInfoListeners;
            synchronized (this) {
                if (this.noMoreExchangeLocations) {
                    checkArgument(this.exchangeLocations.containsAll(newExchangeLocations), "New locations added after no more locations set");
                    return;
                }

                this.exchangeLocations.addAll(newExchangeLocations);
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
