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
import com.facebook.presto.execution.QueryExecution.QueryOutputInfo;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
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

import static com.facebook.presto.execution.BasicStageStats.EMPTY_STAGE_STATS;
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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class QueryStateMachine
{
    public static final Logger QUERY_STATE_LOG = Logger.get(QueryStateMachine.class);

    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final URI self;
    private final Optional<QueryType> queryType;
    private final Optional<ResourceGroupId> resourceGroup;
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

    private final AtomicInteger currentRunningTaskCount = new AtomicInteger();
    private final AtomicInteger peakRunningTaskCount = new AtomicInteger();

    private final QueryStateTimer queryStateTimer;

    private final StateMachine<QueryState> queryState;

    private final AtomicReference<String> setCatalog = new AtomicReference<>();
    private final AtomicReference<String> setSchema = new AtomicReference<>();
    private final AtomicReference<String> setPath = new AtomicReference<>();

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

    private final WarningCollector warningCollector;

    private QueryStateMachine(
            String query,
            Session session,
            URI self,
            Optional<ResourceGroupId> resourceGroup,
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

        this.queryState = new StateMachine<>("query " + query, executor, QUEUED, TERMINAL_QUERY_STATES);
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
                Optional.of(resourceGroup),
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

    public void updateMemoryUsage(long deltaUserMemoryInBytes, long deltaTotalMemoryInBytes, long taskUserMemoryInBytes, long taskTotalMemoryInBytes)
    {
        currentUserMemory.addAndGet(deltaUserMemoryInBytes);
        currentTotalMemory.addAndGet(deltaTotalMemoryInBytes);
        peakUserMemory.updateAndGet(currentPeakValue -> Math.max(currentUserMemory.get(), currentPeakValue));
        peakTotalMemory.updateAndGet(currentPeakValue -> Math.max(currentTotalMemory.get(), currentPeakValue));
        peakTaskUserMemory.accumulateAndGet(taskUserMemoryInBytes, Math::max);
        peakTaskTotalMemory.accumulateAndGet(taskTotalMemoryInBytes, Math::max);
    }

    public BasicQueryInfo getBasicQueryInfo(Optional<BasicStageStats> rootStage)
    {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        ErrorCode errorCode = null;
        if (state == QueryState.FAILED) {
            ExecutionFailureInfo failureCause = this.failureCause.get();
            if (failureCause != null) {
                errorCode = failureCause.getErrorCode();
            }
        }

        BasicStageStats stageStats = rootStage.orElse(EMPTY_STAGE_STATS);
        BasicQueryStats queryStats = new BasicQueryStats(
                queryStateTimer.getCreateTime(),
                getEndTime().orElse(null),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getElapsedTime(),
                queryStateTimer.getExecutionTime(),

                stageStats.getTotalDrivers(),
                stageStats.getQueuedDrivers(),
                stageStats.getRunningDrivers(),
                stageStats.getCompletedDrivers(),

                stageStats.getRawInputDataSize(),
                stageStats.getRawInputPositions(),

                stageStats.getCumulativeUserMemory(),
                stageStats.getUserMemoryReservation(),
                stageStats.getTotalMemoryReservation(),
                succinctBytes(getPeakUserMemoryInBytes()),

                stageStats.getTotalCpuTime(),
                stageStats.getTotalScheduledTime(),

                stageStats.isFullyBlocked(),
                stageStats.getBlockedReasons(),
                stageStats.getProgressPercentage());

        return new BasicQueryInfo(
                queryId,
                session.toSessionRepresentation(),
                resourceGroup,
                state,
                memoryPool.get().getId(),
                stageStats.isScheduled(),
                self,
                query,
                queryStats,
                errorCode == null ? null : errorCode.getType(),
                errorCode,
                queryType,
                warningCollector.getWarnings());
    }

    @VisibleForTesting
    QueryInfo getQueryInfo(Optional<StageInfo> rootStage)
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
        boolean isScheduled = isScheduled(rootStage);
        Optional<List<TaskId>> failedTasks;
        // Traversing all tasks is expensive, thus only construct failedTasks list when query finished.
        if (state.isDone()) {
            failedTasks = Optional.of(getAllStages(rootStage).stream()
                    .map(StageInfo::getTasks)
                    .flatMap(List::stream)
                    .filter(taskInfo -> taskInfo.getTaskStatus().getState() == TaskState.FAILED)
                    .map(TaskInfo::getTaskStatus)
                    .map(TaskStatus::getTaskId)
                    .collect(toImmutableList()));
        }
        else {
            failedTasks = Optional.empty();
        }

        return new QueryInfo(
                queryId,
                session.toSessionRepresentation(),
                state,
                memoryPool.get().getId(),
                isScheduled,
                self,
                outputManager.getQueryOutputInfo().map(QueryOutputInfo::getColumnNames).orElse(ImmutableList.of()),
                query,
                getQueryStats(rootStage),
                Optional.ofNullable(setCatalog.get()),
                Optional.ofNullable(setSchema.get()),
                Optional.ofNullable(setPath.get()),
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
                resourceGroup,
                queryType,
                failedTasks);
    }

    private QueryStats getQueryStats(Optional<StageInfo> rootStage)
    {
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
        long totalBlockedTime = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        long writtenOutputPositions = 0;
        long writtenOutputLogicalDataSize = 0;
        long writtenOutputPhysicalDataSize = 0;

        long writtenIntermediatePhysicalDataSize = 0;

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
            totalBlockedTime += stageStats.getTotalBlockedTime().roundTo(MILLISECONDS);
            if (!stageInfo.getState().isDone()) {
                fullyBlocked &= stageStats.isFullyBlocked();
                blockedReasons.addAll(stageStats.getBlockedReasons());
            }

            if (stageInfo.getPlan().isPresent()) {
                PlanFragment plan = stageInfo.getPlan().get();
                if (!plan.getTableScanSchedulingOrder().isEmpty()) {
                    rawInputDataSize += stageStats.getRawInputDataSize().toBytes();
                    rawInputPositions += stageStats.getRawInputPositions();

                    processedInputDataSize += stageStats.getProcessedInputDataSize().toBytes();
                    processedInputPositions += stageStats.getProcessedInputPositions();
                }

                if (plan.isOutputTableWriterFragment()) {
                    writtenOutputPositions += stageInfo.getStageStats().getOperatorSummaries().stream()
                            .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.class.getSimpleName()))
                            .mapToLong(OperatorStats::getInputPositions)
                            .sum();
                    writtenOutputLogicalDataSize += stageInfo.getStageStats().getOperatorSummaries().stream()
                            .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.class.getSimpleName()))
                            .mapToLong(stats -> stats.getInputDataSize().toBytes())
                            .sum();
                    writtenOutputPhysicalDataSize += stageStats.getPhysicalWrittenDataSize().toBytes();
                }
                else {
                    writtenIntermediatePhysicalDataSize += stageStats.getPhysicalWrittenDataSize().toBytes();
                }
            }

            stageGcStatistics.add(stageStats.getGcInfo());

            completeInfo = completeInfo && stageInfo.isFinalStageInfo();
            operatorStatsSummary.addAll(stageInfo.getStageStats().getOperatorSummaries());
        }

        if (rootStage.isPresent()) {
            StageStats outputStageStats = rootStage.get().getStageStats();
            outputDataSize += outputStageStats.getOutputDataSize().toBytes();
            outputPositions += outputStageStats.getOutputPositions();
        }

        boolean isScheduled = isScheduled(rootStage);

        return new QueryStats(
                queryStateTimer.getCreateTime(),
                getExecutionStartTime().orElse(null),
                getLastHeartbeat(),
                getEndTime().orElse(null),

                queryStateTimer.getElapsedTime(),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getResourceWaitingTime(),
                queryStateTimer.getExecutionTime(),
                queryStateTimer.getAnalysisTime(),
                queryStateTimer.getPlanningTime(),
                queryStateTimer.getFinishingTime(),

                totalTasks,
                runningTasks,
                getPeakRunningTaskCount(),
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
                succinctBytes(getPeakTaskUserMemory()),
                succinctBytes(getPeakTaskTotalMemory()),

                isScheduled,

                succinctDuration(totalScheduledTime, MILLISECONDS),
                succinctDuration(totalCpuTime, MILLISECONDS),
                succinctDuration(totalBlockedTime, MILLISECONDS),
                fullyBlocked,
                blockedReasons,

                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(outputDataSize),
                outputPositions,

                writtenOutputPositions,
                succinctBytes(writtenOutputLogicalDataSize),
                succinctBytes(writtenOutputPhysicalDataSize),

                succinctBytes(writtenIntermediatePhysicalDataSize),

                stageGcStatistics.build(),

                operatorStatsSummary.build());
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
        queryStateTimer.beginWaitingForResources();
        return queryState.setIf(WAITING_FOR_RESOURCES, currentState -> currentState.ordinal() < WAITING_FOR_RESOURCES.ordinal());
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

        Optional<TransactionId> transactionId = session.getTransactionId();
        if (transactionId.isPresent() && transactionManager.transactionExists(transactionId.get()) && transactionManager.isAutoCommit(transactionId.get())) {
            ListenableFuture<?> commitFuture = transactionManager.asyncCommit(transactionId.get());
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

    private void transitionToFinished()
    {
        cleanupQueryQuietly();
        queryStateTimer.endQuery();

        queryState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        cleanupQueryQuietly();
        queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        requireNonNull(throwable, "throwable is null");
        failureCause.compareAndSet(null, toFailure(throwable));

        boolean failed = queryState.setIf(QueryState.FAILED, currentState -> !currentState.isDone());
        if (failed) {
            QUERY_STATE_LOG.debug(throwable, "Query %s failed", queryId);
            session.getTransactionId().ifPresent(transactionId -> {
                if (transactionManager.isAutoCommit(transactionId)) {
                    transactionManager.asyncAbort(transactionId);
                }
                else {
                    transactionManager.fail(transactionId);
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
            session.getTransactionId().ifPresent(transactionId -> {
                if (transactionManager.isAutoCommit(transactionId)) {
                    transactionManager.asyncAbort(transactionId);
                }
                else {
                    transactionManager.fail(transactionId);
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

    private static boolean isScheduled(Optional<StageInfo> rootStage)
    {
        if (!rootStage.isPresent()) {
            return false;
        }
        return getAllStages(rootStage).stream()
                .map(StageInfo::getState)
                .allMatch(state -> (state == StageState.RUNNING) || state.isDone());
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
                outputStage.getState(),
                outputStage.getSelf(),
                Optional.empty(), // Remove the plan
                outputStage.getTypes(),
                outputStage.getStageStats(),
                ImmutableList.of(), // Remove the tasks
                ImmutableList.of(), // Remove the substages
                outputStage.getFailureCause()));

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
                queryInfo.getFailedTasks());
        finalQueryInfo.compareAndSet(finalInfo, Optional.of(prunedQueryInfo));
    }

    private static QueryStats pruneQueryStats(QueryStats queryStats)
    {
        return new QueryStats(
                queryStats.getCreateTime(),
                queryStats.getExecutionStartTime(),
                queryStats.getLastHeartbeat(),
                queryStats.getEndTime(),
                queryStats.getElapsedTime(),
                queryStats.getQueuedTime(),
                queryStats.getResourceWaitingTime(),
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
                queryStats.getUserMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getPeakTaskUserMemory(),
                queryStats.getPeakTaskTotalMemory(),
                queryStats.isScheduled(),
                queryStats.getTotalScheduledTime(),
                queryStats.getTotalCpuTime(),
                queryStats.getTotalBlockedTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
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
                ImmutableList.of()); // Remove the operator summaries as OperatorInfo (especially ExchangeClientStatus) can hold onto a large amount of memory
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
