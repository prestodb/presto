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
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.FINISHING;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.STARTING;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class QueryStateMachine
{
    private static final Logger log = Logger.get(QueryStateMachine.class);

    private final DateTime createTime = DateTime.now();
    private final long createNanos = System.nanoTime();
    private final AtomicLong endNanos = new AtomicLong();

    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final URI self;
    private final boolean autoCommit;
    private final TransactionManager transactionManager;

    private final AtomicReference<VersionedMemoryPoolId> memoryPool = new AtomicReference<>(new VersionedMemoryPoolId(GENERAL_POOL, 0));

    private final AtomicLong peakMemory = new AtomicLong();
    private final AtomicLong currentMemory = new AtomicLong();
    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicReference<DateTime> executionStartTime = new AtomicReference<>();
    private final AtomicReference<DateTime> endTime = new AtomicReference<>();

    private final AtomicReference<Duration> queuedTime = new AtomicReference<>();
    private final AtomicReference<Duration> analysisTime = new AtomicReference<>();
    private final AtomicReference<Duration> distributedPlanningTime = new AtomicReference<>();

    private final AtomicReference<Long> finishingStartNanos = new AtomicReference<>();
    private final AtomicReference<Duration> finishingTime = new AtomicReference<>();

    private final AtomicReference<Duration> totalPlanningTime = new AtomicReference<>();

    private final StateMachine<QueryState> queryState;

    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();

    private final AtomicReference<TransactionId> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();

    private final AtomicReference<String> updateType = new AtomicReference<>();

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<List<String>> outputFieldNames = new AtomicReference<>(ImmutableList.of());

    private final AtomicReference<Set<Input>> inputs = new AtomicReference<>(ImmutableSet.of());

    private QueryStateMachine(QueryId queryId, String query, Session session, URI self, boolean autoCommit, TransactionManager transactionManager, Executor executor)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.query = requireNonNull(query, "query is null");
        this.session = requireNonNull(session, "session is null");
        this.self = requireNonNull(self, "self is null");
        this.autoCommit = autoCommit;
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        this.queryState = new StateMachine<>("query " + query, executor, QUEUED, TERMINAL_QUERY_STATES);
    }

    /**
     * Created QueryStateMachines must be transitioned to terminal states to clean up resources.
     */
    public static QueryStateMachine begin(QueryId queryId, String query, Session session, URI self, boolean transactionControl, TransactionManager transactionManager, Executor executor)
    {
        session.getTransactionId().ifPresent(transactionControl ? transactionManager::trySetActive : transactionManager::checkAndSetActive);

        Session querySession;
        boolean autoCommit = !session.getTransactionId().isPresent() && !transactionControl;
        if (autoCommit) {
            // TODO: make autocommit isolation level a session parameter
            TransactionId transactionId = transactionManager.beginTransaction(true);
            querySession = session.withTransactionId(transactionId);
        }
        else {
            querySession = session;
        }

        QueryStateMachine queryStateMachine = new QueryStateMachine(queryId, query, querySession, self, autoCommit, transactionManager, executor);
        queryStateMachine.addStateChangeListener(newState -> log.debug("Query %s is %s", queryId, newState));
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                session.getTransactionId().ifPresent(transactionManager::trySetInactive);
            }
        });

        return queryStateMachine;
    }

    /**
     * Create a QueryStateMachine that is already in a failed state.
     */
    public static QueryStateMachine failed(QueryId queryId, String query, Session session, URI self, TransactionManager transactionManager, Executor executor, Throwable throwable)
    {
        QueryStateMachine queryStateMachine = new QueryStateMachine(queryId, query, session, self, false, transactionManager, executor);
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

    public long getPeakMemoryInBytes()
    {
        return peakMemory.get();
    }

    public void updateMemoryUsage(long deltaMemoryInBytes)
    {
        long currentMemoryValue = currentMemory.addAndGet(deltaMemoryInBytes);
        if (currentMemoryValue > peakMemory.get()) {
            peakMemory.updateAndGet(x -> currentMemoryValue > x ? currentMemoryValue : x);
        }
    }

    public QueryInfo getQueryInfoWithoutDetails()
    {
        return getQueryInfo(null);
    }

    public QueryInfo getQueryInfo(StageInfo rootStage)
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
        if (state != FINISHED) {
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
        int completedDrivers = 0;

        long cumulativeMemory = 0;
        long totalMemoryReservation = 0;
        long peakMemoryReservation = 0;

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

        boolean fullyBlocked = rootStage != null;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        if (rootStage != null) {
            for (StageInfo stageInfo : getAllStages(rootStage)) {
                StageStats stageStats = stageInfo.getStageStats();
                totalTasks += stageStats.getTotalTasks();
                runningTasks += stageStats.getRunningTasks();
                completedTasks += stageStats.getCompletedTasks();

                totalDrivers += stageStats.getTotalDrivers();
                queuedDrivers += stageStats.getQueuedDrivers();
                runningDrivers += stageStats.getRunningDrivers();
                completedDrivers += stageStats.getCompletedDrivers();

                cumulativeMemory += stageStats.getCumulativeMemory();
                totalMemoryReservation += stageStats.getTotalMemoryReservation().toBytes();
                peakMemoryReservation = getPeakMemoryInBytes();

                totalScheduledTime += stageStats.getTotalScheduledTime().roundTo(NANOSECONDS);
                totalCpuTime += stageStats.getTotalCpuTime().roundTo(NANOSECONDS);
                totalUserTime += stageStats.getTotalUserTime().roundTo(NANOSECONDS);
                totalBlockedTime += stageStats.getTotalBlockedTime().roundTo(NANOSECONDS);
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
            }

            StageStats outputStageStats = rootStage.getStageStats();
            outputDataSize += outputStageStats.getOutputDataSize().toBytes();
            outputPositions += outputStageStats.getOutputPositions();
        }

        QueryStats queryStats = new QueryStats(
                createTime,
                executionStartTime.get(),
                lastHeartbeat.get(),
                endTime.get(),

                elapsedTime.convertToMostSuccinctTimeUnit(),
                queuedTime.get(),
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
                completedDrivers,

                cumulativeMemory,
                new DataSize(totalMemoryReservation, BYTE).convertToMostSuccinctDataSize(),
                new DataSize(peakMemoryReservation, BYTE).convertToMostSuccinctDataSize(),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked,
                blockedReasons,

                new DataSize(rawInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                rawInputPositions,
                new DataSize(processedInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                processedInputPositions,
                new DataSize(outputDataSize, BYTE).convertToMostSuccinctDataSize(),
                outputPositions);

        return new QueryInfo(queryId,
                session.toSessionRepresentation(),
                state,
                memoryPool.get().getId(),
                isScheduled(rootStage),
                self,
                outputFieldNames.get(),
                query,
                queryStats,
                setSessionProperties,
                resetSessionProperties,
                Optional.ofNullable(startedTransactionId.get()),
                clearTransactionId.get(),
                updateType.get(),
                rootStage,
                failureInfo,
                errorCode,
                inputs.get());
    }

    public VersionedMemoryPoolId getMemoryPool()
    {
        return memoryPool.get();
    }

    public void setMemoryPool(VersionedMemoryPoolId memoryPool)
    {
        this.memoryPool.set(requireNonNull(memoryPool, "memoryPool is null"));
    }

    public void setOutputFieldNames(List<String> outputFieldNames)
    {
        requireNonNull(outputFieldNames, "outputFieldNames is null");
        this.outputFieldNames.set(ImmutableList.copyOf(outputFieldNames));
    }

    public void setInputs(List<Input> inputs)
    {
        requireNonNull(inputs, "inputs is null");
        this.inputs.set(ImmutableSet.copyOf(inputs));
    }

    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
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

    public boolean transitionToPlanning()
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos).convertToMostSuccinctTimeUnit());
        return queryState.compareAndSet(QUEUED, PLANNING);
    }

    public boolean transitionToStarting()
    {
        Duration durationSinceCreation = nanosSince(createNanos).convertToMostSuccinctTimeUnit();
        queuedTime.compareAndSet(null, durationSinceCreation);
        totalPlanningTime.compareAndSet(null, durationSinceCreation);

        return queryState.setIf(STARTING, currentState -> currentState == QUEUED || currentState == PLANNING);
    }

    public boolean transitionToRunning()
    {
        Duration durationSinceCreation = nanosSince(createNanos).convertToMostSuccinctTimeUnit();
        queuedTime.compareAndSet(null, durationSinceCreation);
        totalPlanningTime.compareAndSet(null, durationSinceCreation);
        executionStartTime.compareAndSet(null, DateTime.now());

        return queryState.setIf(RUNNING, currentState -> currentState != RUNNING && currentState != FINISHING && !currentState.isDone());
    }

    public boolean transitionToFinishing()
    {
        Duration durationSinceCreation = nanosSince(createNanos).convertToMostSuccinctTimeUnit();
        queuedTime.compareAndSet(null, durationSinceCreation);
        totalPlanningTime.compareAndSet(null, durationSinceCreation);
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        finishingStartNanos.compareAndSet(null, System.nanoTime());

        if (!queryState.setIf(FINISHING, currentState -> currentState != FINISHING && !currentState.isDone())) {
            return false;
        }

        if (autoCommit) {
            transactionManager.asyncCommit(session.getTransactionId().get())
                    .whenComplete((value, throwable) -> {
                        if (throwable == null) {
                            transitionToFinished();
                        }
                        else {
                            transitionToFailed(throwable);
                        }
                    });
        }
        else {
            transitionToFinished();
        }
        return true;
    }

    private boolean transitionToFinished()
    {
        Duration durationSinceCreation = nanosSince(createNanos).convertToMostSuccinctTimeUnit();
        queuedTime.compareAndSet(null, durationSinceCreation);
        totalPlanningTime.compareAndSet(null, durationSinceCreation);
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        finishingStartNanos.compareAndSet(null, System.nanoTime());
        finishingTime.compareAndSet(null, nanosSince(finishingStartNanos.get()));
        endTime.compareAndSet(null, now);
        endNanos.compareAndSet(0, System.nanoTime());

        return queryState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");

        Duration durationSinceCreation = nanosSince(createNanos).convertToMostSuccinctTimeUnit();
        queuedTime.compareAndSet(null, durationSinceCreation);
        totalPlanningTime.compareAndSet(null, durationSinceCreation);
        DateTime now = DateTime.now();
        executionStartTime.compareAndSet(null, now);
        finishingStartNanos.compareAndSet(null, System.nanoTime());
        finishingTime.compareAndSet(null, nanosSince(finishingStartNanos.get()));
        endTime.compareAndSet(null, now);
        endNanos.compareAndSet(0, System.nanoTime());

        failureCause.compareAndSet(null, toFailure(throwable));
        boolean failed = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            log.debug(throwable, "Query %s failed", queryId);
        }
        else {
            log.debug(throwable, "Failure after query %s finished", queryId);
        }

        session.getTransactionId().ifPresent(autoCommit ? transactionManager::asyncAbort : transactionManager::fail);

        return failed;
    }

    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return queryState.waitForStateChange(currentState, maxWait);
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

    private static boolean isScheduled(StageInfo rootStage)
    {
        if (rootStage == null) {
            return false;
        }
        return getAllStages(rootStage).stream()
                .map(StageInfo::getState)
                .allMatch(state -> (state == StageState.RUNNING) || state.isDone());
    }
}
