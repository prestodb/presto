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

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.Input;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.sql.analyzer.Session;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.QueryState.CANCELED;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.inDoneState;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class QueryStateMachine
{
    private static final Logger log = Logger.get(QueryStateMachine.class);

    private final DateTime createTime = DateTime.now();
    private final long createNanos = System.nanoTime();

    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final URI self;

    @GuardedBy("this")
    private DateTime lastHeartbeat = DateTime.now();
    @GuardedBy("this")
    private DateTime executionStartTime;
    @GuardedBy("this")
    private DateTime endTime;

    @GuardedBy("this")
    private Duration queuedTime;
    @GuardedBy("this")
    private Duration analysisTime;
    @GuardedBy("this")
    private Duration distributedPlanningTime;

    private final StateMachine<QueryState> queryState;

    @GuardedBy("this")
    private Throwable failureCause;

    @GuardedBy("this")
    private List<String> outputFieldNames = ImmutableList.of();

    @GuardedBy("this")
    private Set<Input> inputs = ImmutableSet.of();

    public QueryStateMachine(QueryId queryId, String query, Session session, URI self, Executor executor)
    {
        this.queryId = checkNotNull(queryId, "queryId is null");
        this.query = checkNotNull(query, "query is null");
        this.session = checkNotNull(session, "session is null");
        this.self = checkNotNull(self, "self is null");

        this.queryState = new StateMachine<>("query " + query, executor, QueryState.QUEUED);
        queryState.addStateChangeListener(new StateChangeListener<QueryState>()
        {
            @Override
            public void stateChanged(QueryState newValue)
            {
                log.debug("Query %s is %s", QueryStateMachine.this.queryId, newValue);
            }
        });
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public Session getSession()
    {
        return session;
    }

    public QueryInfo getQueryInfoWithoutDetails()
    {
        return getQueryInfo(null);
    }

    public synchronized QueryInfo getQueryInfo(StageInfo rootStage)
    {
        QueryState state = queryState.get();

        Duration elapsedTime;
        if (endTime != null) {
            elapsedTime = new Duration(endTime.getMillis() - createTime.getMillis(), MILLISECONDS);
        }
        else {
            elapsedTime = Duration.nanosSince(createNanos);
        }

        // don't report failure info is query is marked as success
        FailureInfo failureInfo = null;
        if (state != FINISHED) {
            failureInfo = toFailure(failureCause);
        }

        int totalTasks = 0;
        int runningTasks = 0;
        int completedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int completedDrivers = 0;

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

                totalMemoryReservation += stageStats.getTotalMemoryReservation().toBytes();

                totalScheduledTime += stageStats.getTotalScheduledTime().roundTo(NANOSECONDS);
                totalCpuTime += stageStats.getTotalCpuTime().roundTo(NANOSECONDS);
                totalUserTime += stageStats.getTotalUserTime().roundTo(NANOSECONDS);
                totalBlockedTime += stageStats.getTotalBlockedTime().roundTo(NANOSECONDS);

                if (stageInfo.getSubStages().isEmpty()) {
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
                executionStartTime,
                lastHeartbeat,
                endTime,

                elapsedTime.convertToMostSuccinctTimeUnit(),
                queuedTime,
                analysisTime,
                distributedPlanningTime,

                totalTasks,
                runningTasks,
                completedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,

                new DataSize(totalMemoryReservation, BYTE).convertToMostSuccinctDataSize(),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new DataSize(rawInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                rawInputPositions,
                new DataSize(processedInputDataSize, BYTE).convertToMostSuccinctDataSize(),
                processedInputPositions,
                new DataSize(outputDataSize, BYTE).convertToMostSuccinctDataSize(),
                outputPositions);

        return new QueryInfo(queryId,
                session,
                state,
                self,
                outputFieldNames,
                query,
                queryStats,
                rootStage,
                failureInfo,
                inputs);
    }

    public synchronized void setOutputFieldNames(List<String> outputFieldNames)
    {
        checkNotNull(outputFieldNames, "outputFieldNames is null");
        this.outputFieldNames = ImmutableList.copyOf(outputFieldNames);
    }

    public synchronized void setInputs(List<Input> inputs)
    {
        Preconditions.checkNotNull(inputs, "inputs is null");
        this.inputs = ImmutableSet.copyOf(inputs);
    }

    public synchronized QueryState getQueryState()
    {
        return queryState.get();
    }

    public synchronized boolean isDone()
    {
        return queryState.get().isDone();
    }

    public boolean beginPlanning()
    {
        // transition from queued to planning
        if (!queryState.compareAndSet(QueryState.QUEUED, QueryState.PLANNING)) {
            return false;
        }

        // planning has begun
        synchronized (this) {
            Preconditions.checkState(createNanos > 0, "Can not record analysis start");
            queuedTime = Duration.nanosSince(createNanos).convertToMostSuccinctTimeUnit();
        }
        return true;
    }

    public synchronized boolean starting()
    {
        // transition from queued or planning to starting
        return queryState.setIf(QueryState.STARTING, Predicates.in(ImmutableSet.of(QueryState.QUEUED, QueryState.PLANNING)));
    }

    public synchronized boolean running()
    {
        // transition to running if not already done
        return queryState.setIf(QueryState.RUNNING, Predicates.not(inDoneState()));
    }

    public boolean finished()
    {
        synchronized (this) {
            if (endTime == null) {
                endTime = DateTime.now();
            }
        }
        return queryState.setIf(FINISHED, Predicates.not(inDoneState()));
    }

    public boolean cancel()
    {
        synchronized (this) {
            if (endTime == null) {
                endTime = DateTime.now();
            }
        }
        synchronized (this) {
            if (failureCause == null) {
                failureCause = new RuntimeException("Query was canceled");
            }
        }
        return queryState.setIf(CANCELED, Predicates.not(inDoneState()));
    }

    public boolean fail(@Nullable Throwable cause)
    {
        synchronized (this) {
            if (endTime == null) {
                endTime = DateTime.now();
            }
        }
        synchronized (this) {
            if (cause != null) {
                if (failureCause == null) {
                    failureCause = cause;
                }
                else {
                    failureCause.addSuppressed(cause);
                }
            }
        }
        return queryState.setIf(FAILED, Predicates.not(inDoneState()));
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

    public synchronized void recordHeartbeat()
    {
        this.lastHeartbeat = DateTime.now();
    }

    public synchronized void recordExecutionStart()
    {
        if (executionStartTime == null) {
            this.executionStartTime = DateTime.now();
        }
    }

    public synchronized void recordAnalysisTime(long analysisStart)
    {
        analysisTime = Duration.nanosSince(analysisStart).convertToMostSuccinctTimeUnit();
    }

    public synchronized void recordDistributedPlanningTime(long distributedPlanningStart)
    {
        distributedPlanningTime = Duration.nanosSince(distributedPlanningStart).convertToMostSuccinctTimeUnit();
    }
}
