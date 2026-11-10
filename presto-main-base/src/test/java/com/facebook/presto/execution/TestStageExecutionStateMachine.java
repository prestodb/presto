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

import com.facebook.presto.common.QueryTracer;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeMetricEvent;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.common.RuntimeMetricName.QUERY_TRACE_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.TASK_UPDATE_ROUND_TRIP_TIME;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestStageExecutionStateMachine
{
    private static final StageExecutionId STAGE_ID = new StageExecutionId(new StageId(new QueryId("query"), 0), 0);
    private static final SQLException FAILED_CAUSE = new SQLException("FAILED");
    private static final String STAGE_TRACE_PREFIX = "S" + STAGE_ID.getStageId().getId() + "-";
    private static final String STAGE_PLANNED_TRACE = STAGE_TRACE_PREFIX + "stagePlanned";
    private static final String STAGE_SCHEDULING_TRACE = STAGE_TRACE_PREFIX + "stageScheduling";
    private static final String STAGE_FINISHED_TASK_SCHEDULING_TRACE = STAGE_TRACE_PREFIX + "stageFinishedTaskScheduling";
    private static final String STAGE_SCHEDULING_SPLITS_TRACE = STAGE_TRACE_PREFIX + "stageSchedulingSplits";
    private static final String STAGE_SCHEDULED_TRACE = STAGE_TRACE_PREFIX + "stageScheduled";
    private static final String STAGE_RUNNING_TRACE = STAGE_TRACE_PREFIX + "stageRunning";
    private static final String STAGE_FINISHED_TRACE = STAGE_TRACE_PREFIX + "stageFinished";
    private static final String STAGE_FAILED_TRACE = STAGE_TRACE_PREFIX + "stageFailed";

    private final ExecutorService executor = newCachedThreadPool();

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testBasicStateChanges()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();
        assertState(stateMachine, StageExecutionState.PLANNED);

        assertTrue(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageExecutionState.SCHEDULING);

        assertTrue(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageExecutionState.SCHEDULED);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageExecutionState.RUNNING);

        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageExecutionState.FINISHED);
    }

    @Test
    public void testPlanned()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();
        assertState(stateMachine, StageExecutionState.PLANNED);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageExecutionState.SCHEDULING);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageExecutionState.RUNNING);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageExecutionState.FINISHED);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, StageExecutionState.FAILED);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToAborted());
        assertState(stateMachine, StageExecutionState.ABORTED);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToCanceled());
        assertState(stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testScheduling()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageExecutionState.SCHEDULING);

        assertFalse(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageExecutionState.SCHEDULING);

        assertTrue(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageExecutionState.SCHEDULED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageExecutionState.RUNNING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageExecutionState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, StageExecutionState.FAILED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToAborted());
        assertState(stateMachine, StageExecutionState.ABORTED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToCanceled());
        assertState(stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testScheduled()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageExecutionState.SCHEDULED);

        assertFalse(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageExecutionState.SCHEDULED);

        assertFalse(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageExecutionState.SCHEDULED);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageExecutionState.RUNNING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduled();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageExecutionState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduled();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, StageExecutionState.FAILED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduled();
        assertTrue(stateMachine.transitionToAborted());
        assertState(stateMachine, StageExecutionState.ABORTED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduled();
        assertTrue(stateMachine.transitionToCanceled());
        assertState(stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testRunning()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageExecutionState.RUNNING);

        assertFalse(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageExecutionState.RUNNING);

        assertFalse(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageExecutionState.RUNNING);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, StageExecutionState.RUNNING);

        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageExecutionState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, StageExecutionState.FAILED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToAborted());
        assertState(stateMachine, StageExecutionState.ABORTED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToCanceled());
        assertState(stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testFinished()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();

        assertTrue(stateMachine.transitionToFinished());
        assertFinalState(stateMachine, StageExecutionState.FINISHED);
    }

    @Test
    public void testFailed()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();

        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertFinalState(stateMachine, StageExecutionState.FAILED);
    }

    @Test
    public void testAborted()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();

        assertTrue(stateMachine.transitionToAborted());
        assertFinalState(stateMachine, StageExecutionState.ABORTED);
    }

    @Test
    public void testCanceled()
    {
        StageExecutionStateMachine stateMachine = createStageStateMachine();

        assertTrue(stateMachine.transitionToCanceled());
        assertFinalState(stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testTaskUpdateTraceIsAttachedToQueryTrace()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        StageExecutionStateMachine stateMachine = new StageExecutionStateMachine(
                STAGE_ID,
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);

        long startTimeNanos = System.nanoTime();
        stateMachine.recordRoundTripTime(123, startTimeNanos, startTimeNanos + 123, true);
        queryTracer.finishQueryTrace(false);

        RuntimeStats stageRuntimeStats = stateMachine.getStageExecutionInfo(ImmutableList::of, 0, 0).getStats().getRuntimeStats();
        assertEquals(stageRuntimeStats.getMetric(TASK_UPDATE_ROUND_TRIP_TIME).getSum(), 123);
        RuntimeMetricEvent updateEvent = queryRuntimeStats.getMetric(STAGE_TRACE_PREFIX + TASK_UPDATE_ROUND_TRIP_TIME).getEvents().get(0);
        RuntimeMetricEvent queryEvent = queryRuntimeStats.getMetric(QUERY_TRACE_TIME_NANOS).getEvents().get(0);
        assertEquals(updateEvent.getDurationNanos(), 123);
        assertEquals(updateEvent.getParentSpanId(), queryEvent.getSpanId());
        assertEquals(updateEvent.getStartThreadId(), -1);
        assertNull(updateEvent.getStartThreadName());
        assertTrue(updateEvent.isFailed());
    }

    @Test
    public void testTraceGroupsStageExecutionAttempts()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        StageExecutionStateMachine firstAttempt = new StageExecutionStateMachine(
                STAGE_ID,
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);
        StageExecutionStateMachine secondAttempt = new StageExecutionStateMachine(
                new StageExecutionId(STAGE_ID.getStageId(), 1),
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);
        long startTimeNanos = System.nanoTime();

        firstAttempt.recordRoundTripTime(1, startTimeNanos, startTimeNanos + 1, false);
        secondAttempt.recordRoundTripTime(1, startTimeNanos, startTimeNanos + 1, false);
        queryTracer.finishQueryTrace(false);

        assertEquals(queryRuntimeStats.getMetric(STAGE_TRACE_PREFIX + TASK_UPDATE_ROUND_TRIP_TIME).getEvents().size(), 2);
    }

    @Test
    public void testStageLifecycleTrace()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        StageExecutionStateMachine stateMachine = new StageExecutionStateMachine(
                STAGE_ID,
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);

        assertTrue(stateMachine.transitionToScheduling());
        assertFalse(stateMachine.transitionToScheduling());
        assertTrue(stateMachine.transitionToFinishedTaskScheduling());
        assertTrue(stateMachine.transitionToSchedulingSplits());
        assertTrue(stateMachine.transitionToScheduled());
        assertTrue(stateMachine.transitionToRunning());
        assertFalse(stateMachine.transitionToRunning());
        assertTrue(stateMachine.transitionToFinished());
        assertFalse(stateMachine.transitionToCanceled());
        queryTracer.finishQueryTrace(false);

        RuntimeMetricEvent queryEvent = getOnlyTraceEvent(queryRuntimeStats, QUERY_TRACE_TIME_NANOS);
        List<RuntimeMetricEvent> stageEvents = ImmutableList.of(
                getOnlyTraceEvent(queryRuntimeStats, STAGE_PLANNED_TRACE),
                getOnlyTraceEvent(queryRuntimeStats, STAGE_SCHEDULING_TRACE),
                getOnlyTraceEvent(queryRuntimeStats, STAGE_FINISHED_TASK_SCHEDULING_TRACE),
                getOnlyTraceEvent(queryRuntimeStats, STAGE_SCHEDULING_SPLITS_TRACE),
                getOnlyTraceEvent(queryRuntimeStats, STAGE_SCHEDULED_TRACE),
                getOnlyTraceEvent(queryRuntimeStats, STAGE_RUNNING_TRACE),
                getOnlyTraceEvent(queryRuntimeStats, STAGE_FINISHED_TRACE));
        for (RuntimeMetricEvent event : stageEvents) {
            assertEquals(event.getParentSpanId(), queryEvent.getSpanId());
            assertFalse(event.isFailed());
        }
        assertTrue(stageEvents.get(0).getStartTimeNanos() >= queryEvent.getStartTimeNanos());
        for (int index = 1; index < stageEvents.size(); index++) {
            assertEquals(stageEvents.get(index - 1).getEndTimeNanos(), stageEvents.get(index).getStartTimeNanos());
        }
        assertEquals(stageEvents.get(stageEvents.size() - 1).getDurationNanos(), 0);
        assertTrue(stageEvents.get(stageEvents.size() - 1).getEndTimeNanos() <= queryEvent.getEndTimeNanos());
    }

    @Test
    public void testTerminalStageTraceIsRecordedBeforeStateListeners()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        ExecutorService directExecutor = newDirectExecutorService();
        try {
            StageExecutionStateMachine stateMachine = new StageExecutionStateMachine(
                    STAGE_ID,
                    directExecutor,
                    new SplitSchedulerStats(),
                    false,
                    queryTracer);
            assertTrue(stateMachine.transitionToRunning());
            stateMachine.addStateChangeListener(state -> {
                if (state.isDone()) {
                    queryTracer.finishQueryTrace(false);
                }
            });

            assertTrue(stateMachine.transitionToFinished());
        }
        finally {
            directExecutor.shutdownNow();
        }

        RuntimeMetricEvent queryEvent = getOnlyTraceEvent(queryRuntimeStats, QUERY_TRACE_TIME_NANOS);
        RuntimeMetricEvent runningEvent = getOnlyTraceEvent(queryRuntimeStats, STAGE_RUNNING_TRACE);
        RuntimeMetricEvent finishedEvent = getOnlyTraceEvent(queryRuntimeStats, STAGE_FINISHED_TRACE);
        assertTrue(runningEvent.getEndTimeNanos() <= queryEvent.getEndTimeNanos());
        assertTrue(finishedEvent.getEndTimeNanos() <= queryEvent.getEndTimeNanos());
    }

    @Test
    public void testStageLifecycleTraceCapturesTransitionThreads()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        long constructionThreadId = Thread.currentThread().getId();
        String constructionThreadName = Thread.currentThread().getName();
        StageExecutionStateMachine stateMachine = new StageExecutionStateMachine(
                STAGE_ID,
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);
        ExecutorService transitionExecutor = newSingleThreadExecutor(runnable -> new Thread(runnable, "stage-transition-thread"));

        try {
            CompletableFuture.runAsync(() -> assertTrue(stateMachine.transitionToScheduling()), transitionExecutor).join();
            assertTrue(stateMachine.transitionToFinished());
        }
        finally {
            transitionExecutor.shutdownNow();
        }
        queryTracer.finishQueryTrace(false);

        RuntimeMetricEvent plannedEvent = getOnlyTraceEvent(queryRuntimeStats, STAGE_PLANNED_TRACE);
        RuntimeMetricEvent schedulingEvent = getOnlyTraceEvent(queryRuntimeStats, STAGE_SCHEDULING_TRACE);
        RuntimeMetricEvent finishedEvent = getOnlyTraceEvent(queryRuntimeStats, STAGE_FINISHED_TRACE);
        assertEquals(plannedEvent.getStartThreadId(), constructionThreadId);
        assertEquals(plannedEvent.getStartThreadName(), constructionThreadName);
        assertEquals(plannedEvent.getEndThreadName(), "stage-transition-thread");
        assertEquals(schedulingEvent.getStartThreadName(), "stage-transition-thread");
        assertEquals(schedulingEvent.getEndThreadId(), constructionThreadId);
        assertEquals(schedulingEvent.getEndThreadName(), constructionThreadName);
        assertEquals(finishedEvent.getStartThreadId(), constructionThreadId);
        assertEquals(finishedEvent.getEndThreadId(), constructionThreadId);
        assertEquals(finishedEvent.getStartThreadName(), constructionThreadName);
        assertEquals(finishedEvent.getEndThreadName(), constructionThreadName);
    }

    @Test
    public void testFailedStageLifecycleTrace()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        StageExecutionStateMachine stateMachine = new StageExecutionStateMachine(
                STAGE_ID,
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);

        assertTrue(stateMachine.transitionToScheduling());
        assertTrue(stateMachine.transitionToRunning());
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        queryTracer.finishQueryTrace(false);

        assertFalse(getOnlyTraceEvent(queryRuntimeStats, STAGE_PLANNED_TRACE).isFailed());
        assertFalse(getOnlyTraceEvent(queryRuntimeStats, STAGE_SCHEDULING_TRACE).isFailed());
        assertTrue(getOnlyTraceEvent(queryRuntimeStats, STAGE_RUNNING_TRACE).isFailed());
        assertTrue(getOnlyTraceEvent(queryRuntimeStats, STAGE_FAILED_TRACE).isFailed());
    }

    @Test
    public void testDirectStageRunningTrace()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        StageExecutionStateMachine stateMachine = new StageExecutionStateMachine(
                STAGE_ID,
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);

        assertTrue(stateMachine.transitionToRunning());
        assertTrue(stateMachine.transitionToFinished());
        queryTracer.finishQueryTrace(false);

        assertFalse(getOnlyTraceEvent(queryRuntimeStats, STAGE_PLANNED_TRACE).isFailed());
        assertNull(queryRuntimeStats.getMetric(STAGE_SCHEDULING_TRACE));
        assertNull(queryRuntimeStats.getMetric(STAGE_FINISHED_TASK_SCHEDULING_TRACE));
        assertNull(queryRuntimeStats.getMetric(STAGE_SCHEDULING_SPLITS_TRACE));
        assertNull(queryRuntimeStats.getMetric(STAGE_SCHEDULED_TRACE));
        assertFalse(getOnlyTraceEvent(queryRuntimeStats, STAGE_RUNNING_TRACE).isFailed());
        assertEquals(getOnlyTraceEvent(queryRuntimeStats, STAGE_FINISHED_TRACE).getDurationNanos(), 0);
    }

    @Test
    public void testDirectStageScheduledTrace()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        StageExecutionStateMachine stateMachine = new StageExecutionStateMachine(
                STAGE_ID,
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);

        assertTrue(stateMachine.transitionToScheduled());
        assertTrue(stateMachine.transitionToFinished());
        queryTracer.finishQueryTrace(false);

        assertFalse(getOnlyTraceEvent(queryRuntimeStats, STAGE_PLANNED_TRACE).isFailed());
        assertNull(queryRuntimeStats.getMetric(STAGE_SCHEDULING_TRACE));
        assertNull(queryRuntimeStats.getMetric(STAGE_FINISHED_TASK_SCHEDULING_TRACE));
        assertNull(queryRuntimeStats.getMetric(STAGE_SCHEDULING_SPLITS_TRACE));
        assertFalse(getOnlyTraceEvent(queryRuntimeStats, STAGE_SCHEDULED_TRACE).isFailed());
        assertNull(queryRuntimeStats.getMetric(STAGE_RUNNING_TRACE));
        assertEquals(getOnlyTraceEvent(queryRuntimeStats, STAGE_FINISHED_TRACE).getDurationNanos(), 0);
    }

    @Test
    public void testStageFailureWhileSchedulingTrace()
    {
        RuntimeStats queryRuntimeStats = new RuntimeStats();
        QueryTracer queryTracer = queryRuntimeStats.startQueryTrace();
        StageExecutionStateMachine stateMachine = new StageExecutionStateMachine(
                STAGE_ID,
                executor,
                new SplitSchedulerStats(),
                false,
                queryTracer);

        assertTrue(stateMachine.transitionToScheduling());
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        queryTracer.finishQueryTrace(false);

        assertFalse(getOnlyTraceEvent(queryRuntimeStats, STAGE_PLANNED_TRACE).isFailed());
        assertTrue(getOnlyTraceEvent(queryRuntimeStats, STAGE_SCHEDULING_TRACE).isFailed());
        assertNull(queryRuntimeStats.getMetric(STAGE_RUNNING_TRACE));
        assertTrue(getOnlyTraceEvent(queryRuntimeStats, STAGE_FAILED_TRACE).isFailed());
    }

    @Test
    public void testTimestampOverloadsDelegateToLegacyMethods()
    {
        TestingSchedulerStatsTracker tracker = new TestingSchedulerStatsTracker();

        tracker.recordTaskUpdateDeliveredTime(10, 25);
        tracker.recordRoundTripTime(17, 10, 27, true);
        tracker.recordStartWaitForEventLoop(15, 20);

        assertEquals(tracker.taskUpdateDeliveredTimeNanos, 15);
        assertEquals(tracker.roundTripTimeNanos, 17);
        assertEquals(tracker.startWaitForEventLoopNanos, 5);
    }

    private static final class TestingSchedulerStatsTracker
            implements SchedulerStatsTracker
    {
        private long taskUpdateDeliveredTimeNanos;
        private long roundTripTimeNanos;
        private long startWaitForEventLoopNanos;

        @Override
        public void recordTaskUpdateDeliveredTime(long nanos)
        {
            taskUpdateDeliveredTimeNanos = nanos;
        }

        @Override
        public void recordDeliveredUpdates(int updates) {}

        @Override
        public void recordRoundTripTime(long nanos)
        {
            roundTripTimeNanos = nanos;
        }

        @Override
        public void recordStartWaitForEventLoop(long nanos)
        {
            startWaitForEventLoopNanos = nanos;
        }

        @Override
        public void recordTaskUpdateSerializedCpuTime(long nanos) {}

        @Override
        public void recordTaskPlanSerializedCpuTime(long nanos) {}

        @Override
        public void recordEventLoopMethodExecutionCpuTime(long nanos) {}
    }

    private static void assertFinalState(StageExecutionStateMachine stateMachine, StageExecutionState expectedState)
    {
        assertTrue(expectedState.isDone());

        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToScheduling());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToScheduled());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToFinished());
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, expectedState);

        assertFalse(stateMachine.transitionToAborted());
        assertState(stateMachine, expectedState);

        // attempt to fail with another exception, which will fail
        assertFalse(stateMachine.transitionToFailed(new IOException("failure after finish")));
        assertState(stateMachine, expectedState);
    }

    private static void assertState(StageExecutionStateMachine stateMachine, StageExecutionState expectedState)
    {
        assertEquals(stateMachine.getStageExecutionId(), STAGE_ID);

        StageExecutionInfo stageExecutionInfo = stateMachine.getStageExecutionInfo(ImmutableList::of, 0, 0);
        assertEquals(stageExecutionInfo.getTasks(), ImmutableList.of());

        assertEquals(stateMachine.getState(), expectedState);
        assertEquals(stageExecutionInfo.getState(), expectedState);

        if (expectedState == StageExecutionState.FAILED) {
            ExecutionFailureInfo failure = stageExecutionInfo.getFailureCause().get();
            assertEquals(failure.getMessage(), FAILED_CAUSE.getMessage());
            assertEquals(failure.getType(), FAILED_CAUSE.getClass().getName());
        }
        else {
            assertFalse(stageExecutionInfo.getFailureCause().isPresent());
        }
    }

    private static RuntimeMetricEvent getOnlyTraceEvent(RuntimeStats runtimeStats, String name)
    {
        RuntimeMetric metric = requireNonNull(runtimeStats.getMetric(name), name + " is missing");
        assertEquals(metric.getEvents().size(), 1);
        return metric.getEvents().get(0);
    }

    private StageExecutionStateMachine createStageStateMachine()
    {
        return new StageExecutionStateMachine(STAGE_ID, executor, new SplitSchedulerStats(), false);
    }
}
