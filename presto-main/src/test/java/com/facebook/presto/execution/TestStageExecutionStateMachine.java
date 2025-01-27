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

import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStageExecutionStateMachine
{
    private static final StageExecutionId STAGE_ID = new StageExecutionId(new StageId(new QueryId("query"), 0), 0);
    private static final SQLException FAILED_CAUSE = new SQLException("FAILED");

    private final EventLoopGroup eventLoopGroup = new SafeEventLoopGroup(Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("test-event-loop-%s").setDaemon(true).build());

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        eventLoopGroup.shutdownGracefully();
    }

    @Test
    public void testBasicStateChanges()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertState(stageEventLoop, stateMachine, StageExecutionState.PLANNED);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULING);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULED);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertState(stageEventLoop, stateMachine, StageExecutionState.RUNNING);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFinished);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FINISHED);
    }

    @Test
    public void testPlanned()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertState(stageEventLoop, stateMachine, StageExecutionState.PLANNED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULING);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertState(stageEventLoop, stateMachine, StageExecutionState.RUNNING);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFinished);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FINISHED);

        stateMachine = createStageStateMachine(stageEventLoop);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFailed, FAILED_CAUSE);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FAILED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToAborted);
        assertState(stageEventLoop, stateMachine, StageExecutionState.ABORTED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToCanceled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testScheduling()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULING);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULING);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertState(stageEventLoop, stateMachine, StageExecutionState.RUNNING);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFinished);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FINISHED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFailed, FAILED_CAUSE);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FAILED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToAborted);
        assertState(stageEventLoop, stateMachine, StageExecutionState.ABORTED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToCanceled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testScheduled()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULED);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULED);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.SCHEDULED);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertState(stageEventLoop, stateMachine, StageExecutionState.RUNNING);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFinished);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FINISHED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFailed, FAILED_CAUSE);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FAILED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToAborted);
        assertState(stageEventLoop, stateMachine, StageExecutionState.ABORTED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToCanceled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testRunning()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertState(stageEventLoop, stateMachine, StageExecutionState.RUNNING);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertState(stageEventLoop, stateMachine, StageExecutionState.RUNNING);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.RUNNING);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertState(stageEventLoop, stateMachine, StageExecutionState.RUNNING);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFinished);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FINISHED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFailed, FAILED_CAUSE);
        assertState(stageEventLoop, stateMachine, StageExecutionState.FAILED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToAborted);
        assertState(stageEventLoop, stateMachine, StageExecutionState.ABORTED);

        stateMachine = createStageStateMachine(stageEventLoop);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToCanceled);
        assertState(stageEventLoop, stateMachine, StageExecutionState.CANCELED);
    }

    @Test
    public void testFinished()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFinished);
        assertFinalState(stageEventLoop, stateMachine, StageExecutionState.FINISHED);
    }

    @Test
    public void testFailed()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToFailed, FAILED_CAUSE);
        assertFinalState(stageEventLoop, stateMachine, StageExecutionState.FAILED);
    }

    @Test
    public void testAborted()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToAborted);
        assertFinalState(stageEventLoop, stateMachine, StageExecutionState.ABORTED);
    }

    @Test
    public void testCanceled()
    {
        SafeEventLoopGroup.SafeEventLoop stageEventLoop = (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next();
        StageExecutionStateMachine stateMachine = createStageStateMachine(stageEventLoop);

        assertTrueOnEventLoop(stageEventLoop, stateMachine::transitionToCanceled);
        assertFinalState(stageEventLoop, stateMachine, StageExecutionState.CANCELED);
    }

    private static void assertFinalState(SafeEventLoopGroup.SafeEventLoop stageEventLoop, StageExecutionStateMachine stateMachine, StageExecutionState expectedState)
    {
        assertTrue(expectedState.isDone());

        assertState(stageEventLoop, stateMachine, expectedState);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToScheduling);
        assertState(stageEventLoop, stateMachine, expectedState);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToScheduled);
        assertState(stageEventLoop, stateMachine, expectedState);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToRunning);
        assertState(stageEventLoop, stateMachine, expectedState);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToFinished);
        assertState(stageEventLoop, stateMachine, expectedState);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToFailed, FAILED_CAUSE);
        assertState(stageEventLoop, stateMachine, expectedState);

        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToAborted);
        assertState(stageEventLoop, stateMachine, expectedState);

        // attempt to fail with another exception, which will fail
        assertFalseOnEventLoop(stageEventLoop, stateMachine::transitionToFailed, new IOException("failure after finish"));
        assertState(stageEventLoop, stateMachine, expectedState);
    }

    private static void assertTrueOnEventLoop(SafeEventLoopGroup.SafeEventLoop stageEventLoop, Supplier<Boolean> task)
    {
        SettableFuture<Boolean> future = SettableFuture.create();
        stageEventLoop.execute(task, future::set, null);
        assertTrue(getFutureValue(future));
    }

    private static void assertTrueOnEventLoop(SafeEventLoopGroup.SafeEventLoop stageEventLoop, Function<Throwable, Boolean> task, Throwable t)
    {
        SettableFuture<Boolean> future = SettableFuture.create();
        stageEventLoop.execute(() -> {
            future.set(task.apply(t));
        });
        assertTrue(getFutureValue(future));
    }

    private static void assertFalseOnEventLoop(SafeEventLoopGroup.SafeEventLoop stageEventLoop, Supplier<Boolean> task)
    {
        SettableFuture<Boolean> future = SettableFuture.create();
        stageEventLoop.execute(task, future::set, null);
        assertFalse(getFutureValue(future));
    }

    private static void assertFalseOnEventLoop(SafeEventLoopGroup.SafeEventLoop stageEventLoop, Function<Throwable, Boolean> task, Throwable t)
    {
        SettableFuture<Boolean> future = SettableFuture.create();
        stageEventLoop.execute(() -> {
            future.set(task.apply(t));
        });
        assertFalse(getFutureValue(future));
    }

    private static void assertState(SafeEventLoopGroup.SafeEventLoop stageEventloop, StageExecutionStateMachine stateMachine, StageExecutionState expectedState)
    {
        assertEquals(stateMachine.getStageExecutionId(), STAGE_ID);

        StageExecutionInfo stageExecutionInfo = stateMachine.getStageExecutionInfo(ImmutableList::of, 0, 0);
        assertEquals(stageExecutionInfo.getTasks(), ImmutableList.of());

        SettableFuture<StageExecutionState> future = SettableFuture.create();
        stageEventloop.execute(stateMachine::getFutureState, future::set, null);
        assertEquals(getFutureValue(future), expectedState);
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

    private StageExecutionStateMachine createStageStateMachine(SafeEventLoopGroup.SafeEventLoop stageEventLoop)
    {
        return new StageExecutionStateMachine(STAGE_ID, stageEventLoop, new SplitSchedulerStats(), false);
    }
}
