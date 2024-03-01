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
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStageExecutionStateMachine
{
    private static final StageExecutionId STAGE_ID = new StageExecutionId(new StageId(new QueryId("query"), 0), 0);
    private static final SQLException FAILED_CAUSE = new SQLException("FAILED");

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

    private StageExecutionStateMachine createStageStateMachine()
    {
        return new StageExecutionStateMachine(STAGE_ID, executor, new SplitSchedulerStats(), false);
    }
}
