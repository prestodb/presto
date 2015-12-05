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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PartitionFunctionBinding;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestStageStateMachine
{
    private static final StageId STAGE_ID = new StageId("query", "stage");
    private static final URI LOCATION = URI.create("fake://fake-stage");
    private static final PlanFragment PLAN_FRAGMENT = createValuesPlan();
    private static final SQLException FAILED_CAUSE = new SQLException("FAILED");

    private final ExecutorService executor = newCachedThreadPool();

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testBasicStateChanges()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertState(stateMachine, StageState.PLANNED);

        assertTrue(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageState.SCHEDULING);

        assertTrue(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageState.SCHEDULED);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageState.RUNNING);

        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageState.FINISHED);
    }

    @Test
    public void testPlanned()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertState(stateMachine, StageState.PLANNED);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageState.SCHEDULING);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, StageState.FAILED);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToAborted());
        assertState(stateMachine, StageState.ABORTED);

        stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToCanceled());
        assertState(stateMachine, StageState.CANCELED);
    }

    @Test
    public void testScheduling()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageState.SCHEDULING);

        assertFalse(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageState.SCHEDULING);

        assertTrue(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageState.SCHEDULED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, StageState.FAILED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToAborted());
        assertState(stateMachine, StageState.ABORTED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertTrue(stateMachine.transitionToCanceled());
        assertState(stateMachine, StageState.CANCELED);
    }

    @Test
    public void testScheduled()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageState.SCHEDULED);

        assertFalse(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageState.SCHEDULED);

        assertFalse(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageState.SCHEDULED);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduled();
        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduled();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, StageState.FAILED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduled();
        assertTrue(stateMachine.transitionToAborted());
        assertState(stateMachine, StageState.ABORTED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduled();
        assertTrue(stateMachine.transitionToCanceled());
        assertState(stateMachine, StageState.CANCELED);
    }

    @Test
    public void testRunning()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, StageState.RUNNING);

        assertFalse(stateMachine.transitionToScheduling());
        assertState(stateMachine, StageState.RUNNING);

        assertFalse(stateMachine.transitionToScheduled());
        assertState(stateMachine, StageState.RUNNING);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, StageState.RUNNING);

        assertTrue(stateMachine.transitionToFinished());
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, StageState.FAILED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToAborted());
        assertState(stateMachine, StageState.ABORTED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToCanceled());
        assertState(stateMachine, StageState.CANCELED);
    }

    @Test
    public void testFinished()
    {
        StageStateMachine stateMachine = createStageStateMachine();

        assertTrue(stateMachine.transitionToFinished());
        assertFinalState(stateMachine, StageState.FINISHED);
    }

    @Test
    public void testFailed()
    {
        StageStateMachine stateMachine = createStageStateMachine();

        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertFinalState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testAborted()
    {
        StageStateMachine stateMachine = createStageStateMachine();

        assertTrue(stateMachine.transitionToAborted());
        assertFinalState(stateMachine, StageState.ABORTED);
    }

    @Test
    public void testCanceled()
    {
        StageStateMachine stateMachine = createStageStateMachine();

        assertTrue(stateMachine.transitionToCanceled());
        assertFinalState(stateMachine, StageState.CANCELED);
    }

    private static void assertFinalState(StageStateMachine stateMachine, StageState expectedState)
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

    private static void assertState(StageStateMachine stateMachine, StageState expectedState)
    {
        assertEquals(stateMachine.getStageId(), STAGE_ID);
        assertEquals(stateMachine.getLocation(), LOCATION);
        assertSame(stateMachine.getSession(), TEST_SESSION);

        StageInfo stageInfo = stateMachine.getStageInfo(ImmutableList::of, ImmutableList::of);
        assertEquals(stageInfo.getStageId(), STAGE_ID);
        assertEquals(stageInfo.getSelf(), LOCATION);
        assertEquals(stageInfo.getSubStages(), ImmutableList.of());
        assertEquals(stageInfo.getTasks(), ImmutableList.of());
        assertEquals(stageInfo.getTypes(), ImmutableList.of(VARCHAR));
        assertSame(stageInfo.getPlan(), PLAN_FRAGMENT);

        assertEquals(stateMachine.getState(), expectedState);
        assertEquals(stageInfo.getState(), expectedState);

        if (expectedState == StageState.FAILED) {
            ExecutionFailureInfo failure = stageInfo.getFailureCause();
            assertEquals(failure.getMessage(), FAILED_CAUSE.getMessage());
            assertEquals(failure.getType(), FAILED_CAUSE.getClass().getName());
        }
        else {
            assertNull(stageInfo.getFailureCause());
        }
    }

    private StageStateMachine createStageStateMachine()
    {
        return new StageStateMachine(STAGE_ID, LOCATION, TEST_SESSION, PLAN_FRAGMENT, executor);
    }

    private static PlanFragment createValuesPlan()
    {
        Symbol symbol = new Symbol("column");
        PlanNodeId valuesNodeId = new PlanNodeId("plan");
        PlanFragment planFragment = new PlanFragment(
                new PlanFragmentId("plan"),
                new ValuesNode(valuesNodeId,
                        ImmutableList.of(symbol),
                        ImmutableList.of(ImmutableList.of(new StringLiteral("foo")))),
                ImmutableMap.<Symbol, Type>of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(valuesNodeId),
                new PartitionFunctionBinding(SINGLE_DISTRIBUTION, ImmutableList.of(symbol), ImmutableList.of()));

        return planFragment;
    }
}
