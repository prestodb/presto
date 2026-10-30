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
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.stats.Distribution;
import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.QueryCompletionValidator;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestQueryCompletionValidation
{
    private static final StageExecutionId ROOT = StageExecutionId.valueOf("query.0.0");
    private static final StageExecutionId SOURCE = StageExecutionId.valueOf("query.1.0");

    @Test
    public void testValidatesEachStageBeforeOutputFinishes()
    {
        AtomicInteger calls = new AtomicInteger();
        QueryCompletionValidation gate = gate(true, calls);
        gate.validateStage(SOURCE, null, info(SOURCE, StageExecutionState.FINISHED));
        assertEquals(calls.get(), 1);
        assertFalse(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
        gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED));
        assertEquals(calls.get(), 2);
        assertFalse(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
        gate.markOutputFinished();
        assertTrue(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
    }

    @Test
    public void testWaitsForDelayedFinalStageCallback()
    {
        QueryCompletionValidation gate = gate(true, new AtomicInteger());
        gate.markOutputFinished();
        gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED));
        assertFalse(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
        gate.validateStage(SOURCE, null, info(SOURCE, StageExecutionState.FINISHED));
        assertTrue(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
    }

    @Test
    public void testCanceledChildFinalStatisticsStillAllowQueryCompletion()
    {
        AtomicInteger calls = new AtomicInteger();
        QueryCompletionValidation gate = gate(true, calls);
        gate.markOutputFinished();
        gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED));
        assertFalse(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
        gate.validateStage(SOURCE, null, info(SOURCE, StageExecutionState.CANCELED));
        assertEquals(calls.get(), 2);
        assertTrue(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
    }

    @Test
    public void testUnrelatedStageCannotSatisfyRequiredStage()
    {
        QueryCompletionValidation gate = gate(true, new AtomicInteger());
        StageExecutionId required = StageExecutionId.valueOf("query.2.0");
        gate.markOutputFinished();
        gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED));
        gate.validateStage(SOURCE, null, info(SOURCE, StageExecutionState.FINISHED));
        assertFalse(gate.tryFinish(ImmutableSet.of(ROOT, required)));
        gate.validateStage(required, null, info(required, StageExecutionState.FINISHED));
        assertTrue(gate.tryFinish(ImmutableSet.of(ROOT, required)));
    }

    @DataProvider
    public Object[][] failedStageStates()
    {
        return new Object[][] {{StageExecutionState.FAILED}, {StageExecutionState.ABORTED}};
    }

    @Test(dataProvider = "failedStageStates")
    public void testFailureFinalInformationSkipsValidatorsAndCannotFinish(StageExecutionState state)
    {
        AtomicInteger calls = new AtomicInteger();
        QueryCompletionValidation gate = gate(true, calls);
        gate.markOutputFinished();
        gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED));
        gate.validateStage(SOURCE, null, info(SOURCE, state));
        assertEquals(calls.get(), 1);
        assertFalse(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
    }

    @Test
    public void testDisabledValidatorDoesNotWaitOrReadStatistics()
    {
        AtomicInteger calls = new AtomicInteger();
        QueryCompletionValidation gate = gate(false, calls);
        assertFalse(gate.isEnabled());
        gate.validateStage(ROOT, null, null);
        gate.markOutputFinished();
        assertTrue(gate.tryFinish(ImmutableSet.of(ROOT, SOURCE)));
        assertEquals(calls.get(), 0);
    }

    @Test
    public void testAllEnabledValidatorsRun()
    {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        AtomicInteger disabled = new AtomicInteger();
        QueryCompletionValidation gate = new QueryCompletionValidation(TEST_SESSION, ImmutableSet.of(
                validator(true, first::incrementAndGet),
                validator(false, disabled::incrementAndGet),
                validator(true, second::incrementAndGet)));
        gate.markOutputFinished();
        gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED));
        assertEquals(first.get(), 1);
        assertEquals(second.get(), 1);
        assertEquals(disabled.get(), 0);
        assertTrue(gate.tryFinish(ImmutableSet.of(ROOT)));
    }

    @Test
    public void testRejectionByLaterValidatorPreventsFinishing()
    {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        QueryCompletionValidation gate = new QueryCompletionValidation(TEST_SESSION, ImmutableSet.of(
                validator(true, first::incrementAndGet),
                validator(true, () -> {
                    second.incrementAndGet();
                    throw new IllegalStateException("second validator rejected");
                })));
        gate.markOutputFinished();
        IllegalStateException failure = expectThrows(IllegalStateException.class,
                () -> gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED)));
        assertEquals(failure.getMessage(), "second validator rejected");
        assertEquals(first.get(), 1);
        assertEquals(second.get(), 1);
        assertFalse(gate.tryFinish(ImmutableSet.of(ROOT)));
    }

    @Test(timeOut = 10000)
    public void testConcurrentFinishingNotificationsFinishOnce()
    {
        QueryCompletionValidation gate = gate(true, new AtomicInteger());
        gate.markOutputFinished();
        gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED));
        AtomicInteger finishes = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        CompletableFuture<?> first = CompletableFuture.runAsync(() -> {
            ready.countDown();
            awaitUninterruptibly(start);
            if (gate.tryFinish(ImmutableSet.of(ROOT))) {
                finishes.incrementAndGet();
            }
        });
        CompletableFuture<?> second = CompletableFuture.runAsync(() -> {
            ready.countDown();
            awaitUninterruptibly(start);
            if (gate.tryFinish(ImmutableSet.of(ROOT))) {
                finishes.incrementAndGet();
            }
        });
        awaitUninterruptibly(ready);
        start.countDown();
        CompletableFuture.allOf(first, second).join();
        assertEquals(finishes.get(), 1);
    }

    @Test(timeOut = 10000)
    public void testQueryCannotFinishWhileStageValidationIsRunning()
            throws Exception
    {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        QueryCompletionValidation gate = new QueryCompletionValidation(TEST_SESSION, ImmutableSet.of(new QueryCompletionValidator()
        {
            @Override
            public boolean isEnabled(Session session)
            {
                return true;
            }

            @Override
            public void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo)
            {
                entered.countDown();
                try {
                    release.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }));
        gate.markOutputFinished();
        CompletableFuture<?> validation = CompletableFuture.runAsync(() -> gate.validateStage(ROOT, null, info(ROOT, StageExecutionState.FINISHED)));
        try {
            entered.await();
            assertFalse(gate.tryFinish(ImmutableSet.of(ROOT)));
        }
        finally {
            release.countDown();
        }
        validation.join();
        assertTrue(gate.tryFinish(ImmutableSet.of(ROOT)));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "bad outcomes")
    public void testStageFailurePropagatesBeforeOutputFinishes()
    {
        QueryCompletionValidation gate = new QueryCompletionValidation(TEST_SESSION, ImmutableSet.of(new QueryCompletionValidator()
        {
            @Override
            public boolean isEnabled(Session session)
            {
                return true;
            }

            @Override
            public void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo)
            {
                throw new IllegalStateException("bad outcomes");
            }
        }));
        gate.validateStage(SOURCE, null, info(SOURCE, StageExecutionState.FINISHED));
    }

    private static StageExecutionInfo info(StageExecutionId id, StageExecutionState state)
    {
        return StageExecutionInfo.create(id, state, Optional.empty(), ImmutableList.of(),
                0, new Distribution().snapshot(), new RuntimeStats(), 0, 0, 0, 0);
    }

    private static QueryCompletionValidation gate(boolean enabled, AtomicInteger calls)
    {
        return new QueryCompletionValidation(TEST_SESSION, ImmutableSet.of(validator(enabled, calls::incrementAndGet)));
    }

    private static QueryCompletionValidator validator(boolean enabled, Runnable validation)
    {
        return new QueryCompletionValidator()
        {
            @Override
            public boolean isEnabled(Session session)
            {
                return enabled;
            }

            @Override
            public void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo)
            {
                assertTrue(stageInfo.isFinal());
                validation.run();
            }
        };
    }
}
