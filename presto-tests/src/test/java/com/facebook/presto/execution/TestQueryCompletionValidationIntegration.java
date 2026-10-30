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
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestQueryCompletionValidationIntegration
{
    @Test(timeOut = 120000)
    public void testCoordinatorCompletionAppliesEnabledValidators()
            throws Exception
    {
        AtomicBoolean enabled = new AtomicBoolean();
        AtomicReference<Runnable> validationAction = new AtomicReference<>(() -> {});
        AtomicInteger checks = new AtomicInteger();
        AtomicInteger canceledChecks = new AtomicInteger();
        AtomicReference<QueryId> queryId = new AtomicReference<>();
        AtomicReference<BlockingState> blocking = new AtomicReference<>();
        AtomicReference<QueryManager> queryManager = new AtomicReference<>();
        QueryCompletionValidator validator = new QueryCompletionValidator()
        {
            @Override
            public boolean isEnabled(Session session)
            {
                return enabled.get();
            }

            @Override
            public void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo)
            {
                assertTrue(stageInfo.isFinal());
                assertTrue(stageInfo.getState() == StageExecutionState.FINISHED || stageInfo.getState() == StageExecutionState.CANCELED);
                if (stageInfo.getState() == StageExecutionState.CANCELED) {
                    canceledChecks.incrementAndGet();
                }
                queryId.set(session.getQueryId());
                QueryState state = queryManager.get().getQueryState(session.getQueryId());
                assertTrue(!state.isDone() && state != QueryState.FINISHING, "Stage validation must precede finishing");
                checks.incrementAndGet();
                if (fragment.getId().getId() == 0 && blocking.get() != null) {
                    blocking.get().release.countDown();
                }
                validationAction.get().run();
            }
        };
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setNodeCount(3)
                .amendSession(builder -> builder.setSystemProperty(QUERY_MAX_RUN_TIME, "30s"))
                .setExtraModules(ImmutableList.of(binder -> newSetBinder(binder, QueryCompletionValidator.class).addBinding().toInstance(validator)))
                .build()) {
            queryManager.set(runner.getCoordinator().getQueryManager());
            runner.execute("SELECT 1");
            assertEquals(checks.get(), 0);
            enabled.set(true);
            runner.installPlugin(new TpchPlugin());
            runner.createCatalog("tpch", "tpch", ImmutableMap.of());
            runner.execute("SELECT sum(orderkey) FROM tpch.tiny.orders");
            assertTrue(checks.get() > 1, "The distributed query must validate more than one stage");
            runner.installPlugin(new Plugin()
            {
                @Override
                public Set<Class<?>> getFunctions()
                {
                    return ImmutableSet.of(BlockingPredicate.class);
                }
            });
            BlockingState blocked = new BlockingState();
            blocking.set(blocked);
            BlockingPredicate.STATE.set(blocked);
            try {
                assertEquals(runner.execute("SELECT orderkey FROM tpch.tiny.orders WHERE completion_validation_block(orderkey) LIMIT 1").getRowCount(), 1);
                assertTrue(canceledChecks.get() > 0, "A canceled source must be validated");
            }
            finally {
                blocked.release.countDown();
                blocking.set(null);
            }
            int successfulChecks = checks.get();
            validationAction.set(() -> {
                throw new PrestoException(GENERIC_USER_ERROR, "completion-rejected");
            });
            RuntimeException failure = expectThrows(RuntimeException.class, () -> runner.execute("SELECT 1"));
            assertTrue(failure.getMessage().contains("completion-rejected"));
            assertEquals(checks.get(), successfulChecks + 1);
            assertEquals(queryManager.get().getFullQueryInfo(queryId.get()).getErrorCode(), GENERIC_USER_ERROR.toErrorCode());
            assertEquals(queryManager.get().getFullQueryInfo(queryId.get()).getQueryStats().getRuntimeStats()
                    .getMetric("queryCompletionValidationFailures").getSum(), 1);
            validationAction.set(() -> {
                throw new IllegalStateException("unexpected-validator-failure");
            });
            RuntimeException unexpected = expectThrows(RuntimeException.class, () -> runner.execute("SELECT 1"));
            assertTrue(unexpected.getMessage().contains("unexpected-validator-failure"));
            assertEquals(queryManager.get().getFullQueryInfo(queryId.get()).getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
            validationAction.set(() -> {
                throw new IllegalStateException("wrapped-validator-failure", new PrestoException(GENERIC_USER_ERROR, "validator-cause"));
            });
            RuntimeException wrapped = expectThrows(RuntimeException.class, () -> runner.execute("SELECT 1"));
            assertTrue(wrapped.getMessage().contains("wrapped-validator-failure"));
            assertEquals(queryManager.get().getFullQueryInfo(queryId.get()).getErrorCode(), GENERIC_USER_ERROR.toErrorCode());
            validationAction.set(() -> {});
            RuntimeException executionFailure = expectThrows(RuntimeException.class,
                    () -> runner.execute("SELECT sum(10 / (orderkey - 1)) FROM tpch.tiny.orders"));
            assertTrue(executionFailure.getMessage().contains("by zero"), executionFailure.getMessage());
        }
        finally {
            BlockingState state = BlockingPredicate.STATE.getAndSet(null);
            if (state != null) {
                state.release.countDown();
            }
        }
    }

    @Test(timeOut = 120000)
    public void testWaitsForTheRealStageSetBeforeFinishing()
            throws Exception
    {
        CountDownLatch childEntered = new CountDownLatch(1);
        CountDownLatch rootEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<QueryId> queryId = new AtomicReference<>();
        QueryCompletionValidator validator = new QueryCompletionValidator()
        {
            @Override
            public boolean isEnabled(Session session)
            {
                return session.getSource().map("completion-validation"::equals).orElse(false);
            }

            @Override
            public void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo)
            {
                queryId.set(session.getQueryId());
                assertTrue(stageInfo.isFinal());
                if (fragment.getId().getId() == 0) {
                    rootEntered.countDown();
                    return;
                }
                childEntered.countDown();
                try {
                    assertTrue(release.await(30, SECONDS), "Child validation was not released");
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                throw new PrestoException(GENERIC_USER_ERROR, "slow-stage-rejected");
            }
        };
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setNodeCount(3)
                .setExtraModules(ImmutableList.of(binder -> newSetBinder(binder, QueryCompletionValidator.class).addBinding().toInstance(validator)))
                .build()) {
            runner.installPlugin(new TpchPlugin());
            runner.createCatalog("tpch", "tpch", ImmutableMap.of());
            Session session = Session.builder(runner.getDefaultSession())
                    .setSource("completion-validation")
                    .setSystemProperty(QUERY_MAX_RUN_TIME, "30s")
                    .build();
            CompletableFuture<?> query = CompletableFuture.runAsync(() -> runner.execute(session, "SELECT sum(orderkey) FROM tpch.tiny.orders"));
            try {
                assertTrue(childEntered.await(30, SECONDS), "No child validation callback");
                assertTrue(rootEntered.await(30, SECONDS), "No output validation callback");
                QueryInfo info = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId.get());
                assertEquals(info.getOutputStage().get().getLatestAttemptExecutionInfo().getState(), StageExecutionState.FINISHED);
                assertTrue(!info.getState().isDone() && info.getState() != QueryState.FINISHING);
                expectThrows(TimeoutException.class, () -> query.get(1, SECONDS));
                release.countDown();
                CompletionException failure = expectThrows(CompletionException.class, query::join);
                assertTrue(failure.getMessage().contains("slow-stage-rejected"));
                assertEquals(runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId.get()).getErrorCode(), GENERIC_USER_ERROR.toErrorCode());
            }
            finally {
                release.countDown();
                if (!query.isDone() && queryId.get() != null) {
                    runner.getCoordinator().getQueryManager().cancelQuery(queryId.get());
                }
            }
        }
    }

    private static final class BlockingState
    {
        private final AtomicBoolean firstCall = new AtomicBoolean();
        private final CountDownLatch release = new CountDownLatch(1);
    }

    public static final class BlockingPredicate
    {
        private static final AtomicReference<BlockingState> STATE = new AtomicReference<>();

        @ScalarFunction(value = "completion_validation_block", deterministic = false)
        @SqlType(BOOLEAN)
        public static boolean block(@SqlType(BIGINT) long value)
                throws InterruptedException
        {
            BlockingState state = STATE.get();
            if (state.firstCall.compareAndSet(false, true)) {
                assertTrue(state.release.await(30, SECONDS), "Output stage did not release the source");
            }
            return value > 0;
        }
    }
}
