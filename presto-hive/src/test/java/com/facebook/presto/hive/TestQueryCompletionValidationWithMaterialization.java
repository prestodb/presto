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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryCompletionValidator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.CTE_PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.SystemSessionProperties.EXECUTION_POLICY;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveQueryRunner.createDatabaseMetastoreObject;
import static com.facebook.presto.hive.HiveQueryRunner.createSession;
import static com.facebook.presto.hive.HiveQueryRunner.getFileHiveMetastore;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestQueryCompletionValidationWithMaterialization
{
    @Test(timeOut = 180000)
    public void testRuntimeReplacementStagesAreValidated()
            throws Exception
    {
        Map<Integer, PlanFragment> validated = new ConcurrentHashMap<>();
        AtomicInteger rejectedStage = new AtomicInteger(-1);
        AtomicReference<QueryId> rejectedQuery = new AtomicReference<>();
        AtomicReference<QueryManager> queryManager = new AtomicReference<>();
        QueryCompletionValidator validator = new QueryCompletionValidator()
        {
            @Override
            public boolean isEnabled(Session session)
            {
                return validationEnabled(session);
            }

            @Override
            public void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo)
            {
                assertTrue(stageInfo.isFinal());
                QueryState state = queryManager.get().getQueryState(session.getQueryId());
                assertTrue(!state.isDone() && state != QueryState.FINISHING);
                validated.put(fragment.getId().getId(), fragment);
                if (fragment.getId().getId() == rejectedStage.get()) {
                    rejectedQuery.set(session.getQueryId());
                    throw new PrestoException(GENERIC_USER_ERROR, "runtime-stage-rejected");
                }
            }
        };
        try (DistributedQueryRunner runner = createRunner(validator, ImmutableList.of(TpchTable.NATION, TpchTable.SUPPLIER))) {
            queryManager.set(runner.getCoordinator().getQueryManager());
            Session session = validationSession(runner);
            String sql = "SELECT phone, regionkey FROM nation INNER JOIN supplier ON supplier.nationkey=nation.nationkey";
            QueryId queryId = runner.executeWithQueryId(session, sql).getQueryId();
            QueryInfo info = queryManager.get().getFullQueryInfo(queryId);
            assertEquals(info.getState(), QueryState.FINISHED);
            List<StageId> optimized = info.getRuntimeOptimizedStages().get();
            assertTrue(!optimized.isEmpty(), "The query must replace a stage at runtime");
            List<StageInfo> stages = StageInfo.getAllStages(info.getOutputStage());
            assertEquals(validated.size(), stages.size());
            for (StageInfo stage : stages) {
                assertSame(validated.get(stage.getStageId().getId()).getRoot(), stage.getPlan().get().getRoot());
            }
            rejectedStage.set(optimized.get(0).getId());
            RuntimeException failure = expectThrows(RuntimeException.class, () -> runner.execute(session, sql));
            assertTrue(failure.getMessage().contains("runtime-stage-rejected"));
            QueryInfo failed = queryManager.get().getFullQueryInfo(rejectedQuery.get());
            assertTrue(failed.getRuntimeOptimizedStages().get().contains(new StageId(rejectedQuery.get(), rejectedStage.get())));
            assertEquals(failed.getErrorCode(), GENERIC_USER_ERROR.toErrorCode());
        }
    }

    @Test(timeOut = 180000)
    public void testUnconsumedMaterializationIsCanceled()
            throws Exception
    {
        AtomicBoolean canceledMaterialization = new AtomicBoolean();
        Map<Integer, StageExecutionState> observedStages = new ConcurrentHashMap<>();
        BlockingCteState blocking = new BlockingCteState();
        QueryCompletionValidator validator = new QueryCompletionValidator()
        {
            @Override
            public boolean isEnabled(Session session)
            {
                return validationEnabled(session);
            }

            @Override
            public void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo)
            {
                assertTrue(stageInfo.isFinal());
                observedStages.put(fragment.getId().getId(), stageInfo.getState());
                if (stageInfo.getState() == StageExecutionState.CANCELED && PlanNodeSearcher.searchFrom(fragment.getRoot())
                        .where(node -> node instanceof TableFinishNode)
                        .findSingle().isPresent()) {
                    canceledMaterialization.set(true);
                    blocking.release.countDown();
                }
            }
        };
        try (DistributedQueryRunner runner = createRunner(validator, ImmutableList.of(TpchTable.ORDERS, TpchTable.LINE_ITEM))) {
            runner.installPlugin(new Plugin()
            {
                @Override
                public Set<Class<?>> getFunctions()
                {
                    return ImmutableSet.of(BlockingCteFunctions.class);
                }
            });
            BlockingCteFunctions.STATE.set(blocking);
            try {
                Session session = Session.builder(validationSession(runner))
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                        .setSystemProperty(CTE_PARTITIONING_PROVIDER_CATALOG, "hive")
                        .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                        .build();
                assertEquals(runner.execute(session, "WITH c AS (SELECT orderkey, sum(quantity) AS value FROM lineitem WHERE completion_cte_block(orderkey) GROUP BY orderkey) " +
                        "SELECT orderkey, totalprice AS value FROM orders WHERE completion_cte_started(orderkey) UNION ALL SELECT orderkey, value FROM c LIMIT 10").getRowCount(), 10);
                assertTrue(canceledMaterialization.get(), "An unconsumed materialization must be canceled and validated: " + observedStages +
                        "; producer entered=" + blocking.firstCall.get() + "; release=" + blocking.release.getCount());
            }
            finally {
                blocking.release.countDown();
            }
        }
        finally {
            BlockingCteFunctions.STATE.compareAndSet(blocking, null);
        }
    }

    private static boolean validationEnabled(Session session)
    {
        return session.getSource().map("completion-validation"::equals).orElse(false);
    }

    private static Session validationSession(DistributedQueryRunner runner)
    {
        return Session.builder(runner.getDefaultSession()).setSource("completion-validation").build();
    }

    private static DistributedQueryRunner createRunner(QueryCompletionValidator validator, List<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner runner = DistributedQueryRunner.builder(createSession(Optional.empty()))
                .amendSession(builder -> builder.setSystemProperty(QUERY_MAX_RUN_TIME, "60s"))
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .put("query.partitioning-provider-catalog", "hive")
                        .put("query.exchange-materialization-strategy", "ALL")
                        .put("experimental.runtime-optimizer-enabled", "true")
                        .put("experimental.enable-stats-collection-for-temporary-table", "true")
                        .put("join-distribution-type", "PARTITIONED")
                        .put("optimizer.join-reordering-strategy", "ELIMINATE_CROSS_JOINS")
                        .put("query.hash-partition-count", "11")
                        .put("colocated-joins-enabled", "true")
                        .put("grouped-execution-enabled", "true")
                        .build())
                .setExtraModules(ImmutableList.of(binder -> newSetBinder(binder, QueryCompletionValidator.class).addBinding().toInstance(validator)))
                .build();
        try {
            runner.installPlugin(new TpchPlugin());
            runner.createCatalog("tpch", "tpch");
            ExtendedHiveMetastore metastore = getFileHiveMetastore(runner);
            metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject("tpch"));
            metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject("__temporary_tables__"));
            runner.installPlugin(new HivePlugin("hive", Optional.of(metastore)));
            runner.createCatalog("hive", "hive", ImmutableMap.of(
                    "hive.security", "sql-standard",
                    "hive.collect-column-statistics-on-write", "true",
                    "hive.temporary-table-schema", "__temporary_tables__"));
            for (TpchTable<?> table : tables) {
                runner.execute("CREATE TABLE " + table.getTableName() + " AS SELECT * FROM tpch.tiny." + table.getTableName());
            }
            return runner;
        }
        catch (Throwable failure) {
            runner.close();
            throw failure;
        }
    }

    private static final class BlockingCteState
    {
        private final AtomicBoolean firstCall = new AtomicBoolean();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
    }

    public static final class BlockingCteFunctions
    {
        private static final AtomicReference<BlockingCteState> STATE = new AtomicReference<>();

        @ScalarFunction(value = "completion_cte_block", deterministic = false)
        @SqlType(BOOLEAN)
        public static boolean block(@SqlType(BIGINT) long value)
                throws InterruptedException
        {
            BlockingCteState state = STATE.get();
            if (state.firstCall.compareAndSet(false, true)) {
                state.entered.countDown();
                assertTrue(state.release.await(30, SECONDS), "Output did not cancel unconsumed materialization");
            }
            return value > 0;
        }

        @ScalarFunction(value = "completion_cte_started", deterministic = false)
        @SqlType(BOOLEAN)
        public static boolean started(@SqlType(BIGINT) long value)
                throws InterruptedException
        {
            assertTrue(STATE.get().entered.await(30, SECONDS), "The CTE producer did not start");
            return value > 0;
        }
    }
}
