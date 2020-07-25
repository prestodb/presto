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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.execution.TestQueues.LONG_LASTING_QUERY;
import static com.facebook.presto.execution.TestQueues.newSession;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_HAS_TOO_MANY_STAGES;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestQueryTaskLimit
{
    private ExecutorService executor;
    private Session defaultSession;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool();
        defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf1000")
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
            throws Exception
    {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, SECONDS));
        executor = null;
    }

    @Test(timeOut = 30_000, expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*Query killed because the cluster is overloaded with too many tasks.*")
    public void testExceedTaskLimit()
            throws Exception
    {
        ImmutableMap<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .put("max-total-running-task-count-to-kill-query", "4")
                .put("max-query-running-task-count", "4")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(defaultSession, extraProperties)) {
            Future<?> query = executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk"));

            waitForQueryToBeKilled(queryRunner);

            query.get();
        }
    }

    @Test(timeOut = 30_000)
    public void testQueuingWhenTaskLimitExceeds()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(defaultSession, ImmutableMap.of())) {
            QueryId firstQuery = createQuery(queryRunner, newSession("test", ImmutableSet.of(), null), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, firstQuery, RUNNING);

            queryRunner.getCoordinator().getResourceGroupManager().get().setTaskLimitExceeded(true);

            QueryId secondQuery = createQuery(queryRunner, newSession("test", ImmutableSet.of(), null), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, secondQuery, QUEUED);

            queryRunner.getCoordinator().getResourceGroupManager().get().setTaskLimitExceeded(false);
            waitForQueryState(queryRunner, secondQuery, RUNNING);
        }
    }

    public static DistributedQueryRunner createQueryRunner(Session session, Map<String, String> properties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 2, properties);

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    private void waitForQueryToBeKilled(DistributedQueryRunner queryRunner)
            throws InterruptedException
    {
        while (true) {
            for (BasicQueryInfo info : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                if (info.getState().isDone()) {
                    assertNotNull(info.getErrorCode());
                    assertEquals(info.getErrorCode().getCode(), QUERY_HAS_TOO_MANY_STAGES.toErrorCode().getCode());
                    MILLISECONDS.sleep(100);
                    return;
                }
            }
            MILLISECONDS.sleep(10);
        }
    }
}
