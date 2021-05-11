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
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.transaction.TransactionInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.testing.Assertions.assertLessThanOrEqual;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestBrutalShutdown
{
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 240_000;
    private static final Session TINY_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private ListeningExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testQueryRetryOnShutdown()
            throws Exception
    {
        int totalQueries = 5;
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of())) {
            queryRetryOnShutdown(TINY_SESSION, queryRunner, executor, totalQueries);

            int totalSuccessfulQueries = 0;
            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                if (info.getQuery().contains("-- retry query")) {
                    assertEquals(info.getState(), FINISHED);
                }
                if (info.getState() == FINISHED) {
                    totalSuccessfulQueries++;
                }
            }
            assertEquals(totalSuccessfulQueries, totalQueries);
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testTransactionalQueryRetryOnShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of())) {
            executor.submit(() -> queryRunner.execute(TINY_SESSION, "START TRANSACTION")).get();

            TransactionInfo transactionInfo = queryRunner.getCoordinator().getTransactionManager().getAllTransactionInfos().get(0);
            Session session = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .setTransactionId(transactionInfo.getTransactionId())
                    .build();

            // only send 1 query as the first failed query will abort the transaction
            queryRetryOnShutdown(session, queryRunner, executor, 1);

            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                if (info.getQuery().contains("-- retry query")) {
                    fail("no retry query is allowed within a transaction");
                }
            }
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testRetryCircuitBreaker()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("global-query-retry-failure-limit", "2"))) {
            queryRetryOnShutdown(TINY_SESSION, queryRunner, executor, 10);

            int totalSuccessfulRetryQueries = 0;
            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                if (info.getQuery().contains("-- retry query")) {
                    assertEquals(info.getState(), FINISHED);
                    totalSuccessfulRetryQueries++;
                }
            }
            assertLessThanOrEqual(totalSuccessfulRetryQueries, 2);
        }
    }

    private static void queryRetryOnShutdown(
            Session session,
            DistributedQueryRunner queryRunner,
            ListeningExecutorService executor,
            int totalQueries)
            throws Exception
    {
        List<ListenableFuture<?>> queryFutures = new ArrayList<>();
        for (int i = 0; i < totalQueries; i++) {
            queryFutures.add(executor.submit(() -> queryRunner.execute(session, "SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
        }

        TestingPrestoServer worker = queryRunner.getServers()
                .stream()
                .filter(server -> !server.isCoordinator())
                .findFirst()
                .get();

        TaskManager taskManager = worker.getTaskManager();

        // wait until tasks show up on the worker
        while (taskManager.getAllTaskInfo().isEmpty()) {
            MILLISECONDS.sleep(100);
        }

        // kill a worker
        worker.stopResponding();

        for (ListenableFuture<?> future : queryFutures) {
            try {
                future.get();
            }
            catch (Exception e) {
                // it is ok to fail
            }
        }
    }

    private static DistributedQueryRunner createQueryRunner(Map<String, String> extraCoordinatorProperties)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("node-scheduler.include-coordinator", "false")
                .put("per-query-retry-limit", "1")
                .put("exchange.max-error-duration", "1s")
                .put("query.remote-task.max-error-duration", "1s")
                .build();

        Map<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                // decrease the heartbeat interval so we detect failed nodes faster
                .put("failure-detector.enabled", "true")
                .put("failure-detector.heartbeat-interval", "1s")
                .put("failure-detector.http-client.request-timeout", "500ms")
                .put("failure-detector.exponential-decay-seconds", "1")
                .put("failure-detector.threshold", "0.1")
                .putAll(extraCoordinatorProperties)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TINY_SESSION)
                .setCoordinatorCount(1)
                .setNodeCount(5)
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(properties)
                .build();

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
}
