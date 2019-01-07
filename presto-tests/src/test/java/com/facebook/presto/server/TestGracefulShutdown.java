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
package com.facebook.presto.server;

import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.server.testing.TestingPrestoServer.TestShutdownAction;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.memory.TestMemoryManager.createQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestGracefulShutdown
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
    public void testShutdown()
            throws Exception
    {
        try (QueryWorkerInfo info = runQueries()) {
            info.worker.getGracefulShutdownHandler().requestShutdown();
            Futures.allAsList(info.queryFutures).get();
            assertQueriesFinished(info);
            TestShutdownAction shutdownAction = (TestShutdownAction) info.worker.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS);
            assertTrue(shutdownAction.isWorkerShutdown());
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testCoordinatorShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, ImmutableMap.of())) {
            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();

            coordinator.getGracefulShutdownHandler().requestShutdown();
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testCancelWaitingForActiveTasks()
            throws Exception
    {
        try (QueryWorkerInfo info = runQueries()) {
            TestShutdownAction shutdownAction = (TestShutdownAction) info.worker.getShutdownAction();
            info.worker.getGracefulShutdownHandler().requestShutdown();
            shutdownAction.waitForShutdownToStart(SHUTDOWN_TIMEOUT_MILLIS);
            info.worker.getGracefulShutdownHandler().cancelShutdown();
            assertTrue(shutdownAction.waitActiveTasksCancelled(SHUTDOWN_TIMEOUT_MILLIS));
            assertFalse(shutdownAction.isWorkerShutdown());
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testSkipJVMShutdown()
            throws Exception
    {
        try (QueryWorkerInfo info = runQueries()) {
            TestShutdownAction shutdownAction = (TestShutdownAction) info.worker.getShutdownAction();
            info.worker.getGracefulShutdownHandler().requestShutdown();
            shutdownAction.waitForShutdownToStart(SHUTDOWN_TIMEOUT_MILLIS);
            // Test only invocation to simulate that cancel shutdown was invoked after waiting for active tasks to finish.
            // However, JVM shutdown is skipped since the cancel flag was set.
            info.worker.getGracefulShutdownHandler().cancelShutdown(false);
            assertQueriesFinished(info);
            assertTrue(shutdownAction.jvmShutdownCancelled(SHUTDOWN_TIMEOUT_MILLIS));
            assertFalse(shutdownAction.isWorkerShutdown());
        }
    }

    private static class QueryWorkerInfo
            implements Closeable
    {
        private final List<ListenableFuture<?>> queryFutures;
        private final DistributedQueryRunner queryRunner;
        private final TestingPrestoServer worker;

        QueryWorkerInfo(List<ListenableFuture<?>> qf, DistributedQueryRunner runner, TestingPrestoServer worker)
        {
            this.queryFutures = qf;
            this.queryRunner = runner;
            this.worker = worker;
        }

        @Override
        public void close()
        {
            queryRunner.close();
        }
    }

    private void assertQueriesFinished(QueryWorkerInfo info)
            throws ExecutionException, InterruptedException
    {
        Futures.allAsList(info.queryFutures).get();
        List<BasicQueryInfo> queryInfos = info.queryRunner.getCoordinator().getQueryManager().getQueries();
        for (BasicQueryInfo i : queryInfos) {
            assertEquals(i.getState(), FINISHED);
        }
    }

    private QueryWorkerInfo runQueries()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("node-scheduler.include-coordinator", "false")
                .put("shutdown.grace-period", "10s")
                .build();

        DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties);
        List<ListenableFuture<?>> queryFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            queryFutures.add(executor.submit(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk")));
        }

        TestingPrestoServer worker = queryRunner.getServers()
                .stream()
                .filter(server -> !server.isCoordinator())
                .findFirst()
                .get();

        TaskManager taskManager = worker.getTaskManager();

        // wait until tasks show up on the worker
        while (taskManager.getAllTaskInfo().isEmpty()) {
            MILLISECONDS.sleep(500);
        }
        return new QueryWorkerInfo(queryFutures, queryRunner, worker);
    }
}
