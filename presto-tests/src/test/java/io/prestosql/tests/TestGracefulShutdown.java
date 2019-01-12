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
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.prestosql.Session;
import io.prestosql.execution.TaskManager;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.server.testing.TestingPrestoServer.TestShutdownAction;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.prestosql.execution.QueryState.FINISHED;
import static io.prestosql.memory.TestMemoryManager.createQueryRunner;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
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
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("node-scheduler.include-coordinator", "false")
                .put("shutdown.grace-period", "10s")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
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

            worker.getGracefulShutdownHandler().requestShutdown();

            Futures.allAsList(queryFutures).get();

            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertEquals(info.getState(), FINISHED);
            }

            TestShutdownAction shutdownAction = (TestShutdownAction) worker.getShutdownAction();
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
}
