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
package com.facebook.presto.memory;

import com.facebook.presto.Session;
import com.facebook.presto.execution.TestQueryRunnerUtil;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.cancelQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClusterMemoryLeakDetector
{
    @Test(timeOut = 30_000)
    public void testLeakDetector()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TestQueryRunnerUtil.createQueryRunner()) {
            TestingPrestoServer worker = queryRunner.getServers().get(0);
            MemoryPool generalPool = worker.getLocalMemoryManager().getPool(LocalMemoryManager.GENERAL_POOL);
            ClusterMemoryLeakDetector leakDetector = queryRunner.getCoordinator().getClusterMemoryLeakDetector();

            Session testSession = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("sf100000")
                    .setSource("test")
                    .setClientTags(ImmutableSet.of())
                    .setResourceEstimates(null)
                    .build();

            // start a long running query
            QueryId queryId = createQuery(queryRunner, testSession, "SELECT COUNT(*) FROM lineitem");

            // reserve some memory, the query won't be able to release it so effectively this is a leak
            generalPool.reserve(queryId, 1L);

            // wait for the query to start
            waitForQueryState(queryRunner, queryId, RUNNING);

            // wait until the cluster-level memory pools are updated
            waitUntilQueryMemoryReservationIsObserved(queryRunner, queryId);

            // at this point the leak detector should report no leaked queries as the query is still running
            leakDetector.checkForMemoryLeaks(0L);
            assertEquals(leakDetector.getNumberOfLeakedQueries(), 0);

            // kill the query
            cancelQuery(queryRunner, queryId);
            waitForQueryState(queryRunner, queryId, FAILED);

            // at this point the leak detector should report exactly one leaked query
            leakDetector.checkForMemoryLeaks(0L);
            assertEquals(leakDetector.getNumberOfLeakedQueries(), 1);

            // free the leaked memory
            generalPool.free(queryId, 1L);

            // wait until the cluster-level memory pools observe the previous free operation
            waitUntilAllMemoryReservationsAreFreed(queryRunner);

            // at this point the leak detector should report no leaked queries
            leakDetector.checkForMemoryLeaks(0L);
            assertEquals(leakDetector.getNumberOfLeakedQueries(), 0);
        }
    }

    private void waitUntilQueryMemoryReservationIsObserved(DistributedQueryRunner queryRunner, QueryId queryId)
            throws InterruptedException, ExecutionException
    {
        SettableFuture<Void> clusterMemoryPoolsUpdated = SettableFuture.create();
        ClusterMemoryManager clusterMemoryManager = queryRunner.getCoordinator().getClusterMemoryManager();
        clusterMemoryManager.addChangeListener(GENERAL_POOL, (memoryPoolInfo) -> {
            if (memoryPoolInfo.getQueryMemoryReservations().containsKey(queryId)) {
                clusterMemoryPoolsUpdated.set(null);
            }
        });
        clusterMemoryPoolsUpdated.get();
    }

    private void waitUntilAllMemoryReservationsAreFreed(DistributedQueryRunner queryRunner)
            throws InterruptedException, ExecutionException
    {
        SettableFuture<Void> clusterMemoryPoolsUpdated = SettableFuture.create();
        ClusterMemoryManager clusterMemoryManager = queryRunner.getCoordinator().getClusterMemoryManager();
        clusterMemoryManager.addChangeListener(GENERAL_POOL, (memoryPoolInfo) -> {
            if (memoryPoolInfo.getQueryMemoryReservations().size() == 0) {
                clusterMemoryPoolsUpdated.set(null);
            }
        });
        clusterMemoryPoolsUpdated.get();
    }
}
