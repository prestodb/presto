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

import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_RESOURCES;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.units.DataSize.succinctBytes;

@Test(singleThreaded = true)
public class TestMemoryAwareExecution
{
    private TestingPrestoServer server;
    private QueryManager queryManager;
    private long totalAvailableMemory;
    private long queryMaxMemoryBytes;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        server = new TestingPrestoServer(
                true,
                ImmutableMap.of("experimental.preallocate-memory-threshold", "1B"),
                null,
                null,
                new SqlParserOptions(),
                ImmutableList.of());

        LocalMemoryManager localMemoryManager = server.getInstance(Key.get(LocalMemoryManager.class));
        ClusterMemoryManager clusterMemoryManager = server.getInstance(Key.get(ClusterMemoryManager.class));
        queryManager = server.getQueryManager();
        queryMaxMemoryBytes = server.getInstance(Key.get(MemoryManagerConfig.class)).getMaxQueryMemory().toBytes();

        // wait for cluster memory to be picked up
        while (clusterMemoryManager.getClusterMemoryBytes() == 0) {
            Thread.sleep(1000);
        }
        totalAvailableMemory = localMemoryManager.getPool(LocalMemoryManager.GENERAL_POOL).getMaxBytes();
    }

    @AfterMethod(alwaysRun = true)
    private void afterMethod()
            throws Exception
    {
        for (BasicQueryInfo info : queryManager.getQueries()) {
            if (!info.getState().isDone()) {
                queryManager.cancelQuery(info.getQueryId());
                waitForState(info.getQueryId(), FAILED);
            }
        }
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
            throws Exception
    {
        server.close();
        server = null;
        queryManager = null;
    }

    @Test
    public void testWaitingForResources()
            throws Exception
    {
        QueryId normalQuery = queryWithResourceEstimate(new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()), queryManager);
        ResourceEstimates estimate = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(DataSize.valueOf("20GB")));
        QueryId highMemoryQuery = queryWithResourceEstimate(estimate, queryManager);
        assertState(normalQuery, RUNNING);
        assertState(highMemoryQuery, WAITING_FOR_RESOURCES);
        queryManager.failQuery(normalQuery, new Exception("Killed"));
        waitForState(normalQuery, FAILED);
    }

    @Test
    public void testPreAllocateTooMuch()
            throws Exception
    {
        DataSize tooMuch = succinctBytes(queryMaxMemoryBytes + 1);
        ResourceEstimates estimate = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(tooMuch));
        QueryId highMemoryQuery = queryWithResourceEstimate(estimate, queryManager);
        assertState(highMemoryQuery, FAILED);
    }

    @Test(invocationCount = 5, invocationTimeOut = 60000)
    public void testStartWhenPreAllocationClears()
            throws Exception
    {
        // Invoke multiple times to make sure that pre-allocation state resets properly and that there aren't weird data races
        ResourceEstimates estimate = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(succinctBytes(totalAvailableMemory)));

        QueryId highMemoryQuery1 = queryWithResourceEstimate(estimate, queryManager);
        assertState(highMemoryQuery1, RUNNING);

        QueryId highMemoryQuery2 = queryWithResourceEstimate(estimate, queryManager);
        assertState(highMemoryQuery2, WAITING_FOR_RESOURCES);

        queryManager.failQuery(highMemoryQuery1, new Exception("Killed"));
        waitForState(highMemoryQuery1, FAILED);
        assertState(highMemoryQuery2, RUNNING);

        queryManager.failQuery(highMemoryQuery2, new Exception("Killed"));
        waitForState(highMemoryQuery2, FAILED);
    }

    private static QueryId queryWithResourceEstimate(ResourceEstimates estimate, QueryManager queryManager)
            throws ExecutionException, InterruptedException
    {
        QueryId queryId = queryManager.createQueryId();
        queryManager.createQuery(queryId, new TestingSessionContext(testSessionBuilder().setResourceEstimates(estimate).build()), "SELECT 1").get();
        return queryId;
    }

    private void assertState(QueryId queryId, QueryState state)
            throws InterruptedException
    {
        assertEquals(waitForState(queryId, state), state);
    }

    private QueryState waitForState(QueryId queryId, QueryState state)
            throws InterruptedException
    {
        QueryState actualState;
        do {
            actualState = queryManager.getQueryState(queryId);
            if (actualState.isDone() || actualState == state) {
                return actualState;
            }
            Thread.sleep(1000);
        }
        while (true);
    }
}
