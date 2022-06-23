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

import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.TestingSessionContext;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_CPU_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_OUTPUT_SIZE_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_SCAN_RAW_BYTES_READ_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder.builder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryManager
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().build();
        TestingPrestoServer server = queryRunner.getCoordinator();
        server.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        server.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @AfterMethod
    public void cancelAllQueriesAfterTest()
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        ImmutableList.copyOf(dispatchManager.getQueries()).forEach(queryInfo -> dispatchManager.cancelQuery(queryInfo.getQueryId()));
    }

    @Test(timeOut = 60_000L)
    public void testFailQuery()
            throws Exception
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        QueryId queryId = dispatchManager.createQueryId();
        dispatchManager.createQuery(
                queryId,
                "slug",
                0,
                new TestingSessionContext(TEST_SESSION),
                "SELECT * FROM lineitem")
                .get();

        // wait until query starts running
        while (true) {
            QueryState state = dispatchManager.getQueryInfo(queryId).getState();
            if (state.isDone()) {
                fail("unexpected query state: " + state);
            }
            if (state == RUNNING) {
                break;
            }
            Thread.sleep(100);
        }

        // cancel query
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        queryManager.failQuery(queryId, new PrestoException(GENERIC_INTERNAL_ERROR, "mock exception"));
        QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
        assertEquals(queryInfo.getState(), FAILED);
        assertEquals(queryInfo.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertNotNull(queryInfo.getFailureInfo());
        assertEquals(queryInfo.getFailureInfo().getMessage(), "mock exception");
        assertEquals(queryManager.getStats().getQueuedQueries(), 0);
    }

    @Test(timeOut = 60_000L)
    public void testFailQueryPrerun()
            throws Exception
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        // Create 3 running queries to guarantee queueing
        createQueries(dispatchManager, 3);
        QueryId queryId = dispatchManager.createQueryId();
        dispatchManager.createQuery(
                queryId,
                "slug",
                0,
                new TestingSessionContext(TEST_SESSION),
                "SELECT * FROM lineitem")
                .get();

        assertNotEquals(dispatchManager.getStats().getQueuedQueries(), 0L, "Expected 0 queued queries, found: " + dispatchManager.getStats().getQueuedQueries());

        // wait until it's admitted but fail it before it starts
        while (true) {
            QueryState state = dispatchManager.getQueryInfo(queryId).getState();
            if (state.ordinal() >= RUNNING.ordinal()) {
                fail("unexpected query state: " + state);
            }
            if (state.ordinal() >= QUEUED.ordinal()) {
                // cancel query
                dispatchManager.failQuery(queryId, new PrestoException(GENERIC_USER_ERROR, "mock exception"));
                break;
            }
        }

        QueryState state = dispatchManager.getQueryInfo(queryId).getState();
        assertEquals(state, FAILED);
        //Give the stats a time to update
        Thread.sleep(1000);
        assertEquals(queryManager.getStats().getQueuedQueries(), 0);
    }

    void createQueries(DispatchManager dispatchManager, int queryCount)
            throws InterruptedException, java.util.concurrent.ExecutionException
    {
        for (int i = 0; i < queryCount; i++) {
            dispatchManager.createQuery(
                    dispatchManager.createQueryId(),
                    "slug",
                    0,
                    new TestingSessionContext(TEST_SESSION),
                    "SELECT * FROM lineitem")
                    .get();
        }
    }

    @Test(timeOut = 60_000L)
    public void testQueryCpuLimit()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = builder().setSingleExtraProperty("query.max-cpu-time", "1ms").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT COUNT(*) FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertEquals(queryInfo.getState(), FAILED);
            assertEquals(queryInfo.getErrorCode(), EXCEEDED_CPU_LIMIT.toErrorCode());
        }
    }

    @Test(timeOut = 60_000L)
    public void testQueryScanExceeded()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().setSingleExtraProperty("query.max-scan-raw-input-bytes", "0B").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT COUNT(*) FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertEquals(queryInfo.getState(), FAILED);
            assertEquals(queryInfo.getErrorCode(), EXCEEDED_SCAN_RAW_BYTES_READ_LIMIT.toErrorCode());
        }
    }

    @Test(timeOut = 60_000L)
    public void testQueryOutputSizeExceeded()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().setSingleExtraProperty("query.max-output-size", "1B").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT COUNT(*) FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertEquals(queryInfo.getState(), FAILED);
            assertEquals(queryInfo.getErrorCode(), EXCEEDED_OUTPUT_SIZE_LIMIT.toErrorCode());
        }
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
