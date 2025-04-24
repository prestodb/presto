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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.MockQueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TestEventListener.EventsBuilder;
import com.facebook.presto.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import com.facebook.presto.execution.TestingSessionContext;
import com.facebook.presto.plugin.blackhole.BlackHolePlugin;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getSimpleQueryRunner;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_CPU_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_OUTPUT_POSITIONS_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_OUTPUT_SIZE_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_SCAN_RAW_BYTES_READ_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder.builder;
import static com.facebook.presto.utils.ResourceUtils.getResourceFilePath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryManager
{
    private DistributedQueryRunner queryRunner;
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM blackhole.default.dummy";

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = getSimpleQueryRunner();
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        queryRunner.execute("CREATE TABLE blackhole.default.dummy (col BIGINT) WITH (split_count = 1, rows_per_page = 1, pages_per_split = 1, page_processing_delay = '10m')");
        TestingPrestoServer server = queryRunner.getCoordinator();
        server.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        server.getResourceGroupManager().get()
                .forceSetConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
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
                        LONG_LASTING_QUERY)
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

        //Wait for the queries to be in running state
        while (dispatchManager.getStats().getRunningQueries() != 3) {
            Thread.sleep(1000);
        }
        dispatchManager.createQuery(
                        queryId,
                        "slug",
                        0,
                        new TestingSessionContext(TEST_SESSION),
                        LONG_LASTING_QUERY)
                .get();

        //Wait for query to be in queued state
        while (dispatchManager.getStats().getQueuedQueries() != 1) {
            Thread.sleep(1000);
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        // wait until it's admitted but fail it before it starts
        while (dispatchManager.getStats().getQueuedQueries() > 0 && stopwatch.elapsed().toMillis() < 5000) {
            QueryState state = dispatchManager.getQueryInfo(queryId).getState();
            if (state.ordinal() == FAILED.ordinal()) {
                Thread.sleep(100);
                continue;
            }
            if (state.ordinal() >= QUEUED.ordinal()) {
                // cancel query
                dispatchManager.failQuery(queryId, new PrestoException(GENERIC_USER_ERROR, "mock exception"));
                continue;
            }
        }

        QueryState state = dispatchManager.getQueryInfo(queryId).getState();
        assertEquals(state, FAILED);
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
                            LONG_LASTING_QUERY)
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
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getSchema(), TEST_SESSION.getSchema());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getCatalog(), TEST_SESSION.getCatalog());
        }
    }

    @Test(timeOut = 60_000L)
    public void testQueryScanExceeded()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = builder().setSingleExtraProperty("query.max-scan-raw-input-bytes", "0B").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT COUNT(*) FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertEquals(queryInfo.getState(), FAILED);
            assertEquals(queryInfo.getErrorCode(), EXCEEDED_SCAN_RAW_BYTES_READ_LIMIT.toErrorCode());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getSchema(), TEST_SESSION.getSchema());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getCatalog(), TEST_SESSION.getCatalog());
        }
    }

    @Test(timeOut = 60_000L)
    public void testQueryOutputPositionsExceeded()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = builder().setSingleExtraProperty("query.max-output-positions", "10").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT * FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertEquals(queryInfo.getState(), FAILED);
            assertEquals(queryInfo.getErrorCode(), EXCEEDED_OUTPUT_POSITIONS_LIMIT.toErrorCode());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getSchema(), TEST_SESSION.getSchema());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getCatalog(), TEST_SESSION.getCatalog());
        }
    }

    @Test(timeOut = 120_000L)
    public void testQueryOutputSizeExceeded()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = builder().setSingleExtraProperty("query.max-output-size", "1B").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT COUNT(*) FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertEquals(queryInfo.getState(), FAILED);
            assertEquals(queryInfo.getErrorCode(), EXCEEDED_OUTPUT_SIZE_LIMIT.toErrorCode());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getSchema(), TEST_SESSION.getSchema());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getCatalog(), TEST_SESSION.getCatalog());
        }
    }

    @Test(timeOut = 60_000L)
    public void testQueryClientTimeoutExceeded()
            throws Exception
    {
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(SystemSessionProperties.QUERY_CLIENT_TIMEOUT, "1s")
                .build();

        try (DistributedQueryRunner queryRunner = builder().setSingleExtraProperty("query.client.timeout", "3m").build()) {
            QueryId queryId = createQuery(queryRunner, session, "SELECT COUNT(*) FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertEquals(queryInfo.getState(), FAILED);
            assertEquals(queryInfo.getErrorCode(), ABANDONED_QUERY.toErrorCode());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getSchema(), session.getSchema());
            assertEquals(queryManager.getQuerySession(queryId).getAccessControlContext().getCatalog(), session.getCatalog());
        }
    }

    @Test
    public void testQueryCountMetrics()
            throws Exception
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        // Create a total of 6 queries to test concurrency limit and
        // ensure that some queries are queued as concurrency limit is 3
        createQueries(dispatchManager, 6);

        while (dispatchManager.getStats().getRunningQueries() != 3
                || dispatchManager.getStats().getQueuedQueries() != 3) {
            Thread.sleep(1000);
        }

        List<BasicQueryInfo> queries = dispatchManager.getQueries();
        assertEquals(dispatchManager.getStats().getQueuedQueries(),
                queries.stream().filter(basicQueryInfo -> basicQueryInfo.getState() == QUEUED).count());
        assertEquals(dispatchManager.getStats().getRunningQueries(),
                queries.stream().filter(basicQueryInfo -> basicQueryInfo.getState() == RUNNING).count());

        Stopwatch stopwatch = Stopwatch.createStarted();

        long oldQueuedQueryCount = dispatchManager.getStats().getQueuedQueries();

        // Assert that number of queued queries are decreasing with time and
        // number of running queries are always <= 3 (max concurrency limit)
        while (dispatchManager.getStats().getQueuedQueries() + dispatchManager.getStats().getRunningQueries() > 0
                && stopwatch.elapsed().toMillis() < 60000) {
            assertTrue(dispatchManager.getStats().getQueuedQueries() <= oldQueuedQueryCount);
            assertTrue(dispatchManager.getStats().getRunningQueries() <= 3);

            oldQueuedQueryCount = dispatchManager.getStats().getQueuedQueries();

            Thread.sleep(1000);
        }
    }

    @Test
    public void testQueryCompletedInfoNotPruned()
            throws Exception
    {
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(TEST_SESSION)
                .setNodeCount(0)
                .build()) {
            EventsBuilder eventsBuilder = new EventsBuilder();
            eventsBuilder.initialize(1);
            TestingEventListenerPlugin testEventListenerPlugin = new TestingEventListenerPlugin(eventsBuilder);
            runner.installPlugin(testEventListenerPlugin);
            QueryManager manager = runner.getCoordinator().getQueryManager();
            QueryId id = runner.getCoordinator().getDispatchManager().createQueryId();
            @Language("SQL") String sql = "SELECT * FROM lineitem WHERE linenumber = 0 LIMIT 1";
            QueryInfo mockInfo = mockInfo(sql, id.toString(), FINISHED);
            MockExecution exec = new MockExecution(eventsBuilder, mockInfo);
            manager.createQuery(exec);

            // when the listener executes, we will verify that the query completed event exists
            // when pruneInfo is called
            exec.finalInfoListeners.forEach(item -> item.stateChanged(mockInfo));
            // verify we actually called pruneQueryFinished to assert that it was checked
            assertEquals(exec.pruneFinishedCalls, 1);
        }
    }

    private static class MockExecution
            extends MockQueryExecution
    {
        List<StateMachine.StateChangeListener<QueryInfo>> finalInfoListeners = new ArrayList<>();
        private final EventsBuilder eventsBuilder;
        int pruneFinishedCalls;
        int pruneExpiredCalls;
        private final QueryInfo info;

        private MockExecution(EventsBuilder eventsBuilder, QueryInfo info)
        {
            this.eventsBuilder = eventsBuilder;
            this.info = info;
        }

        @Override
        public long getCreateTimeInMillis()
        {
            return info.getQueryStats().getCreateTimeInMillis();
        }

        @Override
        public Duration getQueuedTime()
        {
            return info.getQueryStats().getQueuedTime();
        }

        @Override
        public Duration getTotalCpuTime()
        {
            return info.getQueryStats().getTotalCpuTime();
        }

        @Override
        public long getRawInputDataSizeInBytes()
        {
            return info.getQueryStats().getRawInputDataSize().toBytes();
        }

        @Override
        public long getOutputDataSizeInBytes()
        {
            return info.getQueryStats().getOutputDataSize().toBytes();
        }

        @Override
        public Session getSession()
        {
            return TEST_SESSION;
        }

        @Override
        public void addFinalQueryInfoListener(StateMachine.StateChangeListener<QueryInfo> stateChangeListener)
        {
            finalInfoListeners.add(stateChangeListener);
        }

        @Override
        public void pruneExpiredQueryInfo()
        {
            pruneExpiredCalls++;
            Optional<QueryCompletedEvent> event = eventsBuilder.getQueryCompletedEvents().stream()
                    .filter(x -> x.getMetadata().getQueryId().equals(info.getQueryId().toString()))
                    .findFirst();
            // verify that the event listener was notified before prune was called
            assertTrue(event.isPresent());
        }

        @Override
        public void pruneFinishedQueryInfo()
        {
            pruneFinishedCalls++;
            Optional<QueryCompletedEvent> event = eventsBuilder.getQueryCompletedEvents().stream()
                    .filter(x -> x.getMetadata().getQueryId().equals(info.getQueryId().toString()))
                    .findFirst();
            // verify that the event listener was notified before prune was called
            assertTrue(event.isPresent());
        }
    }

    private static QueryInfo mockInfo(String query, String queryId, QueryState state)
    {
        return new QueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                state,
                new MemoryPoolId("reserved"),
                true,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                query,
                Optional.empty(),
                Optional.empty(),
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30").getMillis(),
                        DateTime.parse("1991-09-06T05:01-05:30").getMillis(),
                        DateTime.parse("1991-09-06T05:02-05:30").getMillis(),
                        DateTime.parse("1991-09-06T06:00-05:30").getMillis(),
                        Duration.valueOf("8m"),
                        Duration.valueOf("5m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        Duration.valueOf("5m"),
                        Duration.valueOf("6m"),
                        Duration.valueOf("35m"),
                        Duration.valueOf("44m"),
                        Duration.valueOf("9m"),
                        Duration.valueOf("10m"),
                        Duration.valueOf("11m"),
                        13,
                        14,
                        15,
                        16,
                        100,
                        17,
                        18,
                        34,
                        19,
                        100,
                        17,
                        18,
                        19,
                        100,
                        17,
                        18,
                        19,
                        20.0,
                        43.0,
                        DataSize.valueOf("21GB"),
                        DataSize.valueOf("22GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("24GB"),
                        DataSize.valueOf("25GB"),
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("42GB"),
                        true,
                        Duration.valueOf("23m"),
                        Duration.valueOf("24m"),
                        Duration.valueOf("0m"),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        DataSize.valueOf("123MB"),
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("32GB"),
                        40,
                        DataSize.valueOf("31GB"),
                        32,
                        33,
                        DataSize.valueOf("34GB"),
                        DataSize.valueOf("35GB"),
                        DataSize.valueOf("36GB"),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        new RuntimeStats()),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "33",
                Optional.empty(),
                null,
                EXCEEDED_GLOBAL_MEMORY_LIMIT.toErrorCode(),
                ImmutableList.of(
                        new PrestoWarning(
                                new WarningCode(123, "WARNING_123"),
                                "warning message")),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }
}
