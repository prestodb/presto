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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.MAX_STAGE_RETRIES;
import static com.facebook.presto.SystemSessionProperties.PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.SystemSessionProperties.RECOVERABLE_GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static com.facebook.presto.SystemSessionProperties.SCALE_WRITERS;
import static com.facebook.presto.SystemSessionProperties.TASK_PARTITIONED_WRITER_COUNT;
import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_BUCKETED_SCHEMA;
import static com.facebook.presto.hive.HiveSessionProperties.OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.VIRTUAL_BUCKET_COUNT;
import static com.facebook.presto.spi.security.SelectedRole.Type.ALL;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Collections.shuffle;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHiveRecoverableExecution
{
    private static final Logger log = Logger.get(TestHiveRecoverableExecution.class);

    private static final int TEST_TIMEOUT = 120_000;
    private static final int INVOCATION_COUNT = 1;

    private DistributedQueryRunner queryRunner;
    private ListeningExecutorService executor;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
        executor = listeningDecorator(newCachedThreadPool());
    }

    private DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap.Builder<String, String> extraPropertiesBuilder = ImmutableMap.<String, String>builder()
                // task get results timeout has to be significantly higher that the task status update timeout
                .put("exchange.max-error-duration", "5m")
                // set the timeout of a single HTTP request to 1s, to make sure that the task result requests are actually failing
                // The default is 10s, that might not be sufficient to make sure that the request fails before the recoverable execution kicks in
                .put("exchange.http-client.request-timeout", "1s");

        ImmutableMap.Builder<String, String> extraCoordinatorPropertiesBuilder = ImmutableMap.<String, String>builder();

        extraCoordinatorPropertiesBuilder
                // decrease the heartbeat interval so we detect failed nodes faster
                .put("failure-detector.enabled", "true")
                .put("failure-detector.heartbeat-interval", "1s")
                .put("failure-detector.http-client.request-timeout", "500ms")
                .put("failure-detector.exponential-decay-seconds", "1")
                .put("failure-detector.threshold", "0.1")
                // allow 2 out of 4 tasks to fail
                .put("max-failed-task-percentage", "0.6")
                // set the timeout of the task update requests to something low to improve overall test latency
                .put("scheduler.http-client.request-timeout", "5s")
                // this effectively disables the retries
                .put("query.remote-task.max-error-duration", "1s")
                .put("use-legacy-scheduler", "false");

        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS),
                // extra properties
                extraPropertiesBuilder.build(),
                // extra coordinator properties
                extraCoordinatorPropertiesBuilder.build(),
                Optional.empty());
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        if (executor != null) {
            executor.shutdownNow();
        }
        executor = null;

        if (queryRunner != null) {
            queryRunner.close();
        }
        queryRunner = null;
    }

    @DataProvider(name = "testSettings")
    public static Object[][] testSettings()
    {
        return new Object[][] {{1, true}, {2, false}, {2, true}};
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "testSettings", invocationCount = INVOCATION_COUNT)
    public void testCreateBucketedTable(int writerConcurrency, boolean optimizedPartitionUpdateSerializationEnabled)
            throws Exception
    {
        testRecoverableGroupedExecution(
                queryRunner,
                writerConcurrency,
                optimizedPartitionUpdateSerializationEnabled,
                ImmutableList.of(
                        "CREATE TABLE create_bucketed_table_1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders",
                        "CREATE TABLE create_bucketed_table_2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders",
                        "CREATE TABLE create_bucketed_table_3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders"),
                "CREATE TABLE create_bucketed_table_success WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                        "SELECT key1, value1, key2, value2, key3, value3\n" +
                        "FROM create_bucketed_table_1\n" +
                        "JOIN create_bucketed_table_2\n" +
                        "ON key1 = key2\n" +
                        "JOIN create_bucketed_table_3\n" +
                        "ON key2 = key3",
                "CREATE TABLE create_bucketed_table_failure WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                        "SELECT key1, value1, key2, value2, key3, value3\n" +
                        "FROM create_bucketed_table_1\n" +
                        "JOIN create_bucketed_table_2\n" +
                        "ON key1 = key2\n" +
                        "JOIN create_bucketed_table_3\n" +
                        "ON key2 = key3",
                15000,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS create_bucketed_table_1",
                        "DROP TABLE IF EXISTS create_bucketed_table_2",
                        "DROP TABLE IF EXISTS create_bucketed_table_3",
                        "DROP TABLE IF EXISTS create_bucketed_table_success",
                        "DROP TABLE IF EXISTS create_bucketed_table_failure"));
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "testSettings", invocationCount = INVOCATION_COUNT)
    public void testInsertBucketedTable(int writerConcurrency, boolean optimizedPartitionUpdateSerializationEnabled)
            throws Exception
    {
        testRecoverableGroupedExecution(
                queryRunner,
                writerConcurrency,
                optimizedPartitionUpdateSerializationEnabled,
                ImmutableList.of(
                        "CREATE TABLE insert_bucketed_table_1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders",
                        "CREATE TABLE insert_bucketed_table_2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders",
                        "CREATE TABLE insert_bucketed_table_3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders",
                        "CREATE TABLE insert_bucketed_table_success (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key'], partitioned_by = ARRAY['partition_key'])",
                        "CREATE TABLE insert_bucketed_table_failure (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key'], partitioned_by = ARRAY['partition_key'])"),
                "INSERT INTO insert_bucketed_table_success\n" +
                        "SELECT key1, value1, 'foo'\n" +
                        "FROM insert_bucketed_table_1\n" +
                        "JOIN insert_bucketed_table_2\n" +
                        "ON key1 = key2\n" +
                        "JOIN insert_bucketed_table_3\n" +
                        "ON key2 = key3",
                "INSERT INTO insert_bucketed_table_failure\n" +
                        "SELECT key1, value1, 'foo'\n" +
                        "FROM insert_bucketed_table_1\n" +
                        "JOIN insert_bucketed_table_2\n" +
                        "ON key1 = key2\n" +
                        "JOIN insert_bucketed_table_3\n" +
                        "ON key2 = key3",
                15000,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS insert_bucketed_table_1",
                        "DROP TABLE IF EXISTS insert_bucketed_table_2",
                        "DROP TABLE IF EXISTS insert_bucketed_table_3",
                        "DROP TABLE IF EXISTS insert_bucketed_table_success",
                        "DROP TABLE IF EXISTS insert_bucketed_table_failure"));
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "testSettings", invocationCount = INVOCATION_COUNT)
    public void testCreateUnbucketedTableWithGroupedExecution(int writerConcurrency, boolean optimizedPartitionUpdateSerializationEnabled)
            throws Exception
    {
        testRecoverableGroupedExecution(
                queryRunner,
                writerConcurrency,
                optimizedPartitionUpdateSerializationEnabled,
                ImmutableList.of(
                        "CREATE TABLE create_unbucketed_table_with_grouped_execution_1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders",
                        "CREATE TABLE create_unbucketed_table_with_grouped_execution_2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders",
                        "CREATE TABLE create_unbucketed_table_with_grouped_execution_3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders"),
                "CREATE TABLE create_unbucketed_table_with_grouped_execution_success AS\n" +
                        "SELECT key1, value1, key2, value2, key3, value3\n" +
                        "FROM create_unbucketed_table_with_grouped_execution_1\n" +
                        "JOIN create_unbucketed_table_with_grouped_execution_2\n" +
                        "ON key1 = key2\n" +
                        "JOIN create_unbucketed_table_with_grouped_execution_3\n" +
                        "ON key2 = key3",
                "CREATE TABLE create_unbucketed_table_with_grouped_execution_failure AS\n" +
                        "SELECT key1, value1, key2, value2, key3, value3\n" +
                        "FROM create_unbucketed_table_with_grouped_execution_1\n" +
                        "JOIN create_unbucketed_table_with_grouped_execution_2\n" +
                        "ON key1 = key2\n" +
                        "JOIN create_unbucketed_table_with_grouped_execution_3\n" +
                        "ON key2 = key3",
                15000,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS create_unbucketed_table_with_grouped_execution_1",
                        "DROP TABLE IF EXISTS create_unbucketed_table_with_grouped_execution_2",
                        "DROP TABLE IF EXISTS create_unbucketed_table_with_grouped_execution_3",
                        "DROP TABLE IF EXISTS create_unbucketed_table_with_grouped_execution_success",
                        "DROP TABLE IF EXISTS create_unbucketed_table_with_grouped_execution_failure"));
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "testSettings", invocationCount = INVOCATION_COUNT)
    public void testInsertUnbucketedTableWithGroupedExecution(int writerConcurrency, boolean optimizedPartitionUpdateSerializationEnabled)
            throws Exception
    {
        testRecoverableGroupedExecution(
                queryRunner,
                writerConcurrency,
                optimizedPartitionUpdateSerializationEnabled,
                ImmutableList.of(
                        "CREATE TABLE insert_unbucketed_table_with_grouped_execution_1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders",
                        "CREATE TABLE insert_unbucketed_table_with_grouped_execution_2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders",
                        "CREATE TABLE insert_unbucketed_table_with_grouped_execution_3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders",
                        "CREATE TABLE insert_unbucketed_table_with_grouped_execution_success (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (partitioned_by = ARRAY['partition_key'])",
                        "CREATE TABLE insert_unbucketed_table_with_grouped_execution_failure (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (partitioned_by = ARRAY['partition_key'])"),
                "INSERT INTO insert_unbucketed_table_with_grouped_execution_success\n" +
                        "SELECT key1, value1, 'foo'\n" +
                        "FROM insert_unbucketed_table_with_grouped_execution_1\n" +
                        "JOIN insert_unbucketed_table_with_grouped_execution_2\n" +
                        "ON key1 = key2\n" +
                        "JOIN insert_unbucketed_table_with_grouped_execution_3\n" +
                        "ON key2 = key3",
                "INSERT INTO insert_unbucketed_table_with_grouped_execution_failure\n" +
                        "SELECT key1, value1, 'foo'\n" +
                        "FROM insert_unbucketed_table_with_grouped_execution_1\n" +
                        "JOIN insert_unbucketed_table_with_grouped_execution_2\n" +
                        "ON key1 = key2\n" +
                        "JOIN insert_unbucketed_table_with_grouped_execution_3\n" +
                        "ON key2 = key3",
                15000,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS insert_unbucketed_table_with_grouped_execution_1",
                        "DROP TABLE IF EXISTS insert_unbucketed_table_with_grouped_execution_2",
                        "DROP TABLE IF EXISTS insert_unbucketed_table_with_grouped_execution_3",
                        "DROP TABLE IF EXISTS insert_unbucketed_table_with_grouped_execution_success",
                        "DROP TABLE IF EXISTS insert_unbucketed_table_with_grouped_execution_failure"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testCountOnUnbucketedTable()
            throws Exception
    {
        testRecoverableGroupedExecution(
                queryRunner,
                4,
                true,
                ImmutableList.of(
                        "CREATE TABLE test_table AS\n" +
                                "SELECT orderkey, comment\n" +
                                "FROM orders\n"),
                "CREATE TABLE test_success AS\n" +
                        "SELECT count(*) as a, comment FROM test_table group by comment",
                "create table test_failure AS\n" +
                        "SELECT count(*) as a, comment FROM test_table group by comment",
                14995, // there are 14995 distinct comments in the orders table
                ImmutableList.of(
                        "DROP TABLE IF EXISTS test_table",
                        "DROP TABLE IF EXISTS test_success",
                        "DROP TABLE IF EXISTS test_failure"));
    }

    private void testRecoverableGroupedExecution(
            DistributedQueryRunner queryRunner,
            int writerConcurrency,
            boolean optimizedPartitionUpdateSerializationEnabled,
            List<String> preQueries,
            @Language("SQL") String queryWithoutFailure,
            @Language("SQL") String queryWithFailure,
            int expectedUpdateCount,
            List<String> postQueries)
            throws Exception
    {
        waitUntilAllNodesAreHealthy(queryRunner, new Duration(10, SECONDS));

        Session recoverableSession = createRecoverableSession(writerConcurrency, optimizedPartitionUpdateSerializationEnabled);
        for (@Language("SQL") String postQuery : postQueries) {
            queryRunner.execute(recoverableSession, postQuery);
        }
        try {
            for (@Language("SQL") String preQuery : preQueries) {
                queryRunner.execute(recoverableSession, preQuery);
            }

            // test no failure case
            Stopwatch noRecoveryStopwatch = Stopwatch.createStarted();
            assertEquals(queryRunner.execute(recoverableSession, queryWithoutFailure).getUpdateCount(), OptionalLong.of(expectedUpdateCount));
            log.info("Query with no recovery took %sms", noRecoveryStopwatch.elapsed(MILLISECONDS));

            // cancel all queries and tasks to make sure we are dealing only with a single running query
            cancelAllQueries(queryRunner);
            cancelAllTasks(queryRunner);

            // test failure case
            Stopwatch recoveryStopwatch = Stopwatch.createStarted();
            ListenableFuture<MaterializedResult> result = executor.submit(() -> queryRunner.execute(recoverableSession, queryWithFailure));

            List<TestingPrestoServer> workers = queryRunner.getServers().stream()
                    .filter(server -> !server.isCoordinator())
                    .collect(toList());
            shuffle(workers);

            TestingPrestoServer worker1 = workers.get(0);
            // kill worker1 right away, to make sure recoverable execution works in cases when the task hasn't been yet submitted
            worker1.stopResponding();

            // kill worker2 only after the task has been scheduled
            TestingPrestoServer worker2 = workers.get(1);
            sleep(1000);
            worker2.stopResponding();

            assertEquals(result.get(1000, SECONDS).getUpdateCount(), OptionalLong.of(expectedUpdateCount));
            log.info("Query with recovery took %sms", recoveryStopwatch.elapsed(MILLISECONDS));
        }
        finally {
            queryRunner.getServers().forEach(TestingPrestoServer::startResponding);
            cancelAllQueries(queryRunner);
            cancelAllTasks(queryRunner);
            for (@Language("SQL") String postQuery : postQueries) {
                queryRunner.execute(recoverableSession, postQuery);
            }
        }
    }

    private static void waitUntilAllNodesAreHealthy(DistributedQueryRunner queryRunner, Duration timeout)
            throws TimeoutException, InterruptedException
    {
        TestingPrestoServer coordinator = queryRunner.getCoordinator();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            AllNodes allNodes = coordinator.refreshNodes();
            if (allNodes.getActiveNodes().size() == queryRunner.getNodeCount()) {
                return;
            }
            sleep(1000);
        }
        throw new TimeoutException(format("one of the nodes is still missing after: %s", timeout));
    }

    private static void cancelAllQueries(DistributedQueryRunner queryRunner)
    {
        queryRunner.getQueries().forEach(query -> queryRunner.getCoordinator().getQueryManager().cancelQuery(query.getQueryId()));
    }

    private static void cancelAllTasks(DistributedQueryRunner queryRunner)
    {
        queryRunner.getServers().forEach(TestHiveRecoverableExecution::cancelAllTasks);
    }

    private static void cancelAllTasks(TestingPrestoServer server)
    {
        server.getTaskManager().getAllTaskInfo().forEach(task -> server.getTaskManager().cancelTask(task.getTaskId()));
    }

    private static Session createRecoverableSession(int writerConcurrency, boolean optimizedPartitionUpdateSerializationEnabled)
    {
        Identity identity = new Identity(
                "hive",
                Optional.empty(),
                Optional.of(new SelectedRole(ROLE, Optional.of("admin")))
                        .map(selectedRole -> ImmutableMap.of("hive", selectedRole))
                        .orElse(ImmutableMap.of()),
                ImmutableMap.of(),
                ImmutableMap.of());

        return testSessionBuilder()
                .setIdentity(identity)
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .setSystemProperty(RECOVERABLE_GROUPED_EXECUTION, "true")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .setSystemProperty(TASK_WRITER_COUNT, Integer.toString(writerConcurrency))
                .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, Integer.toString(writerConcurrency))
                .setSystemProperty(PARTITIONING_PROVIDER_CATALOG, "hive")
                .setSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, ALL.name())
                .setSystemProperty(HASH_PARTITION_COUNT, "11")
                .setSystemProperty(MAX_STAGE_RETRIES, "4")
                .setCatalogSessionProperty(HIVE_CATALOG, VIRTUAL_BUCKET_COUNT, "16")
                .setCatalogSessionProperty(HIVE_CATALOG, OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED, optimizedPartitionUpdateSerializationEnabled + "")
                .setCatalog(HIVE_CATALOG)
                .setSchema(TPCH_BUCKETED_SCHEMA)
                .build();
    }
}
