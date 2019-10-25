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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION_FOR_AGGREGATION;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION_FOR_ELIGIBLE_TABLE_SCANS;
import static com.facebook.presto.SystemSessionProperties.RECOVERABLE_GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static com.facebook.presto.SystemSessionProperties.SCALE_WRITERS;
import static com.facebook.presto.SystemSessionProperties.TASK_PARTITIONED_WRITER_COUNT;
import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static com.facebook.presto.execution.TaskState.RUNNING;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_BUCKETED_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.hive.HiveSessionProperties.VIRTUAL_BUCKET_COUNT;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.Thread.sleep;
import static java.util.Collections.shuffle;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveRecoverableGroupedExecution
{
    private static final Logger log = Logger.get(TestHiveRecoverableGroupedExecution.class);

    private static final int TEST_TIMEOUT = 120_000;
    private static final int INVOCATION_COUNT = 1;

    private DistributedQueryRunner queryRunner;
    private ListeningExecutorService executor;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner(
                ImmutableList.of(ORDERS),
                // extra properties
                ImmutableMap.of(
                        // task get results timeout has to be significantly higher that the task status update timeout
                        "exchange.max-error-duration", "5m",
                        // set the timeout of a single HTTP request to 1s, to make sure that the task result requests are actually failing
                        // The default is 10s, that might not be sufficient to make sure that the request fails before the recoverable execution kicks in
                        "exchange.http-client.request-timeout", "1s"),
                // extra coordinator properties
                ImmutableMap.of(
                        // set the timeout of the task update requests to something low to improve overall test latency
                        "scheduler.http-client.request-timeout", "1s",
                        // this effectively disables the retries
                        "query.remote-task.max-error-duration", "1s",
                        // allow 2 out of 4 tasks to fail
                        "max-failed-task-percentage", "0.6"),

                Optional.empty());
        executor = listeningDecorator(newCachedThreadPool());
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

    @DataProvider(name = "writerConcurrency")
    public static Object[][] writerConcurrency()
    {
        return new Object[][] {{1}, {2}};
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "writerConcurrency", invocationCount = INVOCATION_COUNT)
    public void testCreateBucketedTable(int writerConcurrency)
            throws Exception
    {
        testRecoverableGroupedExecution(
                writerConcurrency,
                ImmutableList.of(
                        "CREATE TABLE test_table1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders",
                        "CREATE TABLE test_table2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders",
                        "CREATE TABLE test_table3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders"),
                "CREATE TABLE test_success WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                        "SELECT key1, value1, key2, value2, key3, value3\n" +
                        "FROM test_table1\n" +
                        "JOIN test_table2\n" +
                        "ON key1 = key2\n" +
                        "JOIN test_table3\n" +
                        "ON key2 = key3",
                "CREATE TABLE test_failure WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                        "SELECT key1, value1, key2, value2, key3, value3\n" +
                        "FROM test_table1\n" +
                        "JOIN test_table2\n" +
                        "ON key1 = key2\n" +
                        "JOIN test_table3\n" +
                        "ON key2 = key3",
                15000,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS test_table1",
                        "DROP TABLE IF EXISTS test_table2",
                        "DROP TABLE IF EXISTS test_table3",
                        "DROP TABLE IF EXISTS test_success",
                        "DROP TABLE IF EXISTS test_failure"));
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "writerConcurrency", invocationCount = INVOCATION_COUNT)
    public void testInsertBucketedTable(int writerConcurrency)
            throws Exception
    {
        testRecoverableGroupedExecution(
                writerConcurrency,
                ImmutableList.of(
                        "CREATE TABLE test_table1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders",
                        "CREATE TABLE test_table2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders",
                        "CREATE TABLE test_table3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders",
                        "CREATE TABLE test_success (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key'], partitioned_by = ARRAY['partition_key'])",
                        "CREATE TABLE test_failure (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key'], partitioned_by = ARRAY['partition_key'])"),
                "INSERT INTO test_success\n" +
                        "SELECT key1, value1, 'foo'\n" +
                        "FROM test_table1\n" +
                        "JOIN test_table2\n" +
                        "ON key1 = key2\n" +
                        "JOIN test_table3\n" +
                        "ON key2 = key3",
                "INSERT INTO test_failure\n" +
                        "SELECT key1, value1, 'foo'\n" +
                        "FROM test_table1\n" +
                        "JOIN test_table2\n" +
                        "ON key1 = key2\n" +
                        "JOIN test_table3\n" +
                        "ON key2 = key3",
                15000,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS test_table1",
                        "DROP TABLE IF EXISTS test_table2",
                        "DROP TABLE IF EXISTS test_table3",
                        "DROP TABLE IF EXISTS test_success",
                        "DROP TABLE IF EXISTS test_failure"));
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "writerConcurrency", invocationCount = INVOCATION_COUNT)
    public void testCreateUnbucketedTableWithGroupedExecution(int writerConcurrency)
            throws Exception
    {
        testRecoverableGroupedExecution(
                writerConcurrency,
                ImmutableList.of(
                        "CREATE TABLE test_table1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders",
                        "CREATE TABLE test_table2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders",
                        "CREATE TABLE test_table3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders"),
                "CREATE TABLE test_success AS\n" +
                        "SELECT key1, value1, key2, value2, key3, value3\n" +
                        "FROM test_table1\n" +
                        "JOIN test_table2\n" +
                        "ON key1 = key2\n" +
                        "JOIN test_table3\n" +
                        "ON key2 = key3",
                "CREATE TABLE test_failure AS\n" +
                        "SELECT key1, value1, key2, value2, key3, value3\n" +
                        "FROM test_table1\n" +
                        "JOIN test_table2\n" +
                        "ON key1 = key2\n" +
                        "JOIN test_table3\n" +
                        "ON key2 = key3",
                15000,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS test_table1",
                        "DROP TABLE IF EXISTS test_table2",
                        "DROP TABLE IF EXISTS test_table3",
                        "DROP TABLE IF EXISTS test_success",
                        "DROP TABLE IF EXISTS test_failure"));
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "writerConcurrency", invocationCount = INVOCATION_COUNT)
    public void testInsertUnbucketedTableWithGroupedExecution(int writerConcurrency)
            throws Exception
    {
        testRecoverableGroupedExecution(
                writerConcurrency,
                ImmutableList.of(
                        "CREATE TABLE test_table1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders",
                        "CREATE TABLE test_table2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders",
                        "CREATE TABLE test_table3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders",
                        "CREATE TABLE test_success (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (partitioned_by = ARRAY['partition_key'])",
                        "CREATE TABLE test_failure (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (partitioned_by = ARRAY['partition_key'])"),
                "INSERT INTO test_success\n" +
                        "SELECT key1, value1, 'foo'\n" +
                        "FROM test_table1\n" +
                        "JOIN test_table2\n" +
                        "ON key1 = key2\n" +
                        "JOIN test_table3\n" +
                        "ON key2 = key3",
                "INSERT INTO test_failure\n" +
                        "SELECT key1, value1, 'foo'\n" +
                        "FROM test_table1\n" +
                        "JOIN test_table2\n" +
                        "ON key1 = key2\n" +
                        "JOIN test_table3\n" +
                        "ON key2 = key3",
                15000,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS test_table1",
                        "DROP TABLE IF EXISTS test_table2",
                        "DROP TABLE IF EXISTS test_table3",
                        "DROP TABLE IF EXISTS test_success",
                        "DROP TABLE IF EXISTS test_failure"));
    }

    @Test(timeOut = TEST_TIMEOUT, dataProvider = "writerConcurrency", invocationCount = INVOCATION_COUNT)
    public void testScanFilterProjectionOnlyQueryOnUnbucketedTable(int writerConcurrency)
            throws Exception
    {
        testRecoverableGroupedExecution(
                writerConcurrency,
                ImmutableList.of(
                        "CREATE TABLE test_table AS\n" +
                                "SELECT t.comment\n" +
                                "FROM orders\n" +
                                "CROSS JOIN UNNEST(REPEAT(comment, 10)) AS t (comment)"),
                "CREATE TABLE test_success AS\n" +
                        "SELECT comment value1 FROM test_table",
                "CREATE TABLE test_failure AS\n" +
                        "SELECT comment value1 FROM test_table",
                15000 * 10,
                ImmutableList.of(
                        "DROP TABLE IF EXISTS test_table",
                        "DROP TABLE IF EXISTS test_success",
                        "DROP TABLE IF EXISTS test_failure"));
    }

    private void testRecoverableGroupedExecution(
            int writerConcurrency,
            List<String> preQueries,
            @Language("SQL") String queryWithoutFailure,
            @Language("SQL") String queryWithFailure,
            int expectedUpdateCount,
            List<String> postQueries)
            throws Exception
    {
        Session recoverableSession = createRecoverableSession(writerConcurrency);
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
            Stopwatch stopwatch = Stopwatch.createStarted();
            while (true) {
                // wait for a while
                sleep(500);

                // if the task is already running - move ahead
                if (hasTaskRunning(worker2)) {
                    break;
                }

                // don't fail the test if task execution already finished
                if (stopwatch.elapsed(SECONDS) > 5) {
                    break;
                }
            }
            worker2.stopResponding();

            assertEquals(result.get(60, SECONDS).getUpdateCount(), OptionalLong.of(expectedUpdateCount));
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

    private static void cancelAllQueries(DistributedQueryRunner queryRunner)
    {
        queryRunner.getQueries().forEach(query -> queryRunner.getCoordinator().getQueryManager().cancelQuery(query.getQueryId()));
        queryRunner.getQueries().forEach(query -> assertTrue(query.getState().isDone()));
    }

    private static void cancelAllTasks(DistributedQueryRunner queryRunner)
    {
        queryRunner.getServers().forEach(TestHiveRecoverableGroupedExecution::cancelAllTasks);
    }

    private static void cancelAllTasks(TestingPrestoServer server)
    {
        server.getTaskManager().getAllTaskInfo().forEach(task -> server.getTaskManager().cancelTask(task.getTaskStatus().getTaskId()));
        server.getTaskManager().getAllTaskInfo().forEach(task -> assertTrue(task.getTaskStatus().getState().isDone()));
    }

    private static boolean hasTaskRunning(TestingPrestoServer server)
    {
        return server.getTaskManager().getAllTaskInfo().stream()
                .anyMatch(taskInfo -> taskInfo.getTaskStatus().getState() == RUNNING);
    }

    private static Session createRecoverableSession(int writerConcurrency)
    {
        Identity identity = new Identity(
                "hive",
                Optional.empty(),
                Optional.of(new SelectedRole(ROLE, Optional.of("admin")))
                        .map(selectedRole -> ImmutableMap.of("hive", selectedRole))
                        .orElse(ImmutableMap.of()),
                ImmutableMap.of());

        return testSessionBuilder()
                .setIdentity(identity)
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION_FOR_AGGREGATION, "true")
                .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .setSystemProperty(RECOVERABLE_GROUPED_EXECUTION, "true")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .setSystemProperty(GROUPED_EXECUTION_FOR_ELIGIBLE_TABLE_SCANS, "true")
                .setSystemProperty(TASK_WRITER_COUNT, Integer.toString(writerConcurrency))
                .setSystemProperty(TASK_PARTITIONED_WRITER_COUNT, Integer.toString(writerConcurrency))
                .setCatalogSessionProperty(HIVE_CATALOG, VIRTUAL_BUCKET_COUNT, "16")
                .setCatalog(HIVE_CATALOG)
                .setSchema(TPCH_BUCKETED_SCHEMA)
                .build();
    }
}
