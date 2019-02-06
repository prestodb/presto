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

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION_FOR_AGGREGATION;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION_FOR_ELIGIBLE_TABLE_SCANS;
import static com.facebook.presto.SystemSessionProperties.RECOVERABLE_GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.REDISTRIBUTE_WRITES;
import static com.facebook.presto.SystemSessionProperties.SCALE_WRITERS;
import static com.facebook.presto.SystemSessionProperties.TASK_WRITER_COUNT;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_BUCKETED_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.hive.HiveSessionProperties.VIRTUAL_BUCKET_COUNT;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true, enabled = false)
public class TestHiveRecoverableGroupedExecution
{
    private static final Set<StageState> SPLIT_SCHEDULING_STARTED_STATES = ImmutableSet.of(StageState.SCHEDULING_SPLITS, StageState.SCHEDULED, StageState.RUNNING, StageState.FINISHED);

    private final Session recoverableSession;
    private final DistributedQueryRunnerSupplier distributedQueryRunnerSupplier;
    private ListeningExecutorService executor;

    @SuppressWarnings("unused")
    public TestHiveRecoverableGroupedExecution()
    {
        this(() -> createQueryRunner(
                ImmutableList.of(ORDERS),
                ImmutableMap.of("query.remote-task.max-error-duration", "1s"),
                Optional.empty()),
                createRecoverableSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))));
    }

    protected TestHiveRecoverableGroupedExecution(DistributedQueryRunnerSupplier distributedQueryRunnerSupplier, Session recoverableSession)
    {
        this.distributedQueryRunnerSupplier = requireNonNull(distributedQueryRunnerSupplier, "distributedQueryRunnerSupplier is null");
        this.recoverableSession = requireNonNull(recoverableSession, "recoverableSession is null");
    }

    @BeforeClass
    public void setUp()
    {
        executor = listeningDecorator(newCachedThreadPool());
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Test(timeOut = 60_000, enabled = false)
    public void testCreateBucketedTable()
            throws Exception
    {
        testRecoverableGroupedExecution(
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

    @Test(timeOut = 60_000, enabled = false)
    public void testInsertBucketedTable()
            throws Exception
    {
        testRecoverableGroupedExecution(
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

    @Test(timeOut = 60_000, enabled = false)
    public void testCreateUnbucketedTableWithGroupedExecution()
            throws Exception
    {
        testRecoverableGroupedExecution(
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

    @Test(timeOut = 60_000, enabled = false)
    public void testInsertUnbucketedTableWithGroupedExecution()
            throws Exception
    {
        testRecoverableGroupedExecution(
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

    @Test(timeOut = 60_000, enabled = false)
    public void testScanFilterProjectionOnlyQueryOnUnbucketedTable()
            throws Exception
    {
        testRecoverableGroupedExecution(
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
            List<String> preQueries,
            @Language("SQL") String queryWithoutFailure,
            @Language("SQL") String queryWithFailure,
            int expectedUpdateCount,
            List<String> postQueries)
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = distributedQueryRunnerSupplier.get()) {
            try {
                for (@Language("SQL") String preQuery : preQueries) {
                    queryRunner.execute(recoverableSession, preQuery);
                }

                // test no failure case
                assertEquals(queryRunner.execute(recoverableSession, queryWithoutFailure).getUpdateCount(), OptionalLong.of(expectedUpdateCount));

                // test failure case
                ListenableFuture<MaterializedResult> result = executor.submit(() -> queryRunner.execute(recoverableSession, queryWithFailure));

                // wait for split scheduling starts
                while (!isSplitSchedulingStarted(queryRunner)) {
                    sleep(10);
                }

                // wait for additional 300ms before close a worker
                sleep(300);

                assertTrue(getRunningQueryId(queryRunner.getQueries()).isPresent());
                TestingPrestoServer worker = queryRunner.getServers().stream()
                        .filter(server -> !server.isCoordinator())
                        .findFirst()
                        .get();
                worker.close();

                assertEquals(result.get(30, SECONDS).getUpdateCount(), OptionalLong.of(expectedUpdateCount));
            }
            finally {
                for (@Language("SQL") String postQuery : postQueries) {
                    queryRunner.execute(recoverableSession, postQuery);
                }
            }
        }
    }

    private boolean isSplitSchedulingStarted(DistributedQueryRunner queryRunner)
    {
        Optional<QueryId> runningQueryId = getRunningQueryId(queryRunner.getQueries());
        if (!runningQueryId.isPresent()) {
            return false;
        }

        return queryRunner.getQueryInfo(runningQueryId.get()).getOutputStage().get().getSubStages().stream()
                .map(StageInfo::getState)
                .allMatch(SPLIT_SCHEDULING_STARTED_STATES::contains);
    }

    private static Optional<QueryId> getRunningQueryId(List<BasicQueryInfo> basicQueryInfos)
    {
        return basicQueryInfos.stream()
                .filter(basicQueryInfo -> basicQueryInfo.getState() == QueryState.RUNNING)
                .map(BasicQueryInfo::getQueryId)
                .collect(toOptional());
    }

    private static Session createRecoverableSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(new Identity(
                        "hive",
                        Optional.empty(),
                        role.map(selectedRole -> ImmutableMap.of("hive", selectedRole))
                                .orElse(ImmutableMap.of()),
                        ImmutableMap.of()))
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION_FOR_AGGREGATION, "true")
                .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .setSystemProperty(RECOVERABLE_GROUPED_EXECUTION, "true")
                .setSystemProperty(SCALE_WRITERS, "false")
                .setSystemProperty(REDISTRIBUTE_WRITES, "false")
                .setSystemProperty(GROUPED_EXECUTION_FOR_ELIGIBLE_TABLE_SCANS, "true")
                .setSystemProperty(TASK_WRITER_COUNT, "1")
                .setCatalogSessionProperty(HIVE_CATALOG, VIRTUAL_BUCKET_COUNT, "16")
                .setCatalog(HIVE_CATALOG)
                .setSchema(TPCH_BUCKETED_SCHEMA)
                .build();
    }

    @FunctionalInterface
    public interface DistributedQueryRunnerSupplier
    {
        DistributedQueryRunner get()
                throws Exception;
    }
}
