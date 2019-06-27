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
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.SystemSessionProperties.DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION_FOR_AGGREGATION;
import static com.facebook.presto.SystemSessionProperties.RECOVERABLE_GROUPED_EXECUTION;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_BUCKETED_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestHiveRecoverableGroupedExecution
{
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

    @Test
    public void testRecoverableGroupedExecutionWithoutFailure()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = distributedQueryRunnerSupplier.get()) {
            try {
                queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_table1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders");
                queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_table2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders");
                queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_table3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders");
                queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_insert (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key'], partitioned_by = ARRAY['partition_key'])");

                MaterializedResult createTable = queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_create_table WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT key1, value1, key2, value2, key3, value3\n" +
                                "FROM test_table1\n" +
                                "JOIN test_table2\n" +
                                "ON key1 = key2\n" +
                                "JOIN test_table3\n" +
                                "ON key2 = key3");
                MaterializedResult insert = queryRunner.execute(
                        recoverableSession,
                        "INSERT INTO test_insert\n" +
                                "SELECT key1, value1, 'foo'\n" +
                                "FROM test_table1\n" +
                                "JOIN test_table2\n" +
                                "ON key1 = key2\n" +
                                "JOIN test_table3\n" +
                                "ON key2 = key3");

                assertEquals(createTable.getUpdateCount(), OptionalLong.of(15000));
                assertEquals(insert.getUpdateCount(), OptionalLong.of(15000));
            }
            finally {
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_table1");
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_table2");
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_table3");
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_create_table");
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_insert");
            }
        }
    }

    @Test(timeOut = 60_000)
    public void testRecoverableGroupedExecutionWithFailure()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = distributedQueryRunnerSupplier.get()) {
            try {
                queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_table1\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT orderkey key1, comment value1 FROM orders");
                queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_table2\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                                "SELECT orderkey key2, comment value2 FROM orders");
                queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_table3\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                                "SELECT orderkey key3, comment value3 FROM orders");
                queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_insert (key BIGINT, value VARCHAR, partition_key VARCHAR)\n" +
                                "WITH (bucket_count = 13, bucketed_by = ARRAY['key'], partitioned_by = ARRAY['partition_key'])");

                ListenableFuture<MaterializedResult> createTable = executor.submit(() -> queryRunner.execute(
                        recoverableSession,
                        "CREATE TABLE test_create_table WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                                "SELECT key1, value1, key2, value2, key3, value3\n" +
                                "FROM test_table1\n" +
                                "JOIN test_table2\n" +
                                "ON key1 = key2\n" +
                                "JOIN test_table3\n" +
                                "ON key2 = key3"));
                ListenableFuture<MaterializedResult> insert = executor.submit(() -> queryRunner.execute(
                        recoverableSession,
                        "INSERT INTO test_insert\n" +
                                "SELECT key1, value1, 'foo'\n" +
                                "FROM test_table1\n" +
                                "JOIN test_table2\n" +
                                "ON key1 = key2\n" +
                                "JOIN test_table3\n" +
                                "ON key2 = key3"));

                // wait for queries to start running
                while (getRunningQueryCount(queryRunner.getQueries()) != 2) {
                    sleep(10);
                }

                // wait for additional 500ms before close a worker
                sleep(500);
                assertEquals(getRunningQueryCount(queryRunner.getQueries()), 2);
                TestingPrestoServer worker = queryRunner.getServers().stream()
                        .filter(server -> !server.isCoordinator())
                        .findFirst()
                        .get();
                worker.close();

                assertEquals(createTable.get(30, SECONDS).getUpdateCount(), OptionalLong.of(15000));
                assertEquals(insert.get(30, SECONDS).getUpdateCount(), OptionalLong.of(15000));
            }
            finally {
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_table1");
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_table2");
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_table3");
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_create_table");
                queryRunner.execute(recoverableSession, "DROP TABLE IF EXISTS test_insert");
            }
        }
    }

    private static long getRunningQueryCount(List<BasicQueryInfo> basicQueryInfos)
    {
        return basicQueryInfos.stream()
                .map(BasicQueryInfo::getState)
                .filter(queryState -> queryState == RUNNING)
                .count();
    }

    private static Session createRecoverableSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(new Identity(
                        "hive",
                        Optional.empty(),
                        role.map(selectedRole -> ImmutableMap.of("hive", selectedRole))
                                .orElse(ImmutableMap.of())))
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION_FOR_AGGREGATION, "true")
                .setSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, "true")
                .setSystemProperty(RECOVERABLE_GROUPED_EXECUTION, "true")
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
