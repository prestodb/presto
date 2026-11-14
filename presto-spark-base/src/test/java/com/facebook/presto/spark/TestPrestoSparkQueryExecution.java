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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskRdd;
import com.facebook.presto.spark.execution.FragmentExecutionResult;
import com.facebook.presto.spark.execution.PrestoSparkAdaptiveQueryExecution;
import com.facebook.presto.spark.execution.PrestoSparkStaticQueryExecution;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.RoutineCharacteristics.Determinism;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.QueryAssertions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.Dependency;
import org.apache.spark.MapOutputStatistics;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.CTE_PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PULL_ROW_LOCAL_CHAIN_ABOVE_EXCHANGE_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TABLE_WRITER_PARTITION_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.STORAGE_BASED_BROADCAST_JOIN_ENABLED;
import static com.facebook.presto.spark.execution.RuntimeStatistics.createRuntimeStats;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.NOT_DETERMINISTIC;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoSparkQueryExecution
        extends AbstractTestQueryFramework
{
    // task.partitioned-writer-count of the test runner
    private static final int TASK_PARTITIONED_WRITER_COUNT = 4;
    // the Hive partitioning for partitioned tables has 1009 virtual buckets
    private static final int HIVE_VIRTUAL_BUCKET_COUNT = 1009;
    private static final int TABLE_WRITER_PARTITION_COUNT = 7;

    PrestoSparkQueryRunner prestoSparkQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
    {
        prestoSparkQueryRunner = createHivePrestoSparkQueryRunner();

        MockDeleteConnector mockDelete = new MockDeleteConnector();
        prestoSparkQueryRunner.installPlugin(mockDelete.createPlugin());
        prestoSparkQueryRunner.createCatalog(MockDeleteConnector.getConnectorName(), MockDeleteConnector.getConnectorName(), ImmutableMap.of());
        mockDelete.getMetadata().createTable(
                TestingConnectorSession.SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("default", "test_delete_table"),
                        ImmutableList.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build())),
                false);

        return prestoSparkQueryRunner;
    }

    private IPrestoSparkQueryExecution getPrestoSparkQueryExecution(Session session, String sql)
    {
        return prestoSparkQueryRunner.createPrestoSparkQueryExecution(session, sql, ImmutableList.of());
    }

    @Test
    public void testQueryExecutionCreation()
    {
        String sqlText = "select * from lineitem";
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                .build();
        IPrestoSparkQueryExecution psQueryExecution = getPrestoSparkQueryExecution(session, sqlText);
        assertTrue(psQueryExecution instanceof PrestoSparkStaticQueryExecution);

        session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .build();
        psQueryExecution = getPrestoSparkQueryExecution(session, sqlText);
        assertTrue(psQueryExecution instanceof PrestoSparkAdaptiveQueryExecution);
    }

    @Test
    public void testSingleFragmentQueryAdaptiveExecution()
    {
        String sqlText = "select * from lineitem";
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                .build();
        MaterializedResult staticResults = prestoSparkQueryRunner.execute(session, sqlText);

        session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .build();
        MaterializedResult dynamicResults = prestoSparkQueryRunner.execute(session, sqlText);

        QueryAssertions.assertEqualsIgnoreOrder(staticResults, dynamicResults);
    }

    @Test
    public void testJoinQueryAdaptiveExecution()
    {
        String sqlText = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        MaterializedResult staticResults = prestoSparkQueryRunner.execute(session, sqlText);

        session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        MaterializedResult dynamicResults = prestoSparkQueryRunner.execute(session, sqlText);

        QueryAssertions.assertEqualsIgnoreOrder(staticResults, dynamicResults);
    }

    @Test
    public void testGroupByAdaptiveExecution()
    {
        String sqlText = "SELECT custkey, orderstatus FROM orders ORDER BY orderkey DESC LIMIT 10";
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        MaterializedResult staticResults = prestoSparkQueryRunner.execute(session, sqlText);

        session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        MaterializedResult dynamicResults = prestoSparkQueryRunner.execute(session, sqlText);

        QueryAssertions.assertEqualsIgnoreOrder(staticResults, dynamicResults);
    }

    @Test
    public void testRddCreationForPartitionedJoinWithoutShuffle()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        validateFragmentedRddCreation(session, sql);
    }

    @Test
    public void testRddCreationForPartitionedJoinWithShuffle()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey UNION ALL select * from lineitem l join orders o on l.orderkey = o.orderkey";
        validateFragmentedRddCreation(session, sql);
    }

    @Test
    public void testRddCreationForMemoryBasedBroadcastJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "broadcast")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "false")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        validateFragmentedRddCreation(session, sql);
    }

    @Test
    public void testRddCreationForStorageBasedBroadcastJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "broadcast")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .build();
        String sql = "select * from lineitem l join orders o on l.orderkey = o.orderkey";
        validateFragmentedRddCreation(session, sql);
    }

    @Test
    public void testRddCreationForDelete()
    {
        Session session = Session.builder(getSession())
                .setCatalog(MockDeleteConnector.getConnectorName())
                .setSchema("default")
                .build();

        String sql = "DELETE FROM test_delete_table WHERE id > 5";
        IPrestoSparkQueryExecution queryExecution = getPrestoSparkQueryExecution(session, sql);
        assertTrue(queryExecution instanceof PrestoSparkStaticQueryExecution);
        PrestoSparkStaticQueryExecution sparkQueryExecution = (PrestoSparkStaticQueryExecution) queryExecution;
        SubPlan rootPlanFragment = sparkQueryExecution.createFragmentedPlan();
        Session executionSession = sparkQueryExecution.getSession();
        TableWriteInfo writeInfo = sparkQueryExecution.getTableWriteInfo(executionSession, rootPlanFragment.getFragment().getRoot());
        assertTrue(writeInfo.getWriterTarget().isPresent());
        assertTrue(writeInfo.getWriterTarget().get() instanceof ExecutionWriterTarget.DeleteHandle);
    }

    @Test
    public void testTableWriterPartitionCountForPartitionedTable()
    {
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_partitioned WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders";

        SubPlan writerFragment = getTableWriterFragment(sql, Optional.empty());
        assertFalse(writerFragment.getFragment().getTableScanSchedulingOrder().isEmpty(), "table writer is expected to run in the table scan fragment");

        PrestoSparkStaticQueryExecution execution = createQueryExecution(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT), ImmutableMap.of());
        writerFragment = getOnlyElement(getTableWriterFragments(execution.createFragmentedPlan()));
        assertTrue(writerFragment.getFragment().getTableScanSchedulingOrder().isEmpty(), "table writer is expected to read a shuffle");
        PartitioningScheme writerInput = getWriterInput(writerFragment);
        assertTrue(writerInput.getPartitioning().getHandle().getConnectorId().isPresent());
        assertEquals(getOnlyElement(writerInput.getPartitioning().getVariableReferences()).getName(), "orderstatus");
        assertBucketToPartition(writerInput, HIVE_VIRTUAL_BUCKET_COUNT, TABLE_WRITER_PARTITION_COUNT);
        assertFalse(hasNoEffectWarning(execution));

        assertBucketToPartition(getTableWriterInput(sql, Optional.of(1)), HIVE_VIRTUAL_BUCKET_COUNT, 1);
    }

    @Test
    public void testTableWriterPartitionCountOnNativeWorkers()
    {
        // native workers assign the buckets of a task to its writer drivers in their own order, so the buckets are dealt
        // as for a single writer driver
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_partitioned WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders";
        int[] nativeBucketToPartition = getBucketToPartition(sql, ImmutableMap.of(NATIVE_EXECUTION_ENABLED, "true"));
        assertTrue(Arrays.equals(nativeBucketToPartition, getBucketToPartition(sql, ImmutableMap.of(SystemSessionProperties.TASK_PARTITIONED_WRITER_COUNT, "1"))));
        assertFalse(Arrays.equals(nativeBucketToPartition, getBucketToPartition(sql, ImmutableMap.of())));
    }

    @Test
    public void testTableWriterPartitionCountForBucketedTable()
    {
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_bucketed WITH (bucketed_by = ARRAY['orderkey'], bucket_count = 13) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders";

        // ceil(13 / task_partitioned_writer_count) partitions when the property is not set, with task_partitioned_writer_count 4 in the test runner
        assertEquals(getPartitionCount(getTableWriterInput(sql, Optional.empty())), 4);
        assertBucketToPartition(getTableWriterInput(sql, Optional.of(2)), 13, 2);
        assertBucketToPartition(getTableWriterInput(sql, Optional.of(100)), 13, 13);
        assertBucketToPartition(getTableWriterInput(sql, Optional.of(1)), 13, 1);
    }

    @Test
    public void testTableWriterPartitionCountForUnpartitionedTable()
    {
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_unpartitioned AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders";

        PrestoSparkStaticQueryExecution execution = createQueryExecution(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT), ImmutableMap.of());
        SubPlan plan = execution.createFragmentedPlan();
        assertEquals(plan.getAllFragments().size(), createFragmentedPlan(sql, Optional.empty()).getAllFragments().size());
        assertFalse(getOnlyElement(getTableWriterFragments(plan)).getFragment().getTableScanSchedulingOrder().isEmpty());
        assertTrue(hasNoEffectWarning(execution));
    }

    @Test
    public void testTableWriterPartitionCountForUnionAll()
    {
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_union WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders UNION ALL SELECT orderkey, custkey, orderstatus FROM orders";

        SubPlan plan = createFragmentedPlan(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT));
        // The writer is pushed into each branch of the union, and each branch is shuffled directly on the table partitioning.
        // A round robin exchange over the union would route rows differently in a retried task.
        assertFalse(
                flatten(plan).anyMatch(subPlan -> subPlan.getFragment().getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION)),
                "no fragment is expected to have round robin output");
        SubPlan writerFragment = getOnlyElement(getTableWriterFragments(plan));
        assertEquals(searchFrom(writerFragment.getFragment().getRoot()).where(TableWriterNode.class::isInstance).findAll().size(), 2);
        assertEquals(writerFragment.getChildren().size(), 2);
        for (SubPlan branch : writerFragment.getChildren()) {
            assertFalse(branch.getFragment().getTableScanSchedulingOrder().isEmpty(), "each branch is expected to be shuffled from its table scan");
            assertBucketToPartition(branch.getFragment().getPartitioningScheme(), HIVE_VIRTUAL_BUCKET_COUNT, TABLE_WRITER_PARTITION_COUNT);
        }
    }

    @Test
    public void testTableWriterPartitionCountForNonDeterministicPartitionColumn()
    {
        // a recomputed map task would compute other partition values and route rows to other writers than the first attempt
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_random WITH (partitioned_by = ARRAY['shard']) AS " +
                "SELECT orderkey, shard FROM orders CROSS JOIN UNNEST(ARRAY[CAST(floor(rand() * 4) AS BIGINT)]) AS t (shard)");
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_random WITH (partitioned_by = ARRAY['shard']) AS " +
                "SELECT orderkey, custkey, CAST(floor(rand() * 4) AS BIGINT) AS shard FROM orders");
    }

    @Test
    public void testTableWriterPartitionCountForPartitionColumnFromAggregate()
    {
        // only table columns and deterministic projections of them count as deterministic
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_aggregate WITH (partitioned_by = ARRAY['order_count']) AS " +
                "SELECT custkey, CAST(count(*) AS VARCHAR) AS order_count FROM orders GROUP BY custkey");
    }

    @Test
    public void testTableWriterPartitionCountForRowsSelectedByOrder()
    {
        // a recomputed map task could select other rows than the first attempt
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM (" +
                "SELECT orderkey, custkey, orderstatus, row_number() OVER (PARTITION BY custkey ORDER BY orderdate) AS row_number FROM orders) " +
                "WHERE row_number = 1");
    }

    @Test
    public void testTableWriterPartitionCountForAggregateWindowFunctions()
    {
        // the values can depend on the order of rows: over a ROWS frame with ties in the window order, and in the array that array_agg builds
        for (String windowFunction : ImmutableList.of(
                "sum(totalprice) OVER (PARTITION BY custkey ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                "array_agg(orderkey) OVER (PARTITION BY custkey)")) {
            assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_window WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                    "SELECT orderkey, " + windowFunction + " AS window_value, orderstatus FROM orders");
        }
    }

    @Test
    public void testTableWriterPartitionCountForRowsSelectedByLimit()
    {
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders LIMIT 100");
    }

    @Test
    public void testTableWriterPartitionCountForRowsSelectedAtRandom()
    {
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders WHERE rand() < 0.5");
    }

    @Test
    public void testTableWriterPartitionCountForRandomJoinCondition()
    {
        // a recomputed join task could null-extend other orders than the first attempt
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['mktsegment']) AS " +
                "SELECT o.orderkey, c.mktsegment FROM orders o LEFT JOIN customer c ON o.custkey = c.custkey AND rand() < 0.5");
    }

    @Test
    public void testTableWriterPartitionCountForRandomFilterOfJoin()
    {
        // the filter becomes a filter of the join
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders JOIN customer USING (custkey) WHERE rand() < 0.5");
    }

    @Test
    public void testTableWriterPartitionCountForFilterOnRandomValue()
    {
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM (SELECT orderkey, custkey, orderstatus, rand() AS r FROM orders) WHERE r < 0.5");
    }

    @Test
    public void testTableWriterPartitionCountForTableSample()
    {
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders TABLESAMPLE BERNOULLI (50)");
    }

    @Test
    public void testTableWriterPartitionCountForRandomValueCopiedToPartitions()
    {
        // a recomputed task would give the copies of an order on some writers another id than on the others
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_copied WITH (partitioned_by = ARRAY['shard']) AS " +
                "SELECT orderkey, id, shard FROM (SELECT orderkey, CAST(uuid() AS VARCHAR) AS id FROM orders) CROSS JOIN UNNEST(ARRAY[1, 2, 3, 4]) AS t (shard)");
    }

    @Test
    public void testTableWriterPartitionCountForSqlFunctionWithRandomBody()
    {
        // CREATE FUNCTION does not check the declared determinism against the body
        createSqlFunction("twpc_declared_deterministic_random", ImmutableList.of(), "double", DETERMINISTIC, "RETURN rand()");
        createSqlFunction("twpc_nested_random", ImmutableList.of(), "double", DETERMINISTIC, "RETURN unittest.memory.twpc_declared_deterministic_random()");
        for (String function : ImmutableList.of("unittest.memory.twpc_declared_deterministic_random()", "unittest.memory.twpc_nested_random()")) {
            assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                    "SELECT orderkey, custkey, orderstatus FROM orders WHERE " + function + " < 0.5");
        }
    }

    @Test
    public void testTableWriterPartitionCountForSqlFunctionWithDeterministicBody()
    {
        // as by CREATE FUNCTION without DETERMINISTIC
        createSqlFunction("twpc_plus_one", ImmutableList.of(new Parameter("x", BIGINT.getTypeSignature())), "bigint", NOT_DETERMINISTIC, "RETURN x + 1");
        createSqlFunction(
                "twpc_plus_two",
                ImmutableList.of(new Parameter("x", BIGINT.getTypeSignature())),
                "bigint",
                NOT_DETERMINISTIC,
                "RETURN unittest.memory.twpc_plus_one(unittest.memory.twpc_plus_one(x))");
        for (String function : ImmutableList.of("unittest.memory.twpc_plus_one(orderkey)", "unittest.memory.twpc_plus_two(orderkey)")) {
            String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_sql_function WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                    "SELECT " + function + " AS key, custkey, orderstatus FROM orders";
            assertBucketToPartition(getTableWriterInput(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT)), HIVE_VIRTUAL_BUCKET_COUNT, TABLE_WRITER_PARTITION_COUNT);
        }
    }

    @Test
    public void testTableWriterPartitionCountForOrderSensitiveAggregateJoinedBack()
    {
        // a recomputed task could build the array of a customer in another order, so that the orders of the customer on
        // different writers would carry different arrays
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_joined WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT o.orderkey, a.orderkeys, o.orderstatus FROM orders o " +
                "JOIN (SELECT custkey, array_agg(orderkey) AS orderkeys FROM orders GROUP BY custkey) a ON o.custkey = a.custkey");
    }

    @Test
    public void testTableWriterPartitionCountForOrderSensitiveAggregateCopiedByUnnest()
    {
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_copied WITH (partitioned_by = ARRAY['shard']) AS " +
                "SELECT custkey, orderkeys, shard FROM (SELECT custkey, array_agg(orderkey) AS orderkeys FROM orders GROUP BY custkey) " +
                "CROSS JOIN UNNEST(ARRAY[1, 2]) AS t (shard)");
    }

    @Test
    public void testTableWriterPartitionCountForTieDependentAggregateJoinedBack()
    {
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_joined WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT o.orderkey, l.latest_orderkey, o.orderstatus FROM orders o " +
                "JOIN (SELECT custkey, max_by(orderkey, orderdate) AS latest_orderkey FROM orders GROUP BY custkey) l ON o.custkey = l.custkey");
    }

    @Test
    public void testTableWriterPartitionCountForTieDependentAggregateInSemiJoin()
    {
        // with ties in orderdate, a recomputed task could select another latest order of a customer
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_selected WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders WHERE orderkey IN (SELECT max_by(orderkey, orderdate) FROM orders GROUP BY custkey)");
    }

    @Test
    public void testTableWriterPartitionCountForOrderDependentAggregateInGroupingSets()
    {
        // every grouping set gets a copy of the value
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_grouped WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT custkey, max(any_orderkey) AS max_orderkey, orderstatus " +
                "FROM (SELECT custkey, orderstatus, arbitrary(orderkey) AS any_orderkey FROM orders GROUP BY custkey, orderstatus) " +
                "GROUP BY ROLLUP (custkey, orderstatus)");
    }

    @Test
    public void testTableWriterPartitionCountForOrderSensitiveAggregatePerGroup()
    {
        // every group is one row, which one writer writes
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_grouped WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT custkey, array_agg(orderkey) AS orderkeys, orderstatus FROM orders GROUP BY custkey, orderstatus";
        assertBucketToPartition(getTableWriterInput(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT)), HIVE_VIRTUAL_BUCKET_COUNT, TABLE_WRITER_PARTITION_COUNT);
    }

    @Test
    public void testTableWriterPartitionCountForRandomAggregateJoinedBack()
    {
        // a recomputed task would add other noise, so that the orders of a customer on different writers would carry different counts
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_joined WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT o.orderkey, n.noisy_order_count, o.orderstatus FROM orders o " +
                "JOIN (SELECT custkey, noisy_count_gaussian(orderkey, 1.0) AS noisy_order_count FROM orders GROUP BY custkey) n ON o.custkey = n.custkey");
    }

    @Test
    public void testTableWriterPartitionCountForApproximateAggregateJoinedBack()
    {
        // a recomputed task could compute another median, so that the writers would select orders by different medians
        assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_joined WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT o.orderkey, o.orderstatus FROM orders o " +
                "JOIN (SELECT custkey, approx_percentile(totalprice, 0.5) AS median_price FROM orders GROUP BY custkey) p " +
                "ON o.custkey = p.custkey AND o.totalprice > p.median_price");
    }

    @Test
    public void testTableWriterPartitionCountForOrderIndependentAggregatesJoinedBack()
    {
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_joined WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT o.orderkey, a.order_count, a.orderkey_sum, a.distinct_clerks, a.order_checksum, a.max_price, o.orderstatus FROM orders o " +
                "JOIN (SELECT custkey, count(*) AS order_count, sum(orderkey) AS orderkey_sum, approx_distinct(clerk) AS distinct_clerks, " +
                "checksum(orderkey) AS order_checksum, max(totalprice) AS max_price, max(orderdate) AS latest_orderdate FROM orders GROUP BY custkey) a " +
                "ON o.custkey = a.custkey AND o.orderdate = a.latest_orderdate";
        assertBucketToPartition(getTableWriterInput(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT)), HIVE_VIRTUAL_BUCKET_COUNT, TABLE_WRITER_PARTITION_COUNT);
    }

    @Test
    public void testTableWriterPartitionCountForFloatingPointAggregateJoinedBack()
    {
        // a recomputed task could add the values in another order, so that the orders of a customer on different writers
        // would carry sums that differ in the last bits
        for (String aggregate : ImmutableList.of("sum(totalprice)", "avg(totalprice)", "avg(orderkey)")) {
            assertTableWriteNotShuffled("CREATE TABLE hive.hive_test.test_table_writer_partition_count_joined WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                    "SELECT o.orderkey, a.value, o.orderstatus FROM orders o " +
                    "JOIN (SELECT custkey, " + aggregate + " AS value FROM orders GROUP BY custkey) a ON o.custkey = a.custkey");
        }
    }

    @Test
    public void testTableWriterPartitionCountForTemporaryTableWriter()
    {
        // the writer of the materialized CTE is not a write of the query, so it keeps its partition count
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_cte AS " +
                "WITH t AS (SELECT orderkey, custkey FROM orders) SELECT a.orderkey, b.custkey FROM t a JOIN t b ON a.orderkey = b.orderkey";
        Map<String, String> cteMaterialization = ImmutableMap.of(CTE_MATERIALIZATION_STRATEGY, "ALL", CTE_PARTITIONING_PROVIDER_CATALOG, "hive");
        List<SubPlan> expectedWriterFragments = getTableWriterFragments(createQueryExecution(sql, Optional.empty(), cteMaterialization).createFragmentedPlan());
        assertTrue(expectedWriterFragments.stream()
                        .anyMatch(fragment -> searchFrom(fragment.getFragment().getRoot())
                                .where(node -> node instanceof TableWriterNode && ((TableWriterNode) node).getIsTemporaryTableWriter().orElse(false))
                                .matches()),
                "the CTE is expected to be materialized");
        PrestoSparkStaticQueryExecution execution = createQueryExecution(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT), cteMaterialization);
        List<SubPlan> writerFragments = getTableWriterFragments(execution.createFragmentedPlan());
        assertEquals(getWriterInputBucketToPartitions(writerFragments), getWriterInputBucketToPartitions(expectedWriterFragments));
        assertTrue(hasNoEffectWarning(execution));
    }

    @Test
    public void testTableWriterPartitionCountForPartitionColumnsThroughAggregation()
    {
        for (String groupBy : ImmutableList.of("custkey, orderstatus", "ROLLUP (custkey, orderstatus)")) {
            String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_grouped WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                    "SELECT custkey, count(*) AS order_count, orderstatus FROM orders GROUP BY " + groupBy;
            assertBucketToPartition(getTableWriterInput(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT)), HIVE_VIRTUAL_BUCKET_COUNT, TABLE_WRITER_PARTITION_COUNT);
        }
    }

    @Test
    public void testTableWriterPartitionCountForPartitionColumnsThroughUnnest()
    {
        for (String unnest : ImmutableList.of("UNNEST(ARRAY[orderstatus]) AS t (p)", "UNNEST(ARRAY[orderstatus]) WITH ORDINALITY AS t (status, p)")) {
            String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_unnested WITH (partitioned_by = ARRAY['p']) AS " +
                    "SELECT orderkey, p FROM orders CROSS JOIN " + unnest;
            assertBucketToPartition(getTableWriterInput(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT)), HIVE_VIRTUAL_BUCKET_COUNT, TABLE_WRITER_PARTITION_COUNT);
        }
    }

    @Test
    public void testTableWriterPartitionCountForUnsupportedPartitionColumnType()
    {
        // the Hive bucket function cannot hash these types, so a shuffle on the partition columns would fail
        for (String partitionColumn : ImmutableList.of("CAST(shippriority AS DECIMAL(10, 2))", "CAST(orderstatus AS CHAR(1))")) {
            assertTableWriteNotShuffled(format("CREATE TABLE hive.hive_test.test_table_writer_partition_count_unsupported_type WITH (partitioned_by = ARRAY['p']) AS " +
                    "SELECT orderkey, %s AS p FROM orders", partitionColumn));
        }
    }

    @Test
    public void testTableWriterPartitionCountWarningOnlyForTableWrites()
    {
        assertFalse(hasNoEffectWarning(createQueryExecution("SELECT 1", Optional.of(TABLE_WRITER_PARTITION_COUNT), ImmutableMap.of())));
        assertFalse(hasNoEffectWarning(createQueryExecution(
                "EXPLAIN CREATE TABLE hive.hive_test.test_table_writer_partition_count_explain WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                        "SELECT orderkey, custkey, orderstatus FROM orders",
                Optional.of(TABLE_WRITER_PARTITION_COUNT),
                ImmutableMap.of())));
    }

    @Test
    public void testTableWriterPartitionCountForColocatedBucketedWrite()
    {
        // orders_bucketed has the same bucketing, so the write runs in the table scan fragment without a shuffle
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_colocated WITH (bucketed_by = ARRAY['orderkey'], bucket_count = 11) AS " +
                "SELECT orderkey, custkey, orderstatus FROM orders_bucketed";

        SubPlan expectedPlan = createFragmentedPlan(sql, Optional.empty());
        PrestoSparkStaticQueryExecution execution = createQueryExecution(sql, Optional.of(2), ImmutableMap.of());
        SubPlan plan = execution.createFragmentedPlan();
        assertEquals(plan.getAllFragments().size(), expectedPlan.getAllFragments().size());
        SubPlan writerFragment = getOnlyElement(getTableWriterFragments(plan));
        assertFalse(writerFragment.getFragment().getTableScanSchedulingOrder().isEmpty());
        assertEquals(
                writerFragment.getFragment().getPartitioning().getConnectorHandle(),
                getOnlyElement(getTableWriterFragments(expectedPlan)).getFragment().getPartitioning().getConnectorHandle());
        assertTrue(hasNoEffectWarning(execution));
    }

    @Test
    public void testTableWriterPartitionCountWithRowLocalChainAboveExchange()
    {
        String sql = "CREATE TABLE hive.hive_test.test_table_writer_partition_count_unnest WITH (partitioned_by = ARRAY['orderstatus']) AS " +
                "SELECT orderkey, key, orderstatus FROM orders CROSS JOIN UNNEST(ARRAY[orderkey, custkey]) AS t (key)";

        // the unnest is pulled above the exchange that feeds the writer, so it runs between the writer and that exchange
        SubPlan writerFragment = getOnlyElement(getTableWriterFragments(
                createQueryExecution(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT), ImmutableMap.of(PULL_ROW_LOCAL_CHAIN_ABOVE_EXCHANGE_STRATEGY, "ALWAYS_ENABLED")).createFragmentedPlan()));
        assertTrue(searchFrom(writerFragment.getFragment().getRoot()).where(UnnestNode.class::isInstance).matches(), "unnest is expected to run in the table writer fragment");
        assertBucketToPartition(getWriterInput(writerFragment), HIVE_VIRTUAL_BUCKET_COUNT, TABLE_WRITER_PARTITION_COUNT);
    }

    @Test
    public void testMapOutputStatsExtraction()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "broadcast")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();
        Optional<PlanNodeStatsEstimate> planNodeStatsEstimate;

        // Empty stats case
        planNodeStatsEstimate = createRuntimeStats(Optional.empty());
        assertFalse(planNodeStatsEstimate.isPresent());

        // Empty partition array
        planNodeStatsEstimate = createRuntimeStats(Optional.of(new MapOutputStatistics(0, new long[] {})));
        assertEquals(planNodeStatsEstimate.get().getOutputSizeInBytes(), 0);

        // One partition case
        planNodeStatsEstimate = createRuntimeStats(Optional.of(new MapOutputStatistics(0, new long[] {23})));
        assertEquals(planNodeStatsEstimate.get().getOutputSizeInBytes(), 23);

        // Multiple partition case
        planNodeStatsEstimate = createRuntimeStats(Optional.of(new MapOutputStatistics(0, new long[] {23, 520, 190})));
        assertEquals(planNodeStatsEstimate.get().getOutputSizeInBytes(), 733);
    }

    private SubPlan createFragmentedPlan(String sql, Optional<Integer> tableWriterPartitionCount)
    {
        return createQueryExecution(sql, tableWriterPartitionCount, ImmutableMap.of()).createFragmentedPlan();
    }

    private PrestoSparkStaticQueryExecution createQueryExecution(String sql, Optional<Integer> tableWriterPartitionCount, Map<String, String> systemProperties)
    {
        return createQueryExecution(sql, tableWriterPartitionCount, systemProperties, false);
    }

    private PrestoSparkStaticQueryExecution createQueryExecution(String sql, Optional<Integer> tableWriterPartitionCount, Map<String, String> systemProperties, boolean pushdownFilter)
    {
        Session.SessionBuilder session = Session.builder(getSession())
                .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false");
        tableWriterPartitionCount.ifPresent(partitionCount -> session.setSystemProperty(SPARK_TABLE_WRITER_PARTITION_COUNT, String.valueOf(partitionCount)));
        systemProperties.forEach(session::setSystemProperty);
        if (pushdownFilter) {
            session.setCatalogSessionProperty("hive", PUSHDOWN_FILTER_ENABLED, "true");
        }
        return (PrestoSparkStaticQueryExecution) getPrestoSparkQueryExecution(session.build(), sql);
    }

    private SubPlan getTableWriterFragment(String sql, Optional<Integer> tableWriterPartitionCount)
    {
        return getOnlyElement(getTableWriterFragments(createFragmentedPlan(sql, tableWriterPartitionCount)));
    }

    private PartitioningScheme getTableWriterInput(String sql, Optional<Integer> tableWriterPartitionCount)
    {
        return getWriterInput(getTableWriterFragment(sql, tableWriterPartitionCount));
    }

    private static List<SubPlan> getTableWriterFragments(SubPlan plan)
    {
        return flatten(plan)
                .filter(subPlan -> searchFrom(subPlan.getFragment().getRoot()).where(TableWriterNode.class::isInstance).matches())
                .collect(toImmutableList());
    }

    private int[] getBucketToPartition(String sql, Map<String, String> systemProperties)
    {
        return getWriterInput(getOnlyElement(getTableWriterFragments(createQueryExecution(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT), systemProperties).createFragmentedPlan())))
                .getBucketToPartition()
                .get();
    }

    private static List<List<Integer>> getWriterInputBucketToPartitions(List<SubPlan> writerFragments)
    {
        return writerFragments.stream()
                .flatMap(fragment -> fragment.getChildren().stream())
                .map(child -> child.getFragment().getPartitioningScheme().getBucketToPartition()
                        .map(bucketToPartition -> IntStream.of(bucketToPartition).boxed().collect(toImmutableList()))
                        .orElse(ImmutableList.of()))
                .collect(toImmutableList());
    }

    private static PartitioningScheme getWriterInput(SubPlan writerFragment)
    {
        return getOnlyElement(writerFragment.getChildren()).getFragment().getPartitioningScheme();
    }

    private static Stream<SubPlan> flatten(SubPlan subPlan)
    {
        return Stream.concat(Stream.of(subPlan), subPlan.getChildren().stream().flatMap(TestPrestoSparkQueryExecution::flatten));
    }

    private void createSqlFunction(String name, List<Parameter> parameters, String returnType, Determinism determinism, String body)
    {
        // Presto on Spark does not run CREATE FUNCTION
        prestoSparkQueryRunner.getMetadata().getFunctionAndTypeManager().createFunction(
                new SqlInvokedFunction(
                        QualifiedObjectName.valueOf("unittest.memory." + name),
                        parameters,
                        parseTypeSignature(returnType),
                        name,
                        RoutineCharacteristics.builder().setDeterminism(determinism).build(),
                        body,
                        notVersioned()),
                true);
    }

    private void assertTableWriteNotShuffled(String sql)
    {
        // with filter pushdown, Hive folds the filters of the query into its table scans
        for (boolean pushdownFilter : ImmutableList.of(false, true)) {
            PrestoSparkStaticQueryExecution execution = createQueryExecution(sql, Optional.of(TABLE_WRITER_PARTITION_COUNT), ImmutableMap.of(), pushdownFilter);
            assertFalse(
                    flatten(execution.createFragmentedPlan()).anyMatch(subPlan -> subPlan.getFragment().getPartitioningScheme().getPartitioning().getHandle().getConnectorId().isPresent()),
                    format("table write is not expected to be shuffled on the table partitioning (filter pushdown %s): %s", pushdownFilter, sql));
            assertTrue(hasNoEffectWarning(execution), sql);
        }
    }

    private static boolean hasNoEffectWarning(PrestoSparkStaticQueryExecution execution)
    {
        return execution.getSession().getWarningCollector().getWarnings().stream()
                .anyMatch(warning -> warning.getMessage().contains(SPARK_TABLE_WRITER_PARTITION_COUNT));
    }

    private static int getPartitionCount(PartitioningScheme writerInput)
    {
        return IntStream.of(writerInput.getBucketToPartition().get()).max().getAsInt() + 1;
    }

    private static void assertBucketToPartition(PartitioningScheme writerInput, int bucketCount, int partitionCount)
    {
        assertTrue(writerInput.getBucketToPartition().isPresent(), "bucketToPartition is expected to be assigned");
        int[] bucketToPartition = writerInput.getBucketToPartition().get();
        assertEquals(bucketToPartition.length, bucketCount);
        // the partitions are numbered from 0 and own nearly equal numbers of whole buckets
        Map<Integer, Long> bucketsPerPartition = IntStream.of(bucketToPartition).boxed().collect(groupingBy(identity(), counting()));
        assertEquals(bucketsPerPartition.keySet(), IntStream.range(0, partitionCount).boxed().collect(toImmutableSet()));
        bucketsPerPartition.values().forEach(buckets -> assertTrue(
                buckets == bucketCount / partitionCount || buckets == (bucketCount + partitionCount - 1) / partitionCount,
                bucketsPerPartition.toString()));
        // the Java local exchange of a writer task sends bucket b to writer driver b % task_partitioned_writer_count
        int[][] bucketsPerDriver = new int[partitionCount][TASK_PARTITIONED_WRITER_COUNT];
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            bucketsPerDriver[bucketToPartition[bucket]][bucket % TASK_PARTITIONED_WRITER_COUNT]++;
        }
        for (int[] drivers : bucketsPerDriver) {
            IntSummaryStatistics statistics = IntStream.of(drivers).summaryStatistics();
            assertTrue(statistics.getMax() - statistics.getMin() <= 1, "buckets per writer driver: " + Arrays.toString(drivers));
        }
    }

    private void validateFragmentedRddCreation(Session session, String sql)
    {
        PrestoSparkStaticQueryExecution execution = (PrestoSparkStaticQueryExecution) getPrestoSparkQueryExecution(session, sql);
        RddAndMore rddAndMoreStatic = null;
        RddAndMore rddAndMoreFromFragmentedExecution = null;
        try {
            SubPlan rootFragmentedPlan = execution.createFragmentedPlan();
            TableWriteInfo tableWriteInfo = execution.getTableWriteInfo(session, rootFragmentedPlan);
            rddAndMoreStatic = execution.createRdd(rootFragmentedPlan, PrestoSparkSerializedPage.class, tableWriteInfo);
            FragmentExecutionResult fragmentExecutionResult = executeInStages(execution, rootFragmentedPlan, tableWriteInfo);
            rddAndMoreFromFragmentedExecution = fragmentExecutionResult.getRddAndMore();
        }
        catch (Exception e) {
            fail("Failed while creating RDD", e);
        }

        assertRddAndMoreEquals(rddAndMoreStatic, rddAndMoreFromFragmentedExecution);
    }

    // For testing purpose, execute plan by executing sub-plans starting at(in the same order) :
    // 1. Level 2
    // 2. Level 1
    // 3. level 0
    // For plans with more than 3 levels, it will execute child lvels in same step
    // todo - this can be updated in future with methods in Adaptive execution to get subplans at shuffle boundaries
    private FragmentExecutionResult executeInStages(PrestoSparkStaticQueryExecution execution,
            SubPlan rootFragmentedPlan,
            TableWriteInfo tableWriteInfo)
    {
        // Level 2
        rootFragmentedPlan.getChildren().stream()
                .map(SubPlan::getChildren)
                .flatMap(Collection::stream)
                .forEach(subPlan -> excecuteSubPlanWithUncheckedException(execution, subPlan, tableWriteInfo, Optional.empty()));

        // Level 1
        rootFragmentedPlan.getChildren().stream()
                .forEach(subPlan -> excecuteSubPlanWithUncheckedException(execution, subPlan, tableWriteInfo, Optional.empty()));

        // Level 0 - Root
        return excecuteSubPlanWithUncheckedException(execution, rootFragmentedPlan, tableWriteInfo, Optional.empty());
    }

    // This exists as forEach can't handle methods with checked exception
    private FragmentExecutionResult excecuteSubPlanWithUncheckedException(PrestoSparkStaticQueryExecution execution,
            SubPlan subPlan,
            TableWriteInfo tableWriteInfo,
            Optional<Class<?>> outputType)
    {
        try {
            return execution.executeFragment(subPlan, tableWriteInfo, outputType);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private void assertRddAndMoreEquals(RddAndMore rddAndMore1, RddAndMore rddAndMore2)
    {
        assertEquals(rddAndMore1.getBroadcastDependencies().size(), rddAndMore2.getBroadcastDependencies().size());
        assertRddEquals(rddAndMore1.getRdd().rdd(), rddAndMore2.getRdd().rdd());
    }

    private void assertRddEquals(RDD rdd1, RDD rdd2)
    {
        assertEquals(rdd1.name(), rdd2.name());
        assertEquals(rdd1.getClass(), rdd2.getClass());
        assertEquals(rdd1.getDependencies().size(), rdd2.getDependencies().size());
        assertEquals(rdd1.getNumPartitions(), rdd2.getNumPartitions());

        // type specific assertions
        if (rdd1 instanceof PrestoSparkTaskRdd) {
            assertPrestoSparkTaskRddEquals((PrestoSparkTaskRdd) rdd1, (PrestoSparkTaskRdd) rdd2);
        }
        else if (rdd1 instanceof ShuffledRDD) {
            assertShuffledRddEquals((ShuffledRDD) rdd1, (ShuffledRDD) rdd2);
        }
    }

    private void assertShuffledRddEquals(ShuffledRDD shuffledRDD1, ShuffledRDD shuffledRDD2)
    {
        assertEquals(shuffledRDD1.getNumPartitions(), shuffledRDD2.getNumPartitions());
        assertRddEquals(shuffledRDD1.prev(), shuffledRDD2.prev());
        for (int i = 0; i < shuffledRDD1.getDependencies().size(); i++) {
            assertRddEquals(
                    ((Dependency) shuffledRDD1.getDependencies().apply(i)).rdd(),
                    ((Dependency) shuffledRDD2.getDependencies().apply(i)).rdd());
        }
    }

    private void assertPrestoSparkTaskRddEquals(PrestoSparkTaskRdd prestoSparkTaskRdd1, PrestoSparkTaskRdd prestoSparkTaskRdd2)
    {
        assertEquals(
                prestoSparkTaskRdd1.getShuffleInputRdds().size(),
                prestoSparkTaskRdd2.getShuffleInputRdds().size(),
                "Expected same number of shuffle inputs");

        assertEquals(
                prestoSparkTaskRdd1.getShuffleInputFragmentIds().stream().collect(Collectors.toSet()),
                prestoSparkTaskRdd2.getShuffleInputFragmentIds().stream().collect(Collectors.toSet()),
                "Expected same input fragment ids");

        assertEquals(
                prestoSparkTaskRdd1.getTaskSourceRdd() == null,
                prestoSparkTaskRdd2.getTaskSourceRdd() == null,
                "Expected both RDDs to either contain TaskSourceRdd or not contain it");

        for (int i = 0; i < prestoSparkTaskRdd1.getShuffleInputRdds().size(); i++) {
            assertRddEquals((RDD) prestoSparkTaskRdd1.getShuffleInputRdds().get(i), (RDD) prestoSparkTaskRdd2.getShuffleInputRdds().get(i));
        }

        for (int i = 0; i < prestoSparkTaskRdd1.getDependencies().size(); i++) {
            assertRddEquals(
                    ((Dependency) prestoSparkTaskRdd1.getDependencies().apply(i)).rdd(),
                    ((Dependency) prestoSparkTaskRdd2.getDependencies().apply(i)).rdd());
        }
    }
}
