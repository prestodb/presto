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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.PARTIAL_MERGE_PUSHDOWN_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_STAGE_COUNT;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_TOTAL_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.OUT_OF_MEMORY_RETRY_SPARK_CONFIGS;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_HASH_PARTITION_COUNT_SCALING_FACTOR_ON_OUT_OF_MEMORY;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_QUERY_EXECUTION_STRATEGIES;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_RETRY_ON_OUT_OF_MEMORY_BROADCAST_JOIN_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ERROR_CODES;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_SPLIT_ASSIGNMENT_BATCH_SIZE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.STORAGE_BASED_BROADCAST_JOIN_ENABLED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialMergePushdownStrategy.PUSH_THROUGH_LOW_MEMORY_OPERATORS;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.FileAssert.assertFile;

public class TestPrestoSparkQueryRunner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner();
    }

    @Test
    public void testTableWrite()
    {
        // some basic tests
        assertUpdate(
                "CREATE TABLE hive.hive_test.hive_orders AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders",
                15000);

        assertUpdate(
                "INSERT INTO hive.hive_test.hive_orders " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders",
                30000);

        assertQuery(
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM hive.hive_test.hive_orders",
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders");

        // 3-way union all with potentially non-flattened plan
        // See https://github.com/prestodb/presto/issues/12625
        //
        // CreateTable is not supported yet, use CreateTableAsSelect
        assertUpdate(
                "CREATE TABLE hive.hive_test.test_table_write_with_union AS " +
                        "SELECT orderkey, 'dummy' AS dummy " +
                        "FROM orders",
                15000);
        assertUpdate(
                "INSERT INTO hive.hive_test.test_table_write_with_union " +
                        "SELECT orderkey, dummy " +
                        "FROM (" +
                        "   SELECT orderkey, 'a' AS dummy FROM orders " +
                        "UNION ALL" +
                        "   SELECT orderkey, 'bb' AS dummy FROM orders " +
                        "UNION ALL" +
                        "   SELECT orderkey, 'ccc' AS dummy FROM orders " +
                        ")",
                45000);
    }

    @Test
    public void testZeroFileCreatorForBucketedTable()
    {
        assertUpdate(
                getSession(),
                "CREATE TABLE hive.hive_test.test_hive_orders_bucketed_join_zero_file WITH (bucketed_by=array['orderkey'], bucket_count=8) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders_bucketed " +
                        "WHERE orderkey = 1",
                1);
    }

    @Test
    public void testBucketedTableWriteSimple()
    {
        // simple write from a bucketed table to a bucketed table
        // same bucket count
        testBucketedTableWriteSimple(getSession(), 8, 8);
        for (Session testSession : getTestCompatibleBucketCountSessions()) {
            // incompatible bucket count
            testBucketedTableWriteSimple(testSession, 3, 13);
            testBucketedTableWriteSimple(testSession, 13, 7);
            // compatible bucket count
            testBucketedTableWriteSimple(testSession, 4, 8);
            testBucketedTableWriteSimple(testSession, 8, 4);
        }
    }

    private void testBucketedTableWriteSimple(Session session, int inputBucketCount, int outputBucketCount)
    {
        assertUpdate(
                session,
                format("CREATE TABLE hive.hive_test.test_hive_orders_bucketed_simple_input WITH (bucketed_by=array['orderkey'], bucket_count=%s) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders_bucketed", inputBucketCount),
                15000);
        assertQuery(
                session,
                "SELECT count(*) " +
                        "FROM hive.hive_test.test_hive_orders_bucketed_simple_input " +
                        "WHERE \"$bucket\" = 0",
                format("SELECT count(*) FROM orders WHERE orderkey %% %s = 0", inputBucketCount));

        assertUpdate(
                session,
                format("CREATE TABLE hive.hive_test.test_hive_orders_bucketed_simple_output WITH (bucketed_by=array['orderkey'], bucket_count=%s) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM hive.hive_test.test_hive_orders_bucketed_simple_input", outputBucketCount),
                15000);
        assertQuery(
                session,
                "SELECT count(*) " +
                        "FROM hive.hive_test.test_hive_orders_bucketed_simple_output " +
                        "WHERE \"$bucket\" = 0",
                format("SELECT count(*) FROM orders WHERE orderkey %% %s = 0", outputBucketCount));

        dropTable("hive_test", "test_hive_orders_bucketed_simple_input");
        dropTable("hive_test", "test_hive_orders_bucketed_simple_output");
    }

    @Test
    public void testBucketedTableWriteAggregation()
    {
        // aggregate on a bucket key and write to a bucketed table
        // same bucket count
        testBucketedTableWriteAggregation(getSession(), 8, 8);
        for (Session testSession : getTestCompatibleBucketCountSessions()) {
            // incompatible bucket count
            testBucketedTableWriteAggregation(testSession, 7, 13);
            testBucketedTableWriteAggregation(testSession, 13, 7);
            // compatible bucket count
            testBucketedTableWriteAggregation(testSession, 4, 8);
            testBucketedTableWriteAggregation(testSession, 8, 4);
        }
    }

    private void testBucketedTableWriteAggregation(Session session, int inputBucketCount, int outputBucketCount)
    {
        assertUpdate(
                session,
                format("CREATE TABLE hive.hive_test.test_hive_orders_bucketed_aggregation_input WITH (bucketed_by=array['orderkey'], bucket_count=%s) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders_bucketed", inputBucketCount),
                15000);

        assertUpdate(
                session,
                format("CREATE TABLE hive.hive_test.test_hive_orders_bucketed_aggregation_output WITH (bucketed_by=array['orderkey'], bucket_count=%s) AS " +
                        "SELECT orderkey, sum(totalprice) totalprice " +
                        "FROM hive.hive_test.test_hive_orders_bucketed_aggregation_input " +
                        "GROUP BY orderkey", outputBucketCount),
                15000);
        assertQuery(
                session,
                "SELECT count(*) " +
                        "FROM hive.hive_test.test_hive_orders_bucketed_aggregation_output " +
                        "WHERE \"$bucket\" = 0",
                format("SELECT count(*) FROM orders WHERE orderkey %% %s = 0", outputBucketCount));

        dropTable("hive_test", "test_hive_orders_bucketed_aggregation_input");
        dropTable("hive_test", "test_hive_orders_bucketed_aggregation_output");
    }

    @Test
    public void testBucketedTableWriteJoin()
    {
        // join on a bucket key and write to a bucketed table
        // same bucket count
        testBucketedTableWriteJoin(getSession(), 8, 8, 8);
        for (Session testSession : getTestCompatibleBucketCountSessions()) {
            // incompatible bucket count
            testBucketedTableWriteJoin(testSession, 7, 13, 17);
            testBucketedTableWriteJoin(testSession, 13, 7, 17);
            testBucketedTableWriteJoin(testSession, 7, 7, 17);
            // compatible bucket count
            testBucketedTableWriteJoin(testSession, 4, 4, 8);
            testBucketedTableWriteJoin(testSession, 8, 8, 4);
            testBucketedTableWriteJoin(testSession, 4, 8, 8);
            testBucketedTableWriteJoin(testSession, 8, 4, 8);
            testBucketedTableWriteJoin(testSession, 4, 8, 4);
            testBucketedTableWriteJoin(testSession, 8, 4, 4);
        }
    }

    private void testBucketedTableWriteJoin(Session session, int firstInputBucketCount, int secondInputBucketCount, int outputBucketCount)
    {
        assertUpdate(
                session,
                format("CREATE TABLE hive.hive_test.test_hive_orders_bucketed_join_input_1 WITH (bucketed_by=array['orderkey'], bucket_count=%s) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders_bucketed", firstInputBucketCount),
                15000);

        assertUpdate(
                session,
                format("CREATE TABLE hive.hive_test.test_hive_orders_bucketed_join_input_2 WITH (bucketed_by=array['orderkey'], bucket_count=%s) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders_bucketed", secondInputBucketCount),
                15000);

        assertUpdate(
                session,
                format("CREATE TABLE hive.hive_test.test_hive_orders_bucketed_join_output WITH (bucketed_by=array['orderkey'], bucket_count=%s) AS " +
                                "SELECT  first.orderkey, second.totalprice " +
                                "FROM hive.hive_test.test_hive_orders_bucketed_join_input_1 first " +
                                "INNER JOIN hive.hive_test.test_hive_orders_bucketed_join_input_2 second " +
                                "ON first.orderkey = second.orderkey ",
                        outputBucketCount),
                15000);
        assertQuery(
                session,
                "SELECT count(*) " +
                        "FROM hive.hive_test.test_hive_orders_bucketed_join_output " +
                        "WHERE \"$bucket\" = 0",
                format("SELECT count(*) FROM orders WHERE orderkey %% %s = 0", outputBucketCount));

        dropTable("hive_test", "test_hive_orders_bucketed_join_input_1");
        dropTable("hive_test", "test_hive_orders_bucketed_join_input_2");
        dropTable("hive_test", "test_hive_orders_bucketed_join_output");
    }

    private void dropTable(String schema, String table)
    {
        ((PrestoSparkQueryRunner) getQueryRunner()).getMetastore().dropTable(METASTORE_CONTEXT, schema, table, true);
    }

    @Test
    public void testAggregation()
    {
        assertQuery("select partkey, count(*) c from lineitem where partkey % 10 = 1 group by partkey having count(*) = 42");
    }

    @Test
    public void testBucketedAggregation()
    {
        assertBucketedQuery("SELECT orderkey, count(*) c FROM lineitem_bucketed WHERE partkey % 10 = 1 GROUP BY orderkey");
    }

    @Test
    public void testJoin()
    {
        assertQuery("SELECT l.orderkey, l.linenumber, p.brand " +
                "FROM lineitem l, part p " +
                "WHERE l.partkey = p.partkey");
    }

    @Test
    public void testBucketedJoin()
    {
        // both tables are bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem_bucketed l " +
                "JOIN orders_bucketed o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");

        // only probe side table is bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem_bucketed l " +
                "JOIN orders o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");

        // only build side table is bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem l " +
                "JOIN orders_bucketed o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");

        // different number of buckets
        assertUpdate("create table if not exists hive.hive_test.bucketed_nation_for_join_4 " +
                        "WITH (bucket_count = 4, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.bucketed_nation_for_join_8 " +
                        "WITH (bucket_count = 8, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);

        for (Session session : getTestCompatibleBucketCountSessions()) {
            String expected = "SELECT * FROM nation first " +
                    "INNER JOIN nation second " +
                    "ON first.nationkey = second.nationkey";
            assertQuery(
                    session,
                    "SELECT * FROM hive.hive_test.bucketed_nation_for_join_4 first " +
                            "INNER JOIN hive.hive_test.bucketed_nation_for_join_8 second " +
                            "ON first.nationkey = second.nationkey",
                    expected);
            assertQuery(
                    session,
                    "SELECT * FROM hive.hive_test.bucketed_nation_for_join_8 first " +
                            "INNER JOIN hive.hive_test.bucketed_nation_for_join_4 second " +
                            "ON first.nationkey = second.nationkey",
                    expected);

            expected = "SELECT * FROM nation first " +
                    "INNER JOIN nation second " +
                    "ON first.nationkey = second.nationkey " +
                    "INNER JOIN nation third " +
                    "ON second.nationkey = third.nationkey";

            assertQuery(
                    session,
                    "SELECT * FROM hive.hive_test.bucketed_nation_for_join_4 first " +
                            "INNER JOIN hive.hive_test.bucketed_nation_for_join_8 second " +
                            "ON first.nationkey = second.nationkey " +
                            "INNER JOIN nation third " +
                            "ON second.nationkey = third.nationkey",
                    expected);
            assertQuery(
                    session,
                    "SELECT * FROM hive.hive_test.bucketed_nation_for_join_8 first " +
                            "INNER JOIN hive.hive_test.bucketed_nation_for_join_4 second " +
                            "ON first.nationkey = second.nationkey " +
                            "INNER JOIN nation third " +
                            "ON second.nationkey = third.nationkey",
                    expected);
        }
    }

    private List<Session> getTestCompatibleBucketCountSessions()
    {
        return ImmutableList.of(
                Session.builder(getSession())
                        .setSystemProperty(PARTIAL_MERGE_PUSHDOWN_STRATEGY, PUSH_THROUGH_LOW_MEMORY_OPERATORS.name())
                        .build(),
                Session.builder(getSession())
                        .setCatalogSessionProperty("hive", "optimize_mismatched_bucket_count", "true")
                        .build());
    }

    @Test
    public void testJoinUnderUnionALL()
    {
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_10 " +
                        "WITH (bucket_count = 10, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_20 " +
                        "WITH (bucket_count = 20, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_30 " +
                        "WITH (bucket_count = 30, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);

        assertQuery("SELECT hive.hive_test.partitioned_nation_10.nationkey " +
                        "FROM hive.hive_test.partitioned_nation_10 " +
                        "JOIN hive.hive_test.partitioned_nation_20 " +
                        "  ON hive.hive_test.partitioned_nation_10.nationkey = hive.hive_test.partitioned_nation_20.nationkey " +
                        "UNION ALL " +
                        "SELECT hive.hive_test.partitioned_nation_10.nationkey " +
                        "FROM hive.hive_test.partitioned_nation_10 " +
                        "JOIN hive.hive_test.partitioned_nation_30 " +
                        "  ON hive.hive_test.partitioned_nation_10.nationkey = hive.hive_test.partitioned_nation_30.nationkey ",
                "SELECT m.nationkey " +
                        "FROM nation m " +
                        "JOIN nation n " +
                        "  ON m.nationkey = n.nationkey " +
                        "UNION ALL " +
                        "SELECT m.nationkey " +
                        "FROM nation m " +
                        "JOIN nation n " +
                        "  ON m.nationkey = n.nationkey");

        assertQuery("SELECT nationkey " +
                        "FROM nation " +
                        "UNION ALL " +
                        "SELECT hive.hive_test.partitioned_nation_10.nationkey " +
                        "FROM hive.hive_test.partitioned_nation_10 " +
                        "JOIN hive.hive_test.partitioned_nation_30 " +
                        "  ON hive.hive_test.partitioned_nation_10.nationkey = hive.hive_test.partitioned_nation_30.nationkey ",
                "SELECT nationkey " +
                        "FROM nation " +
                        "UNION ALL " +
                        "SELECT m.nationkey " +
                        "FROM nation m " +
                        "JOIN nation n " +
                        "  ON m.nationkey = n.nationkey");
    }

    @Test
    public void testAggregationUnderUnionAll()
    {
        assertQuery("SELECT orderkey, 1 FROM orders UNION ALL SELECT orderkey, count(*) FROM orders GROUP BY 1",
                "SELECT orderkey, 1 FROM orders UNION ALL SELECT orderkey, count(*) FROM orders GROUP BY orderkey");

        assertQuery("SELECT " +
                        "   o.regionkey, " +
                        "   l.orderkey " +
                        "FROM (" +
                        "   SELECT " +
                        "       * " +
                        "   FROM lineitem " +
                        "   WHERE" +
                        "       linenumber = 4" +
                        ") l " +
                        "CROSS JOIN (" +
                        "   SELECT" +
                        "       regionkey," +
                        "       1 " +
                        "   FROM nation " +
                        "   UNION ALL " +
                        "   SELECT" +
                        "       regionkey," +
                        "       count(*) " +
                        "   FROM nation " +
                        "       GROUP BY regionkey" +
                        ") o",
                "SELECT " +
                        "   o.regionkey, " +
                        "   l.orderkey " +
                        "FROM (" +
                        "   SELECT " +
                        "       * " +
                        "   FROM lineitem " +
                        "   WHERE" +
                        "       linenumber = 4" +
                        ") l " +
                        "CROSS JOIN (" +
                        "   SELECT" +
                        "       regionkey," +
                        "       1 " +
                        "   FROM nation " +
                        "   UNION ALL " +
                        "   SELECT" +
                        "       regionkey," +
                        "       count(*) " +
                        "   FROM nation " +
                        "       GROUP BY regionkey" +
                        ") o");
    }

    @Test
    public void testCrossJoin()
    {
        assertQuery("" +
                "SELECT o.custkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem WHERE linenumber = 4) l " +
                "CROSS JOIN (SELECT * FROM orders WHERE orderkey = 5) o");
        assertQuery("" +
                "SELECT o.custkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                "CROSS JOIN (" +
                "   SELECT * FROM orders WHERE orderkey = 5 " +
                "   UNION ALL " +
                "   SELECT * FROM orders WHERE orderkey = 5 " +
                ") o");

        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_11 " +
                        "WITH (bucket_count = 11, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_22 " +
                        "WITH (bucket_count = 22, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);
        assertUpdate("create table if not exists hive.hive_test.partitioned_nation_33 " +
                        "WITH (bucket_count = 33, bucketed_by = ARRAY['nationkey']) as select * from nation",
                25);

        // UNION ALL over aggregation
        assertQuery("SELECT o.orderkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT orderkey, 1 FROM orders WHERE orderkey = 5 " +
                        "   UNION ALL " +
                        "   SELECT orderkey, count(*) " +
                        "       FROM orders WHERE orderkey = 5 " +
                        "   GROUP BY 1 " +
                        "   ) o",
                "SELECT o.orderkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT orderkey, 1 FROM orders WHERE orderkey = 5 " +
                        "   UNION ALL " +
                        "   SELECT orderkey, count(*) " +
                        "       FROM orders WHERE orderkey = 5 " +
                        "   GROUP BY orderkey " +
                        "   ) o");

        // 22 buckets UNION ALL 11 buckets
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_22 " +
                        "   UNION ALL " +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");

        // 11 buckets UNION ALL 22 buckets
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        "   UNION ALL " +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_22 " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");

        // bucketed UNION ALL non-bucketed
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");

        // non-bucketed UNION ALL bucketed
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");

        // 11 buckets UNION ALL non-bucketed UNION ALL 22 buckets
        assertQuery("SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_11 " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM hive.hive_test.partitioned_nation_22 " +
                        ") o",
                "SELECT o.regionkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                        "CROSS JOIN (" +
                        "   SELECT * FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        "   UNION ALL " +
                        "   SELECT * FROM nation" +
                        ") o");
    }

    @Test
    public void testNWayJoin()
    {
        assertQuery("SELECT l.orderkey, l.linenumber, p1.brand, p2.brand, p3.brand, p4.brand, p5.brand, p6.brand " +
                "FROM lineitem l, part p1, part p2, part p3, part p4, part p5, part p6 " +
                "WHERE l.partkey = p1.partkey " +
                "AND l.partkey = p2.partkey " +
                "AND l.partkey = p3.partkey " +
                "AND l.partkey = p4.partkey " +
                "AND l.partkey = p5.partkey " +
                "AND l.partkey = p6.partkey");
    }

    @Test
    public void testBucketedNWayJoin()
    {
        // all tables are bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o1.orderstatus, o2.orderstatus, o3.orderstatus, o4.orderstatus, o5.orderstatus, o6.orderstatus " +
                "FROM lineitem_bucketed l, orders_bucketed o1, orders_bucketed o2, orders_bucketed o3, orders_bucketed o4, orders_bucketed o5, orders_bucketed o6 " +
                "WHERE l.orderkey = o1.orderkey " +
                "AND l.orderkey = o2.orderkey " +
                "AND l.orderkey = o3.orderkey " +
                "AND l.orderkey = o4.orderkey " +
                "AND l.orderkey = o5.orderkey " +
                "AND l.orderkey = o6.orderkey");

        // some tables are bucketed
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o1.orderstatus, o2.orderstatus, o3.orderstatus, o4.orderstatus, o5.orderstatus, o6.orderstatus " +
                "FROM lineitem_bucketed l, orders o1, orders_bucketed o2, orders o3, orders_bucketed o4, orders o5, orders_bucketed o6 " +
                "WHERE l.orderkey = o1.orderkey " +
                "AND l.orderkey = o2.orderkey " +
                "AND l.orderkey = o3.orderkey " +
                "AND l.orderkey = o4.orderkey " +
                "AND l.orderkey = o5.orderkey " +
                "AND l.orderkey = o6.orderkey");
        assertBucketedQuery("SELECT l.orderkey, l.linenumber, o1.orderstatus, o2.orderstatus, o3.orderstatus, o4.orderstatus, o5.orderstatus, o6.orderstatus " +
                "FROM lineitem l, orders o1, orders_bucketed o2, orders o3, orders_bucketed o4, orders o5, orders_bucketed o6 " +
                "WHERE l.orderkey = o1.orderkey " +
                "AND l.orderkey = o2.orderkey " +
                "AND l.orderkey = o3.orderkey " +
                "AND l.orderkey = o4.orderkey " +
                "AND l.orderkey = o5.orderkey " +
                "AND l.orderkey = o6.orderkey");
    }

    @Test
    public void testUnionAll()
    {
        assertQuery("SELECT * FROM orders UNION ALL SELECT * FROM orders");

        assertBucketedQuery("SELECT * FROM lineitem_bucketed UNION ALL SELECT * FROM lineitem_bucketed");

        assertBucketedQuery("SELECT * FROM lineitem UNION ALL SELECT * FROM lineitem_bucketed");

        assertBucketedQuery("SELECT * FROM lineitem_bucketed UNION ALL SELECT * FROM lineitem");
    }

    @Test
    public void testBucketedUnionAll()
    {
        // all tables bucketed
        assertBucketedQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM (" +
                "   SELECT * FROM lineitem_bucketed " +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                ") GROUP BY orderkey");

        // some tables bucketed
        assertBucketedQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM (" +
                "   SELECT * FROM lineitem_bucketed " +
                "   UNION ALL " +
                "   SELECT * FROM lineitem" +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                ") GROUP BY orderkey");
        assertBucketedQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM (" +
                "   SELECT * FROM lineitem " +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                "   UNION ALL " +
                "   SELECT * FROM lineitem" +
                ") GROUP BY orderkey");
        assertBucketedQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM (" +
                "   SELECT * FROM lineitem " +
                "   UNION ALL " +
                "   SELECT * FROM lineitem_bucketed" +
                ") GROUP BY orderkey");
    }

    @Test
    public void testValues()
    {
        assertQuery("SELECT a, b " +
                "FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) t1 (a, b) ");
    }

    @Test
    public void testUnionWithAggregationAndJoin()
    {
        assertQuery(
                "SELECT * FROM ( " +
                        "SELECT orderkey, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY orderkey) t " +
                        "JOIN orders o " +
                        "ON (o.orderkey = t.orderkey)");
    }

    @Test
    public void testFailures()
    {
        assertQueryFails("SELECT * FROM orders WHERE custkey / (orderkey - orderkey) = 0", "/ by zero");
        assertQueryFails(
                "CREATE TABLE hive.hive_test.hive_orders_test_failures AS " +
                        "(SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders) " +
                        "UNION ALL " +
                        "(SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "WHERE custkey / (orderkey - orderkey) = 0 )",
                "/ by zero");
    }

    @Test
    public void testSelectFromEmptyTable()
    {
        assertUpdate(
                "CREATE TABLE hive.hive_test.empty_orders AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "WITH NO DATA",
                0);

        assertQuery(
                "SELECT count(*) FROM hive.hive_test.empty_orders",
                "SELECT 0");
    }

    @Test
    public void testSelectFromEmptyBucketedTable()
    {
        assertUpdate(
                "CREATE TABLE hive.hive_test.empty_orders_bucketed WITH (bucketed_by=array['orderkey'], bucket_count=11) AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders " +
                        "WITH NO DATA",
                0);

        assertQuery(
                "SELECT count(*) FROM (SELECT orderkey, count(*) FROM hive.hive_test.empty_orders_bucketed GROUP BY orderkey)",
                "SELECT 0");
    }

    @Test
    public void testLimit()
    {
        MaterializedResult actual = computeActual("SELECT * FROM orders LIMIT 10");
        assertEquals(actual.getRowCount(), 10);
        actual = computeActual("SELECT 'a' FROM orders LIMIT 10");
        assertEquals(actual.getRowCount(), 10);
    }

    @Test
    public void testTableSampleSystem()
    {
        long totalRows = (Long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        long sampledRows = (Long) computeActual("SELECT count(*) FROM orders TABLESAMPLE SYSTEM (1)").getOnlyValue();
        assertThat(sampledRows).isLessThan(totalRows);
    }

    @Test(enabled = false)
    public void testTimeouts()
    {
        // Expected run time for this query is ~30s
        String longRunningCrossJoin = "SELECT count(l1.orderkey), count(l2.orderkey) FROM lineitem l1, lineitem l2";

        Session queryMaxRunTimeLimitSession = Session.builder(getSession())
                .setSystemProperty("query_max_run_time", "2s")
                .build();
        assertQueryFails(queryMaxRunTimeLimitSession, longRunningCrossJoin, "Query exceeded maximum time limit of 2.00s");

        Session queryMaxExecutionTimeLimitSession = Session.builder(getSession())
                .setSystemProperty("query_max_run_time", "3s")
                .setSystemProperty("query_max_execution_time", "2s")
                .build();
        assertQueryFails(queryMaxExecutionTimeLimitSession, longRunningCrossJoin, "Query exceeded maximum time limit of 2.00s");

        // Test whether waitTime is being considered while computing timeouts.
        // Expected run time for this query is ~30s. We will set `dummyServiceWaitTime` as 600s.
        // The timeout logic will set the timeout for the query as 605s (Actual timeout + waitTime)
        // and the query should succeed. This is a bit hacky way to check whether service waitTime
        // is added to the deadline time while submitting jobs
        Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics = ((PrestoSparkQueryRunner) getQueryRunner()).getWaitTimeMetrics();
        PrestoSparkTestingServiceWaitTimeMetrics testingServiceWaitTimeMetrics = (PrestoSparkTestingServiceWaitTimeMetrics) waitTimeMetrics.stream()
                .filter(metric -> metric instanceof PrestoSparkTestingServiceWaitTimeMetrics)
                .findFirst().get();
        testingServiceWaitTimeMetrics.setWaitTime(new Duration(600, SECONDS));
        queryMaxRunTimeLimitSession = Session.builder(getSession())
                .setSystemProperty("query_max_execution_time", "5s")
                .build();
        assertQuerySucceeds(queryMaxRunTimeLimitSession, longRunningCrossJoin);
        testingServiceWaitTimeMetrics.setWaitTime(new Duration(0, SECONDS));
    }

    @Test
    public void testDiskBasedBroadcastJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .build();

        assertQuery(session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey");

        assertQuery(session,
                "select l.orderkey from lineitem l join orders o on l.orderkey = o.orderkey " +
                        "Union all " +
                        "SELECT m.nationkey FROM nation m JOIN nation n  ON m.nationkey = n.nationkey");

        assertQuery(session,
                "SELECT o.custkey, l.orderkey " +
                        "FROM (SELECT * FROM lineitem WHERE linenumber = 4) l " +
                        "CROSS JOIN (SELECT * FROM orders WHERE orderkey = 5) o");

        assertQuery(session,
                "WITH broadcast_table1 AS ( " +
                        "    SELECT " +
                        "        * " +
                        "    FROM lineitem " +
                        "    WHERE " +
                        "        linenumber = 1 " +
                        ")," +
                        "broadcast_table2 AS ( " +
                        "    SELECT " +
                        "        * " +
                        "    FROM lineitem " +
                        "    WHERE " +
                        "        linenumber = 2 " +
                        ")," +
                        "broadcast_table3 AS ( " +
                        "    SELECT " +
                        "        * " +
                        "    FROM lineitem " +
                        "    WHERE " +
                        "        linenumber = 3 " +
                        ")," +
                        "broadcast_table4 AS ( " +
                        "    SELECT " +
                        "        * " +
                        "    FROM lineitem " +
                        "    WHERE " +
                        "        linenumber = 4 " +
                        ")" +
                        "SELECT " +
                        "    * " +
                        "FROM broadcast_table1 a " +
                        "JOIN broadcast_table2 b " +
                        "    ON a.orderkey = b.orderkey " +
                        "JOIN broadcast_table3 c " +
                        "    ON a.orderkey = c.orderkey " +
                        "JOIN broadcast_table4 d " +
                        "    ON a.orderkey = d.orderkey");
    }

    @Test
    public void testStorageBasedBroadcastJoinMaxThreshold()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "1MB")
                .build();

        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                "Query exceeded per-node broadcast memory limit of 1MB \\[Broadcast size: .*MB\\]");
    }

    @Test
    public void testStorageBasedBroadcastJoinDeserializedMaxThreshold()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .setSystemProperty(SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE, "1MB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "100MB")
                .build();

        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                "Query exceeded per-node broadcast memory limit of 1MB \\[Broadcast size: .*MB\\]");
    }

    @Test
    public void testCorrectErrorMessageWhenSubPlanCreationFails()
    {
        String query = "with l as (" +
                "select * from lineitem UNION ALL select * from lineitem UNION ALL select * from lineitem" +
                "), " +
                "o as (" +
                "select * from orders UNION ALL select * from orders UNION ALL select * from orders" +
                ") " +
                "select * from l right outer join o on l.orderkey = o.orderkey";

        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "1")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "10MB")
                .setSystemProperty(QUERY_MAX_MEMORY, "100MB")
                .setSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, "true")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED, "false")
                .setSystemProperty(QUERY_MAX_STAGE_COUNT, "2")
                .build();
        assertQueryFails(session,
                query,
                "Number of stages in the query.* exceeds the allowed maximum.*");
    }

    @Test
    public void testRetryWithHigherHashPartitionCount()
    {
        String query = "with l as (" +
                "select * from lineitem UNION ALL select * from lineitem UNION ALL select * from lineitem" +
                "), " +
                "o as (" +
                "select * from orders UNION ALL select * from orders UNION ALL select * from orders" +
                ") " +
                "select * from l right outer join o on l.orderkey = o.orderkey";

        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "1")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "6.5MB")
                .setSystemProperty(QUERY_MAX_MEMORY, "100MB")
                .setSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, "true")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED, "false")
                .build();
        assertQueryFails(session,
                query,
                "Query exceeded per-node total memory limit of .*Top Consumers: \\{HashBuilderOperator.*");

        session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "1")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "6.5MB")
                .setSystemProperty(QUERY_MAX_MEMORY, "100MB")
                .setSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, "true")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED, "true")
                .setSystemProperty(SPARK_HASH_PARTITION_COUNT_SCALING_FACTOR_ON_OUT_OF_MEMORY, "1.0")
                .build();
        assertQueryFails(session,
                query,
                "Query exceeded per-node total memory limit of .*Top Consumers: \\{HashBuilderOperator.*");

        session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "1")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "6.5MB")
                .setSystemProperty(QUERY_MAX_MEMORY, "100MB")
                .setSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, "true")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED, "true")
                .build();
        assertQuerySucceeds(session, query);

        session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "1")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "6.5MB")
                .setSystemProperty(QUERY_MAX_MEMORY, "100MB")
                .setSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, "true")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED, "true")
                .setSystemProperty(SPARK_HASH_PARTITION_COUNT_SCALING_FACTOR_ON_OUT_OF_MEMORY, "2.0")
                .build();
        assertQuerySucceeds(session, query);
    }

    @Test
    public void testRetryOnOutOfMemoryBroadcastJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .setSystemProperty(SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE, "10B")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_BROADCAST_JOIN_ENABLED, "false")
                .build();

        // Query should fail with broadcast join OOM
        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                "Query exceeded per-node broadcast memory limit of 10B \\[Broadcast size: .*MB\\]");

        session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .setSystemProperty(SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE, "10B")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_BROADCAST_JOIN_ENABLED, "true")
                .build();

        // Query should succeed since broadcast join will be disabled on retry
        assertQuery(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey");
    }

    @Test
    public void testRetryOnOutOfMemoryWithIncreasedContainerSize()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();

        // Query should fail with OOM
        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                ".*Query exceeded per-node .* memory limit of 2MB.*");

        session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "true")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES, "query_max_memory_per_node=100MB,query_max_total_memory_per_node=100MB")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_SPARK_CONFIGS, "spark.executor.memory=1G")
                .build();

        // Query should succeed since memory will be increased on retry
        assertQuery(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey");
    }

    @Test
    public void testRetryOnOutOfMemoryWithIncreasedContainerSizeWithSessionPropertiesProvidedErrorCode()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .build();

        // Query should fail with OOM
        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                ".*Query exceeded per-node .* memory limit of 2MB.*");

        session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "true")
                // Do not provide any error code. INCREASE_CONTAINER_SIZE strategy won't be applied
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ERROR_CODES, "")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES, "query_max_memory_per_node=100MB,query_max_total_memory_per_node=100MB")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_SPARK_CONFIGS, "spark.executor.memory=1G")
                .build();

        // Query should fail with OOM
        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                ".*Query exceeded per-node .* memory limit of 2MB.*");

        session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "true")
                // Support EXCEEDED_LOCAL_MEMORY_LIMIT as the retry error code. INCREASE_CONTAINER_SIZE strategy will be applied
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ERROR_CODES, "EXCEEDED_LOCAL_MEMORY_LIMIT")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES, "query_max_memory_per_node=100MB,query_max_total_memory_per_node=100MB")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_SPARK_CONFIGS, "spark.executor.memory=1G")
                .build();

        // Query should succeed since memory will be increased on retry
        assertQuery(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey");
    }

    @Test
    public void testSmileSerialization()
    {
        String query = "SELECT * FROM nation";
        try (QueryRunner queryRunner = createHivePrestoSparkQueryRunner(ImmutableList.of(NATION), ImmutableMap.of("spark.smile-serialization-enabled", "true"))) {
            MaterializedResult actual = queryRunner.execute(query);
            assertEqualsIgnoreOrder(actual, computeExpected(query, actual.getTypes()));
        }
        try (QueryRunner queryRunner = createHivePrestoSparkQueryRunner(ImmutableList.of(NATION), ImmutableMap.of("spark.smile-serialization-enabled", "false"))) {
            MaterializedResult actual = queryRunner.execute(query);
            assertEqualsIgnoreOrder(actual, computeExpected(query, actual.getTypes()));
        }
    }

    @Test
    public void testIterativeSplitEnumeration()
    {
        for (int batchSize = 1; batchSize <= 8; batchSize *= 2) {
            Session session = Session.builder(getSession())
                    .setSystemProperty(SPARK_SPLIT_ASSIGNMENT_BATCH_SIZE, batchSize + "")
                    .build();

            assertQuery(session, "select partkey, count(*) c from lineitem where partkey % 10 = 1 group by partkey having count(*) = 42");

            assertQuery(session, "SELECT l.orderkey, l.linenumber, p.brand " +
                    "FROM lineitem l, part p " +
                    "WHERE l.partkey = p.partkey");
        }
    }

    @Test
    public void testDropTable()
    {
        assertUpdate(
                "CREATE TABLE hive.hive_test.hive_orders1 AS " +
                        "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders", 15000);
        assertQuery("select count(*) from hive.hive_test.hive_orders1", "select 15000");
        assertQuerySucceeds("DROP TABLE hive.hive_test.hive_orders1");
        assertQueryFails("select count(*) from hive.hive_test.hive_orders1", ".*Table hive.hive_test.hive_orders1 does not exist");
    }

    @Test
    public void testCreateDropSchema()
    {
        assertQuerySucceeds("CREATE SCHEMA hive.hive_test_new");
        assertQuerySucceeds("CREATE TABLE  hive.hive_test_new.test (x bigint)");
        assertQueryFails("DROP SCHEMA hive.hive_test_new", "Schema not empty: hive_test_new");
        assertQuerySucceeds("DROP TABLE hive.hive_test_new.test");
        assertQuerySucceeds("ALTER SCHEMA hive.hive_test_new RENAME TO hive_test_new1");
        assertQueryFails("DROP SCHEMA hive.hive_test_new", ".* Schema 'hive.hive_test_new' does not exist");
        assertQuerySucceeds("DROP SCHEMA hive.hive_test_new1");
    }

    @Test
    public void testCreateAlterTable()
    {
        // create table with default format orc
        String createTableSql = "CREATE TABLE hive.hive_test.hive_orders_new (\n" +
                "   \"x\" bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        assertQuerySucceeds(createTableSql);
        MaterializedResult actual = computeActual("SHOW CREATE TABLE hive.hive_test.hive_orders_new");
        assertEquals(createTableSql, actual.getOnlyValue());
        assertQuerySucceeds("ALTER TABLE hive.hive_test.hive_orders_new RENAME TO hive.hive_test.hive_orders_new1");
        assertQueryFails("DROP TABLE hive.hive_test.hive_orders_new", ".* Table 'hive.hive_test.hive_orders_new' does not exist");
        assertQuerySucceeds("DROP TABLE hive.hive_test.hive_orders_new1");
    }

    @Test
    public void testCreateDropView()
    {
        // create table with default format orc
        String createViewSql = "CREATE VIEW hive.hive_test.hive_view SECURITY DEFINER AS\n" +
                "SELECT *\n" +
                "FROM\n" +
                "  orders";
        assertQuerySucceeds(createViewSql);
        MaterializedResult actual = computeActual("SHOW CREATE VIEW hive.hive_test.hive_view");
        assertEquals(createViewSql, actual.getOnlyValue());
        assertQuerySucceeds("DROP VIEW hive.hive_test.hive_view");
    }

    @Test
    public void testCreateExternalTable()
            throws Exception
    {
        File tempDir = createTempDir();
        File dataFile = new File(tempDir, "test.txt");
        Files.write("hello\nworld\n", dataFile, UTF_8);

        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_create_external (\n" +
                        "   \"name\" varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   external_location = '%s',\n" +
                        "   format = 'TEXTFILE'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                new Path(tempDir.toURI().toASCIIString()).toString());

        assertQuerySucceeds(createTableSql);
        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_create_external");
        assertEquals(actual.getOnlyValue(), createTableSql);

        actual = computeActual("SELECT name FROM test_create_external");
        assertEquals(actual.getOnlyColumnAsSet(), ImmutableSet.of("hello", "world"));

        assertQuerySucceeds("DROP TABLE test_create_external");

        // file should still exist after drop
        assertFile(dataFile);

        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    //
    @Test
    @Ignore("PrsetoSparkQueryRunner was changed to use hive user with admin priviliges. It breaks this tess assumptions." +
            "Update this test to not make assumptions")
    public void testGrants()
    {
        assertQuerySucceeds("CREATE SCHEMA hive.hive_test_new");
        assertQuerySucceeds("CREATE TABLE  hive.hive_test_new.test (x bigint)");

        // Grant user
        assertQuerySucceeds("GRANT SELECT,INSERT,DELETE,UPDATE ON hive.hive_test_new.test to user");
        MaterializedResult actual = computeActual("SHOW GRANTS ON TABLE hive.hive_test_new.test");
        // permissions are in the eighth field
        List<String> grants = actual.getMaterializedRows().stream().map(row -> row.getField(7).toString()).collect(Collectors.toList());
        assertEquals(Ordering.natural().sortedCopy(grants), ImmutableList.of("DELETE", "INSERT", "SELECT", "UPDATE"));

        // Revoke select,insert grants
        assertQuerySucceeds("REVOKE SELECT,INSERT ON hive.hive_test_new.test FROM user");
        actual = computeActual("SHOW GRANTS ON TABLE hive.hive_test_new.test");
        grants = actual.getMaterializedRows().stream().map(row -> row.getField(7).toString()).collect(Collectors.toList());
        assertEquals(Ordering.natural().sortedCopy(grants), ImmutableList.of("DELETE", "UPDATE"));

        assertQuerySucceeds("DROP TABLE hive.hive_test_new.test");
        assertQuerySucceeds("DROP SCHEMA hive.hive_test_new");
    }

    @Test
    @Ignore("PrsetoSparkQueryRunner was changed to use hive user with admin priviliges. It breaks this tess assumptions." +
            "Update this test to not make assumptions")
    public void testRoles()
    {
        assertQuerySucceeds("CREATE ROLE admin");
        assertQuerySucceeds("CREATE ROLE test_role");
        assertQuerySucceeds("GRANT test_role TO USER user");

        // Show Roles
        MaterializedResult actual = computeActual("SHOW ROLES");
        List<String> roles = actual.getMaterializedRows().stream().map(row -> row.getField(0).toString()).collect(Collectors.toList());
        assertEquals(Ordering.natural().sortedCopy(roles), ImmutableList.of("admin", "test_role"));

        // Show roles assigned to user
        actual = computeActual("SHOW ROLE GRANTS");
        roles = actual.getMaterializedRows().stream().map(row -> row.getField(0).toString()).collect(Collectors.toList());
        assertEquals(Ordering.natural().sortedCopy(roles), ImmutableList.of("public", "test_role"));

        // Revokes roles
        assertQuerySucceeds("REVOKE test_role FROM USER user");
        actual = computeActual("SHOW ROLE GRANTS");
        roles = actual.getMaterializedRows().stream().map(row -> row.getField(0).toString()).collect(Collectors.toList());
        assertEquals(Ordering.natural().sortedCopy(roles), ImmutableList.of("public"));

        assertQuerySucceeds("DROP ROLE test_role");
    }

    @Test
    public void testAddColumns()
    {
        assertQuerySucceeds("CREATE TABLE test_add_column (a bigint COMMENT 'test comment AAA')");
        assertQuerySucceeds("ALTER TABLE test_add_column ADD COLUMN b bigint COMMENT 'test comment BBB'");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN a varchar", ".* Column 'a' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN c bad_type", ".* Unknown type 'bad_type' for column 'c'");
        assertQuery("SHOW COLUMNS FROM test_add_column", "VALUES ('a', 'bigint', '', 'test comment AAA', 19L, null, null), ('b', 'bigint', '', 'test comment BBB', 19L, null, null)");
        assertQuerySucceeds("DROP TABLE test_add_column");
    }

    @Test
    public void testRenameColumn()
    {
        String createTable = "" +
                "CREATE TABLE test_rename_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuerySucceeds("ALTER TABLE test_rename_column RENAME COLUMN orderkey TO new_orderkey");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN \"$path\" TO test", ".* Cannot rename hidden column");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN orderstatus TO new_orderstatus", "Renaming partition columns is not supported");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertQuerySucceeds("DROP TABLE test_rename_column");
    }

    @Test
    public void testDropColumn()
    {
        assertQueryFails("DROP TABLE hive.hive_test.hive_orders_new", ".* Table 'hive.hive_test.hive_orders_new' does not exist");
        String createTable = "" +
                "CREATE TABLE test_drop_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderstatus FROM test_drop_column", "SELECT orderkey, orderstatus FROM orders");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN \"$path\"", ".* Cannot drop hidden column");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN orderstatus", "Cannot drop partition columns");
        assertQuerySucceeds("ALTER TABLE test_drop_column DROP COLUMN orderkey");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN custkey", "Cannot drop the only non-partition column in a table");
        assertQuery("SELECT * FROM test_drop_column", "SELECT custkey, orderstatus FROM orders");

        assertQuerySucceeds("DROP TABLE test_drop_column");
    }

    @Test
    public void testCreateType()
    {
        assertQuerySucceeds("CREATE TYPE unittest.memory.num AS integer");
        assertQuerySucceeds("CREATE TYPE unittest.memory.pair AS (fst integer, snd integer)");
        assertQuerySucceeds("CREATE TYPE unittest.memory.pair3 AS (fst unittest.memory.pair, snd integer)");
        assertQuery("SELECT p.fst.fst FROM(SELECT CAST(ROW(CAST(ROW(1,2) AS unittest.memory.pair), 3) AS unittest.memory.pair3) AS p)", "SELECT 1");
        assertQuerySucceeds("CREATE TYPE unittest.memory.pair3Alt AS (fst ROW(fst integer, snd integer), snd integer)");
        assertQuery("SELECT p.fst.snd FROM(SELECT CAST(ROW(ROW(1,2), 3) AS  unittest.memory.pair3Alt) AS p)", "SELECT 2");
    }

    @Test
    public void testCreatePartitionedTable()
    {
        String createPartitionedTable = "" +
                "CREATE TABLE test_partition_table\n" +
                "WITH (\n" +
                "  format = 'Parquet', " +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertQuerySucceeds(createPartitionedTable);
        MaterializedResult actual = computeActual("SELECT count(*) FROM \"test_partition_table$partitions\"");
        // there are 3 partitions
        assertEquals(actual.getOnlyValue().toString(), "3");

        // invoke CALL procedure to add empty partitions
        assertQuerySucceeds(format("CALL system.create_empty_partition('%s', '%s', ARRAY['orderstatus'], ARRAY['%s'])", "tpch", "test_partition_table", "x"));
        assertQuerySucceeds(format("CALL system.create_empty_partition('%s', '%s', ARRAY['orderstatus'], ARRAY['%s'])", "tpch", "test_partition_table", "y"));
        actual = computeActual("SELECT count(*) FROM \"test_partition_table$partitions\"");

        // 2 new partitions added
        assertEquals(actual.getOnlyValue().toString(), "5");
        assertQuerySucceeds("DROP TABLE test_partition_table");
    }

    @Test
    public void testDisableBroadcastJoinExecutionStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .setSystemProperty(SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE, "10B")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_BROADCAST_JOIN_ENABLED, "false")
                .build();

        // Query should fail with broadcast join OOM
        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                "Query exceeded per-node broadcast memory limit of 10B \\[Broadcast size: .*MB\\]");

        session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(STORAGE_BASED_BROADCAST_JOIN_ENABLED, "true")
                .setSystemProperty(SPARK_BROADCAST_JOIN_MAX_MEMORY_OVERRIDE, "10B")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_BROADCAST_JOIN_ENABLED, "false")
                .setSystemProperty(SPARK_QUERY_EXECUTION_STRATEGIES, "DISABLE_BROADCAST_JOIN")
                .build();

        // Query should succeed since broadcast join will be disabled due to
        // presence of DISABLE_BROADCAST_JOIN strategy in session property spark_query_execution_strategies
        assertQuery(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey");
    }

    @Test
    public void testIncreaseContainerSizeExecutionStrategy()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES, "query_max_memory_per_node=100MB,query_max_total_memory_per_node=100MB")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_SPARK_CONFIGS, "spark.executor.memory=1G")
                .build();

        // Query should fail with OOM
        assertQueryFails(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey",
                ".*Query exceeded per-node .* memory limit of 2MB.*");

        session = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "2MB")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_WITH_INCREASED_MEMORY_SETTINGS_ENABLED, "false")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_PRESTO_SESSION_PROPERTIES, "query_max_memory_per_node=100MB,query_max_total_memory_per_node=100MB")
                .setSystemProperty(OUT_OF_MEMORY_RETRY_SPARK_CONFIGS, "spark.executor.memory=1G")
                .setSystemProperty(SPARK_QUERY_EXECUTION_STRATEGIES, "INCREASE_CONTAINER_SIZE")
                .build();

        // Query should succeed since memory will be increased due to
        // presence of INCREASE_CONTAINER_SIZE strategy in session property spark_query_execution_strategies
        assertQuery(
                session,
                "select * from lineitem l join orders o on l.orderkey = o.orderkey");
    }

    @Test
    public void testIncreaseHashPartitionCountExecutionStrategy()
    {
        String query = "with l as (" +
                "select * from lineitem UNION ALL select * from lineitem UNION ALL select * from lineitem" +
                "), " +
                "o as (" +
                "select * from orders UNION ALL select * from orders UNION ALL select * from orders" +
                ") " +
                "select * from l right outer join o on l.orderkey = o.orderkey";

        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "1")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "6.5MB")
                .setSystemProperty(QUERY_MAX_MEMORY, "100MB")
                .setSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, "true")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED, "false")
                .build();
        assertQueryFails(session,
                query,
                "Query exceeded per-node total memory limit of .*Top Consumers: \\{HashBuilderOperator.*");

        session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .setSystemProperty(HASH_PARTITION_COUNT, "1")
                .setSystemProperty(QUERY_MAX_TOTAL_MEMORY_PER_NODE, "6.5MB")
                .setSystemProperty(QUERY_MAX_MEMORY, "100MB")
                .setSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, "true")
                .setSystemProperty(SPARK_RETRY_ON_OUT_OF_MEMORY_HIGHER_PARTITION_COUNT_ENABLED, "false")
                .setSystemProperty(SPARK_QUERY_EXECUTION_STRATEGIES, "INCREASE_HASH_PARTITION_COUNT")
                .build();
        assertQuerySucceeds(session, query);
    }

    private void assertBucketedQuery(String sql)
    {
        assertQuery(sql, sql.replaceAll("_bucketed", ""));
    }
}
