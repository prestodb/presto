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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.ORDER_BASED_EXECUTION_ENABLED;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;

public class TestStreamingAggregationPlan
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, LINE_ITEM, CUSTOMER, NATION),
                ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"),
                Optional.empty());
    }

    @Test
    public void testUnsortedTable()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // even with streaming aggregation enabled, non-ordered table that can't be applied streaming aggregation would use hash based aggregation
            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT custkey, COUNT(*) FROM test_customer \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1",
                    aggregationPlanWithNoStreaming("test_customer", false, "custkey"));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer");
        }
    }

    // bucket-keys and sorted keys
    @Test
    public void testBucketedAndSortedBySameKey()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer2 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            queryRunner.execute("INSERT INTO test_customer2 \n" +
                    "SELECT *, '2021-07-12' as ds FROM tpch.sf1.customer LIMIT 1000");

            // default: streaming aggregation is not turned on by default and hash based aggregation would be used
            assertPlan("SELECT custkey, COUNT(*) FROM test_customer2 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1", aggregationPlanWithNoStreaming("test_customer2", false, "custkey"));

            // streaming aggregation enabled
            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT custkey, COUNT(*) FROM test_customer2 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1",
                    node(
                            OutputNode.class,
                            node(
                                    ExchangeNode.class,
                                    aggregation(
                                            singleGroupingSet("custkey"),
                                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                            ImmutableList.of("custkey"), // streaming
                                            ImmutableMap.of(),
                                            Optional.empty(),
                                            SINGLE,
                                            tableScan("test_customer2", ImmutableMap.of("custkey", "custkey"))))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer2");
        }
    }

    @Test
    public void testBucketedAndSortedByDifferentKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer3 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can't enable stream
            // since it's bucketed by custkey, the local exchange would be removed when order based execution is enabled
            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT custkey, COUNT(*) FROM test_customer3 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1",
                    node(
                            OutputNode.class,
                            node(
                                    ExchangeNode.class,
                                    aggregation(
                                            singleGroupingSet("custkey"),
                                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                            ImmutableList.of(), // non-streaming
                                            ImmutableMap.of(),
                                            Optional.empty(),
                                            SINGLE,
                                            tableScan("test_customer3", ImmutableMap.of("custkey", "custkey"))))));

            // can't enable stream
            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT name, COUNT(*) FROM test_customer3 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1",
                    aggregationPlanWithNoStreaming("test_customer3", true, "name"));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer3");
        }
    }

    @Test
    public void testBucketedByPrefixOfSortedKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer4 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey', 'name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // streaming aggregation enabled
            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT custkey, name, COUNT(*) FROM test_customer4 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2",
                    node(
                            OutputNode.class,
                            node(
                                    ExchangeNode.class,
                                    aggregation(
                                            singleGroupingSet("custkey", "name"),
                                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                            ImmutableList.of("custkey", "name"), // streaming
                                            ImmutableMap.of(),
                                            Optional.empty(),
                                            SINGLE,
                                            tableScan("test_customer4", ImmutableMap.of("custkey", "custkey", "name", "name"))))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer4");
        }
    }

    @Test
    public void testSortedByPrefixOfBucketedKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer5 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can't enable stream
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT custkey, COUNT(*) FROM test_customer5 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1", aggregationPlanWithNoStreaming("test_customer5", false, "custkey"));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer5");
        }
    }

    // Sorted keys and groupby keys
    @Test
    public void testGroupbySameKeysOfSortedbyKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer6 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name'], \n" +
                    "  sorted_by = ARRAY['custkey', 'name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can enable streaming aggregation
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT custkey, name, COUNT(*) FROM test_customer6 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2",
                    node(
                            OutputNode.class,
                            node(
                                    ExchangeNode.class,
                                    aggregation(
                                            singleGroupingSet("custkey", "name"),
                                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                            ImmutableList.of("custkey", "name"), // streaming
                                            ImmutableMap.of(),
                                            Optional.empty(),
                                            SINGLE,
                                            tableScan("test_customer6", ImmutableMap.of("custkey", "custkey", "name", "name"))))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer6");
        }
    }

    @Test
    public void testGroupbySameKeysOfSortedbyKeysWithReverseOrder()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer6_2 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name'], \n" +
                    "  sorted_by = ARRAY['custkey', 'name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can enable streaming aggregation
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT name, custkey, COUNT(*) FROM test_customer6_2 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2",
                    node(
                            OutputNode.class,
                            node(
                                    ExchangeNode.class,
                                    aggregation(
                                            singleGroupingSet("custkey", "name"),
                                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                            ImmutableList.of("custkey", "name"), // streaming
                                            ImmutableMap.of(),
                                            Optional.empty(),
                                            SINGLE,
                                            tableScan("test_customer6_2", ImmutableMap.of("custkey", "custkey", "name", "name"))))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer6_2");
        }
    }

    @Test
    public void testGroupbyKeysNotPrefixOfSortedKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer8 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name'], \n" +
                    "  sorted_by = ARRAY['custkey', 'name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can't enable streaming aggregation
            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT name, COUNT(*) FROM test_customer8 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1",
                    aggregationPlanWithNoStreaming("test_customer8", true, "name"));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer8");
        }
    }

    @Test
    public void testGroupbyKeysPrefixOfSortedKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer9 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey', 'name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can enable streaming aggregation
            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT custkey, COUNT(*) FROM test_customer9 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1",
                    node(
                            OutputNode.class,
                            node(
                                    ExchangeNode.class,
                                    aggregation(
                                            singleGroupingSet("custkey"),
                                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                            ImmutableList.of("custkey"), // streaming
                                            ImmutableMap.of(),
                                            Optional.empty(),
                                            SINGLE,
                                            tableScan("test_customer9", ImmutableMap.of("custkey", "custkey"))))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer9");
        }
    }

    // Partition keys
    @Test
    public void testQueryingMultiplePartitions()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_customer10 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");
            queryRunner.execute("INSERT INTO test_customer10 \n" +
                    "SELECT *, '2021-07-12' as ds FROM tpch.sf1.customer LIMIT 1000");

            // can't enable streaming aggregation when querying multiple partitions without grouping by partition keys
            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT custkey, COUNT(*) FROM test_customer10 \n" +
                            "WHERE ds = '2021-07-11' or ds = '2021-07-12' GROUP BY 1",
                    aggregationPlanWithNoStreaming("test_customer10", false, "custkey"));

            //todo: add streaming aggregation support when grouping keys contain all of the partition keys
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_customer10");
        }
    }

    private Session orderBasedExecutionEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, ORDER_BASED_EXECUTION_ENABLED, "true")
                .build();
    }

    private PlanMatchPattern aggregationPlanWithNoStreaming(String tableName, boolean hasProject, String... groupingKeys)
    {
        ImmutableMap.Builder<String, String> columnReferencesBuilder = ImmutableMap.builder();
        for (String groupingKey : groupingKeys) {
            columnReferencesBuilder.put(groupingKey, groupingKey);
        }
        PlanMatchPattern tableScanPattern = tableScan(tableName, columnReferencesBuilder.build());

        return anyTree(aggregation(
                singleGroupingSet(groupingKeys),
                // note: final aggregation function's parameter is of size one
                ImmutableMap.of(Optional.empty(), functionCall("count", false, ImmutableList.of(anySymbol()))),
                ImmutableList.of(), // non-streaming
                ImmutableMap.of(),
                Optional.empty(),
                FINAL,
                node(
                        ExchangeNode.class,
                        anyTree(aggregation(
                                singleGroupingSet(groupingKeys),
                                // note: partial aggregation function has no parameter
                                ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                ImmutableList.of(), // non-streaming
                                ImmutableMap.of(),
                                Optional.empty(),
                                PARTIAL,
                                hasProject ? node(ProjectNode.class, tableScanPattern) : tableScanPattern)))));
    }
}
