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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.SEGMENTED_AGGREGATION_ENABLED;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.ORDER_BASED_EXECUTION_ENABLED;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;

public class TestSegmentedAggregation
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
    public void testSortedbyKeysPrefixNotASubsetOfGroupbyKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_aggregation_customer0 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['name', 'custkey'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can't enable segmented aggregation
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT custkey, count(name) FROM test_segmented_aggregation_customer0 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1",
                    anyTree(aggregation(
                            singleGroupingSet("custkey"),
                            ImmutableMap.of(Optional.of("count"), functionCall("count", ImmutableList.of("name"))),
                            ImmutableList.of(), // no segmented streaming
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            tableScan("test_segmented_aggregation_customer0", ImmutableMap.of("custkey", "custkey", "name", "name")))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_aggregation_customer0");
        }
    }

    @Test
    public void testAndSortedByKeysArePrefixOfGroupbyKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_aggregation_customer WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name'], \n" +
                    "  sorted_by = ARRAY['custkey', 'name'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            assertPlan(
                    orderBasedExecutionEnabled(),
                    "SELECT custkey, name, nationkey, COUNT(*) FROM test_segmented_aggregation_customer \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2, 3",
                    anyTree(aggregation(
                            singleGroupingSet("custkey", "name", "nationkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                            ImmutableList.of("custkey", "name"), // segmented streaming
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            tableScan("test_segmented_aggregation_customer", ImmutableMap.of("custkey", "custkey", "name", "name", "nationkey", "nationkey")))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_aggregation_customer");
        }
    }

    @Test
    public void testSortedByPrefixOfBucketedKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_aggregation_customer2 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can enable segmented aggregation
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT name, custkey, COUNT(*) FROM test_segmented_aggregation_customer2 \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2",
                    anyTree(aggregation(
                            singleGroupingSet("name", "custkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                            ImmutableList.of("custkey"), // segmented aggregation
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            tableScan("test_segmented_aggregation_customer2", ImmutableMap.of("name", "name", "custkey", "custkey")))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_aggregation_customer2");
        }
    }

    @Test
    public void testGroupByKeysShareElementsAsSortedByKeysPrefix()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_segmented_aggregation_customer_share_elements WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'name', 'nationkey'], \n" +
                    "  sorted_by = ARRAY['custkey', 'phone'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' ) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM customer LIMIT 1000\n");

            // can enable segmented aggregation
            assertPlan(orderBasedExecutionEnabled(),
                    "SELECT name, custkey, nationkey, COUNT(*) FROM test_segmented_aggregation_customer_share_elements \n" +
                            "WHERE ds = '2021-07-11' GROUP BY 1, 2, 3",
                    anyTree(aggregation(
                            singleGroupingSet("name", "custkey", "nationkey"),
                            ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                            ImmutableList.of("custkey"), // segmented aggregation
                            ImmutableMap.of(),
                            Optional.empty(),
                            SINGLE,
                            tableScan("test_segmented_aggregation_customer_share_elements", ImmutableMap.of("name", "name", "custkey", "custkey", "nationkey", "nationkey")))));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_segmented_aggregation_customer_share_elements");
        }
    }

    private Session orderBasedExecutionEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, ORDER_BASED_EXECUTION_ENABLED, "true")
                .setSystemProperty(SEGMENTED_AGGREGATION_ENABLED, "true")
                .build();
    }
}
