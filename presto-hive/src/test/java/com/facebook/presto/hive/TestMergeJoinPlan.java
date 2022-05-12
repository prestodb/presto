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
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PREFER_MERGE_JOIN;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.ORDER_BASED_EXECUTION_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.mergeJoin;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;

public class TestMergeJoinPlan
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, LINE_ITEM, CUSTOMER, NATION),
                ImmutableMap.of(),
                Optional.empty());
    }

    @Test
    public void testSessionProperty()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_join_customer WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.customer LIMIT 1000");

            queryRunner.execute("CREATE TABLE test_join_order WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.\"orders\" LIMIT 1000");

            // By default, we can't enable merge join
            assertPlan(
                    "select * from test_join_customer join test_join_order on test_join_customer.custkey = test_join_order.custkey",
                    joinPlan("test_join_customer", "test_join_order", ImmutableList.of("custkey"), ImmutableList.of("custkey"), false));

            // when we miss session property, we can't enable merge join
            assertPlan(
                    "select * from test_join_customer join test_join_order on test_join_customer.custkey = test_join_order.custkey",
                    joinPlan("test_join_customer", "test_join_order", ImmutableList.of("custkey"), ImmutableList.of("custkey"), false));

            // When merge join session property is turned on and data properties requirements for merge join are met
            assertPlan(
                    mergeJoinEnabled(),
                    "select * from test_join_customer join test_join_order on test_join_customer.custkey = test_join_order.custkey",
                    joinPlan("test_join_customer", "test_join_order", ImmutableList.of("custkey"), ImmutableList.of("custkey"), true));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_join_customer");
            queryRunner.execute("DROP TABLE IF EXISTS test_join_order");
        }
    }

    @Test
    public void testDifferentBucketedByKey()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_join_customer2 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['name'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.customer LIMIT 1000");

            queryRunner.execute("CREATE TABLE test_join_order2 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.\"orders\" LIMIT 1000");

            // merge join can't be enabled
            assertPlan(
                    mergeJoinEnabled(),
                    "select * from test_join_customer2 join test_join_order2 on test_join_customer2.custkey = test_join_order2.custkey",
                    joinPlan("test_join_customer2", "test_join_order2", ImmutableList.of("custkey"), ImmutableList.of("custkey"), false));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_join_customer2");
            queryRunner.execute("DROP TABLE IF EXISTS test_join_order2");
        }
    }

    @Test
    public void testDifferentSortByKey()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_join_customer3 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['name'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.customer LIMIT 1000");

            queryRunner.execute("CREATE TABLE test_join_order3 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.\"orders\" LIMIT 1000");

            // merge join can't be enabled
            assertPlan(
                    mergeJoinEnabled(),
                    "select * from test_join_customer3 join test_join_order3 on test_join_customer3.custkey = test_join_order3.custkey",
                    joinPlan("test_join_customer3", "test_join_order3", ImmutableList.of("custkey"), ImmutableList.of("custkey"), false));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_join_customer3");
            queryRunner.execute("DROP TABLE IF EXISTS test_join_order3");
        }
    }

    @Test
    public void testMultipleSortByKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_join_customer4 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey', 'name'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.customer LIMIT 1000");

            queryRunner.execute("CREATE TABLE test_join_order4 WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.\"orders\" LIMIT 1000");

            // merge join can be enabled
            assertPlan(
                    mergeJoinEnabled(),
                    "select * from test_join_customer4 join test_join_order4 on test_join_customer4.custkey = test_join_order4.custkey",
                    joinPlan("test_join_customer4", "test_join_order4", ImmutableList.of("custkey"), ImmutableList.of("custkey"), true));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_join_customer4");
            queryRunner.execute("DROP TABLE IF EXISTS test_join_order4");
        }
    }

    @Test
    public void testMultipleJoinKeys()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_join_customer5(" +
                    " \"custkey\" bigint, \"name\" varchar(25), \"address\" varchar(40), \"orderkey\" bigint, \"phone\" varchar(15),                                \n" +
                    " \"acctbal\" double, \"mktsegment\" varchar(10), \"comment\" varchar(117), \"ds\" varchar(10)) WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'orderkey'], \n" +
                    "  sorted_by = ARRAY['custkey', 'orderkey'], partitioned_by=array['ds'], \n" +
                    "  format = 'DWRF' )");
            queryRunner.execute("INSERT INTO test_join_customer5 \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.customer LIMIT 1000");

            queryRunner.execute("CREATE TABLE test_join_order5(" +
                    " \"orderkey\" bigint, \"custkey\" bigint, \"orderstatus\" varchar(1), \"totalprice\" double, \"orderdate\" date," +
                    " \"orderpriority\" varchar(15), \"clerk\" varchar(15), \"shippriority\" integer, \"comment\" varchar(79),  \"ds\" varchar(10)) WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey', 'orderkey'], \n" +
                    "  sorted_by = ARRAY['custkey', 'orderkey'], partitioned_by=array['ds'])");
            queryRunner.execute("INSERT INTO test_join_order5 \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.orders LIMIT 1000");

            // merge join can be enabled
            assertPlan(
                    mergeJoinEnabled(),
                    "select * from test_join_customer5 join test_join_order5 on test_join_customer5.custkey = test_join_order5.custkey and test_join_customer5.orderkey = test_join_order5.orderkey",
                    joinPlan("test_join_customer5", "test_join_order5", ImmutableList.of("custkey", "orderkey"), ImmutableList.of("custkey", "orderkey"), true));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_join_customer5");
            queryRunner.execute("DROP TABLE IF EXISTS test_join_order5");
        }
    }

    @Test
    public void testMultiplePartitions()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_join_customer_multi_partitions WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.customer LIMIT 1000");
            queryRunner.execute("INSERT INTO test_join_customer_multi_partitions \n" +
                    "SELECT *, '2021-07-12' as ds FROM tpch.sf1.customer LIMIT 1000");

            queryRunner.execute("CREATE TABLE test_join_order_multi_partitions WITH ( \n" +
                    "  bucket_count = 4, bucketed_by = ARRAY['custkey'], \n" +
                    "  sorted_by = ARRAY['custkey'], partitioned_by=array['ds']) AS \n" +
                    "SELECT *, '2021-07-11' as ds FROM tpch.sf1.\"orders\" LIMIT 1000");
            queryRunner.execute("INSERT INTO test_join_order_multi_partitions \n" +
                    "SELECT *, '2021-07-12' as ds FROM tpch.sf1.orders LIMIT 1000");

            // When partition key doesn't not appear in join keys and we query multiple partitions, we can't enable merge join
            assertPlan(
                    mergeJoinEnabled(),
                    "select * from test_join_customer_multi_partitions join test_join_order_multi_partitions on test_join_customer_multi_partitions.custkey = test_join_order_multi_partitions.custkey",
                    joinPlan("test_join_customer_multi_partitions", "test_join_order_multi_partitions", ImmutableList.of("custkey"), ImmutableList.of("custkey"), false));
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_join_customer_multi_partitions");
            queryRunner.execute("DROP TABLE IF EXISTS test_join_order_multi_partitions");
        }
    }

    private Session mergeJoinEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PREFER_MERGE_JOIN, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, ORDER_BASED_EXECUTION_ENABLED, "true")
                .build();
    }

    private PlanMatchPattern joinPlan(String leftTableName, String rightTableName, List<String> leftJoinKeys, List<String> rightJoinKeys, boolean mergeJoinEnabled)
    {
        int suffix1 = 0;
        int suffix2 = 1;
        ImmutableMap.Builder<String, String> leftColumnReferencesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> rightColumnReferencesBuilder = ImmutableMap.builder();
        ImmutableList.Builder joinClauses = ImmutableList.builder();
        for (int i = 0; i < leftJoinKeys.size(); i++) {
            leftColumnReferencesBuilder.put(leftJoinKeys.get(i) + suffix1, leftJoinKeys.get(i));
            rightColumnReferencesBuilder.put(rightJoinKeys.get(i) + suffix2, rightJoinKeys.get(i));
            joinClauses.add(equiJoinClause(leftJoinKeys.get(i) + suffix1, rightJoinKeys.get(i) + suffix2));
            suffix1 = suffix1 + 2;
            suffix2 = suffix2 + 2;
        }

        return mergeJoinEnabled ?
                anyTree(mergeJoin(
                        INNER,
                        joinClauses.build(),
                        Optional.empty(),
                        PlanMatchPattern.tableScan(leftTableName, leftColumnReferencesBuilder.build()),
                        PlanMatchPattern.tableScan(rightTableName, rightColumnReferencesBuilder.build()))) :
                anyTree(join(
                        INNER,
                        joinClauses.build(),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        anyTree(PlanMatchPattern.tableScan(leftTableName, leftColumnReferencesBuilder.build())),
                        anyTree(PlanMatchPattern.tableScan(rightTableName, rightColumnReferencesBuilder.build()))));
    }
}
