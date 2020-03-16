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

package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.BiConsumer;

import static com.facebook.presto.SystemSessionProperties.AGGREGATION_PARTITIONING_MERGING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PARTITIONING_PRECISION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.SystemSessionProperties.USE_STREAMING_EXCHANGE_FOR_MARK_DISTINCT;
import static com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy.ALL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartitioningPrecisionStrategy.PREFER_EXACT_PARTITIONING;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static org.testng.Assert.assertEquals;

/**
 * These are plan tests similar to what we have for other optimizers (e.g. {@link com.facebook.presto.sql.planner.TestPredicatePushdown})
 * They test that the plan for a query after the optimizer runs is as expected.
 * These are separate from {@link TestAddExchanges} because those are unit tests for
 * how layouts get chosen.
 */
public class TestAddExchangesPlans
        extends BasePlanTest
{
    @Test
    public void testRepartitionForUnionWithAnyTableScans()
    {
        assertDistributedPlan("SELECT nationkey FROM nation UNION select regionkey from region",
                anyTree(
                        aggregation(ImmutableMap.of(),
                                anyTree(
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))),
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                tableScan("region"))))))));
        assertDistributedPlan("SELECT nationkey FROM nation UNION select 1",
                anyTree(
                        aggregation(ImmutableMap.of(),
                                anyTree(
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))),
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                values())))))));
    }

    @Test
    public void testRepartitionForUnionAllBeforeHashJoin()
    {
        assertDistributedPlan("SELECT * FROM (SELECT nationkey FROM nation UNION ALL select nationkey from nation) n join region r on n.nationkey = r.regionkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("nationkey", "regionkey")),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("nation")))),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));

        assertDistributedPlan("SELECT * FROM (SELECT nationkey FROM nation UNION ALL select 1) n join region r on n.nationkey = r.regionkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("nationkey", "regionkey")),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        values()))),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
    }

    private void assertPlanWithMergePartitionStrategy(
            String sql,
            String partitionMergingStrategy,
            int remoteRepartitionExchangeCount,
            PlanMatchPattern pattern)
    {
        Session session = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(AGGREGATION_PARTITIONING_MERGING_STRATEGY, partitionMergingStrategy)
                .setSystemProperty(TASK_CONCURRENCY, "2")
                .build();
        BiConsumer<Plan, Integer> validateMultipleRemoteRepartitionExchange = (plan, count) -> assertEquals(
                searchFrom(plan.getRoot()).where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == REMOTE_STREAMING && ((ExchangeNode) node).getType() == REPARTITION).count(),
                count.intValue());

        assertPlanWithSession(sql, session, false, pattern, plan -> validateMultipleRemoteRepartitionExchange.accept(plan, remoteRepartitionExchangeCount));
    }

    @Test
    public void testMergePartitionWithGroupingSets()
    {
        String sql = "SELECT orderkey, count(distinct(custkey)) FROM orders GROUP BY GROUPING SETS((orderkey), ())";

        assertPlanWithMergePartitionStrategy(sql, "bottom_up", 2,
                anyTree(node(AggregationNode.class,
                        anyTree(exchange(REMOTE_STREAMING, REPARTITION,
                                node(AggregationNode.class,
                                        anyTree(node(AggregationNode.class,
                                                anyTree(exchange(REMOTE_STREAMING, REPARTITION,
                                                        node(AggregationNode.class,
                                                                anyTree(node(GroupIdNode.class,
                                                                        tableScan("orders"))))))))))))));
        assertPlanWithMergePartitionStrategy(sql, "top_down", 2,
                anyTree(node(AggregationNode.class,
                        anyTree(exchange(REMOTE_STREAMING, REPARTITION,
                                node(AggregationNode.class,
                                        anyTree(node(AggregationNode.class,
                                                anyTree(exchange(REMOTE_STREAMING, REPARTITION,
                                                        node(AggregationNode.class,
                                                                anyTree(node(GroupIdNode.class,
                                                                        tableScan("orders"))))))))))))));
    }

    @Test
    public void testMergePartitionWithAggregation()
    {
        String sql = "SELECT count(orderdate), custkey FROM (SELECT orderdate, custkey FROM orders GROUP BY orderdate, custkey) GROUP BY custkey";

        // disable merging partition preference
        assertPlanWithMergePartitionStrategy(sql, "bottom_up", 2,
                anyTree(node(AggregationNode.class,
                        anyTree(exchange(REMOTE_STREAMING, REPARTITION,
                                anyTree(node(AggregationNode.class,
                                        node(AggregationNode.class,
                                                anyTree(exchange(REMOTE_STREAMING, REPARTITION,
                                                        node(AggregationNode.class,
                                                                anyTree(tableScan("orders")))))))))))));

        // enable merging partition preference
        assertPlanWithMergePartitionStrategy(sql, "top_down", 1,
                anyTree(node(AggregationNode.class,
                        node(AggregationNode.class,
                                anyTree(exchange(REMOTE_STREAMING, REPARTITION,
                                        anyTree(node(AggregationNode.class,
                                                anyTree(tableScan("orders"))))))))));
    }

    @Test
    public void testAggregateIsExactlyPartitioned()
    {
        assertExactDistributedPlan(
                "SELECT\n" +
                        "    AVG(1)\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(*)\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        "    GROUP BY\n" +
                        "        orderkey,\n" +
                        "        orderstatus\n" +
                        ")\n" +
                        "GROUP BY\n" +
                        "    orderkey",
                anyTree(
                        exchange(REMOTE_STREAMING, REPARTITION,
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ordertatus", "orderstatus",
                                                                "orderkey", "orderkey",
                                                                "orderdate", "orderdate"))))))));
    }

    @Test
    public void testWindowIsExactlyPartitioned()
    {
        assertExactDistributedPlan(
                "SELECT\n" +
                        "    AVG(otherwindow) OVER (\n" +
                        "        PARTITION BY\n" +
                        "            orderkey\n" +
                        "    )\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(*) OVER (\n" +
                        "            PARTITION BY\n" +
                        "                orderkey,\n" +
                        "                orderstatus\n" +
                        "        ) AS otherwindow\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        ")",
                anyTree(
                        exchange(REMOTE_STREAMING, REPARTITION,
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of(
                                                                "orderkey", "orderkey",
                                                                "orderdate", "orderdate"))))))));
    }

    @Test
    public void testRowNumberIsExactlyPartitioned()
    {
        assertExactDistributedPlan(
                "SELECT\n" +
                        "    *\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        a,\n" +
                        "        ROW_NUMBER() OVER (\n" +
                        "            PARTITION BY\n" +
                        "                a\n" +
                        "        ) rn\n" +
                        "    FROM (\n" +
                        "        VALUES\n" +
                        "            (1)\n" +
                        "    ) t (a)\n" +
                        ") t",
                anyTree(
                        exchange(REMOTE_STREAMING, REPARTITION,
                                anyTree(
                                        values("a")))));
    }

    @Test
    public void testTopNRowNumberIsExactlyPartitioned()
    {
        assertExactDistributedPlan(
                "SELECT\n" +
                        "    a,\n" +
                        "    ROW_NUMBER() OVER (\n" +
                        "        PARTITION BY\n" +
                        "            a\n" +
                        "        ORDER BY\n" +
                        "            a\n" +
                        "    ) rn\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        a,\n" +
                        "        b,\n" +
                        "        COUNT(*)\n" +
                        "    FROM (\n" +
                        "        VALUES\n" +
                        "            (1, 2)\n" +
                        "    ) t (a, b)\n" +
                        "    GROUP BY\n" +
                        "        a,\n" +
                        "        b\n" +
                        ")\n" +
                        "LIMIT\n" +
                        "    2",
                anyTree(
                        exchange(REMOTE_STREAMING, REPARTITION,
                                anyTree(
                                        values("a", "b")))));
    }

    @Test
    public void testJoinExactlyPartitioned()
    {
        ExpectedValueProvider<FunctionCall> arbitrary = PlanMatchPattern.functionCall("arbitrary", false, ImmutableList.of(anySymbol()));
        assertExactDistributedPlan(
                "SELECT\n" +
                        "    orders.orderkey,\n" +
                        "    orders.orderstatus\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        ARBITRARY(orderstatus) AS orderstatus,\n" +
                        "        COUNT(*)\n" +
                        "    FROM orders\n" +
                        "    GROUP BY\n" +
                        "        orderkey\n" +
                        ") t,\n" +
                        "orders\n" +
                        "WHERE\n" +
                        "    orders.orderkey = t.orderkey\n" +
                        "    AND orders.orderstatus = t.orderstatus",
                anyTree(
                        join(INNER, ImmutableList.of(
                                equiJoinClause("ORDERKEY_LEFT", "ORDERKEY_RIGHT"),
                                equiJoinClause("orderstatus", "ORDERSTATUS_RIGHT")),
                                exchange(REMOTE_STREAMING, REPARTITION,
                                        anyTree(
                                                aggregation(
                                                        singleGroupingSet("ORDERKEY_LEFT"),
                                                        ImmutableMap.of(Optional.of("orderstatus"), arbitrary),
                                                        ImmutableList.of("ORDERKEY_LEFT"),
                                                        ImmutableMap.of(),
                                                        Optional.empty(),
                                                        SINGLE,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY_LEFT", "orderkey",
                                                                "ORDERSTATUS_LEFT", "orderstatus"))))),
                                exchange(LOCAL, REPARTITION,
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY_RIGHT", "orderkey",
                                                                "ORDERSTATUS_RIGHT", "orderstatus"))))))));
    }

    @Test
    public void testSemiJoinExactlyPartitioned()
    {
        assertExactDistributedPlan(
                "SELECT\n" +
                        "    orderkey\n" +
                        "FROM orders\n" +
                        "WHERE\n" +
                        "    orderkey IN (\n" +
                        "        SELECT\n" +
                        "            orderkey\n" +
                        "        FROM orders\n" +
                        "        WHERE\n" +
                        "            orderkey IS NULL\n" +
                        "            AND orderstatus IS NULL\n" +
                        "    )",
                anyTree(
                        semiJoin("ORDERKEY_OK", "VALUE_ORDERKEY", "S",
                                exchange(REMOTE_STREAMING, REPARTITION,
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of(
                                                        "ORDERKEY_OK", "orderkey")))),
                                anyTree(

                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        values("VALUE_ORDERKEY")))))));
    }

    @Test
    public void testMarkDistinctIsExactlyPartitioned()
    {
        assertExactDistributedPlan(
                "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(DISTINCT orderdate),\n" +
                        "        COUNT(DISTINCT clerk)\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        "    GROUP BY\n" +
                        "        orderkey,\n" +
                        "        orderstatus\n",
                anyTree(
                        exchange(REMOTE_STREAMING, REPARTITION,
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                                anyTree(
                                                                        tableScan("orders", ImmutableMap.of(
                                                                                "orderstatus", "orderstatus",
                                                                                "orderkey", "orderkey",
                                                                                "clerk", "clerk",
                                                                                "orderdate", "orderdate"))))))))));
    }

    @Test
    public void testMarkDistinctStreamingExchange()
    {
        assertMaterializedWithStreamingMarkDistinctDistributedPlan(
                "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(DISTINCT orderdate),\n" +
                        "        COUNT(DISTINCT clerk)\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        "    GROUP BY\n" +
                        "        orderkey,\n" +
                        "        orderstatus\n",
                anyTree(
                        exchange(REMOTE_MATERIALIZED, REPARTITION,
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                                anyTree(
                                                                        tableScan("orders", ImmutableMap.of(
                                                                                "orderstatus", "orderstatus",
                                                                                "orderkey", "orderkey",
                                                                                "clerk", "clerk",
                                                                                "orderdate", "orderdate"))))))))));
    }

    void assertMaterializedWithStreamingMarkDistinctDistributedPlan(String sql, PlanMatchPattern pattern)
    {
        assertDistributedPlan(
                sql,
                TestingSession.testSessionBuilder()
                        .setCatalog("local")
                        .setSchema("tiny")
                        .setSystemProperty(PARTITIONING_PRECISION_STRATEGY, PREFER_EXACT_PARTITIONING.toString())
                        .setSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, ALL.toString())
                        .setSystemProperty(USE_STREAMING_EXCHANGE_FOR_MARK_DISTINCT, "true")
                        .build(),
                pattern);
    }

    void assertExactDistributedPlan(String sql, PlanMatchPattern pattern)
    {
        assertDistributedPlan(
                sql,
                TestingSession.testSessionBuilder()
                        .setCatalog("local")
                        .setSchema("tiny")
                        .setSystemProperty(PARTITIONING_PRECISION_STRATEGY, PREFER_EXACT_PARTITIONING.toString())
                        .build(),
                pattern);
    }
}
