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
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static com.facebook.presto.SystemSessionProperties.AGGREGATION_PARTITIONING_MERGING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
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
}
