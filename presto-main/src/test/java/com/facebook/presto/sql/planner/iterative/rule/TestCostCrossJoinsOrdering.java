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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.CALCULATE_CROSS_JOIN_COST;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestCostCrossJoinsOrdering
        extends BasePlanTest
{
    public TestCostCrossJoinsOrdering()
    {
        super(ImmutableMap.of(
                CALCULATE_CROSS_JOIN_COST, "true",
                JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.AUTOMATIC.name(),
                JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.AUTOMATIC.name()));
    }

    @Test
    public void testCrossJoinIsUsedInJoinReordering()
    {
        String query = "select 1 FROM supplier s , lineitem l , orders o where l.orderkey = o.orderkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.of(REPLICATED),
                                tableScan("supplier"),
                                exchange(join(INNER,
                                        ImmutableList.of(equiJoinClause("L_ORDERKEY", "O_ORDERKEY")),
                                        anyTree(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))),
                                        anyTree(tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testInnerJoinWithPredicate()
    {
        assertPlan("SELECT 1 from orders, lineitem, partsupp, region where partsupp.partkey = lineitem.partkey and lineitem.quantity > 7.5",
                anyTree(
                        join(INNER,
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.of(REPLICATED),
                                tableScan("orders"),
                                anyTree(
                                        join(INNER,
                                                ImmutableList.of(),
                                                Optional.empty(),
                                                Optional.of(REPLICATED),
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("L_PARTKEY", "R_PARTKEY")),
                                                        anyTree(tableScan("lineitem", ImmutableMap.of("L_PARTKEY", "partkey"))),
                                                        anyTree(tableScan("partsupp", ImmutableMap.of("R_PARTKEY", "partkey")))),
                                                anyTree(tableScan("region")))))));
    }

    @Test
    public void testMultiInnerJoin()
    {
        assertPlan("SELECT 1 from orders, customer, supplier, nation where orders.custkey = customer.custkey and nation.nationkey = supplier.nationkey",
                anyTree(
                        join(INNER,
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.of(REPLICATED),
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("L_CUSTKEY", "R_CUSTKEY")),
                                        anyTree(tableScan("orders", ImmutableMap.of("L_CUSTKEY", "custkey"))),
                                        anyTree(tableScan("customer", ImmutableMap.of("R_CUSTKEY", "custkey")))),
                                anyTree(join(INNER,
                                        ImmutableList.of(equiJoinClause("L_NATIONKEY", "R_NATIONKEY")),
                                        anyTree(tableScan("supplier", ImmutableMap.of("L_NATIONKEY", "nationkey"))),
                                        anyTree(tableScan("nation", ImmutableMap.of("R_NATIONKEY", "nationkey"))))))));
    }
}
