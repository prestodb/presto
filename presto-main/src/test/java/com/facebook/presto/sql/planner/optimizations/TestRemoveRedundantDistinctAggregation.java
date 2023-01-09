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

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestRemoveRedundantDistinctAggregation
        extends BasePlanTest
{
    @Test
    public void testDistinctOverSingleGroupBy()
    {
        assertPlan("SELECT DISTINCT orderpriority, SUM(totalprice) FROM orders GROUP BY orderpriority",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                        FINAL,
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        project(
                                                                ImmutableMap.of(),
                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority")))))))));
    }

    @Test
    public void testDistinctOverSingleGroupingSet()
    {
        assertPlan("SELECT DISTINCT orderpriority, SUM(totalprice) FROM orders GROUP BY GROUPING SETS ((orderpriority))",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                        FINAL,
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        project(
                                                                ImmutableMap.of(),
                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority")))))))));
    }

    // Should not trigger
    @Test
    public void testDistinctOverMultipleGroupingSet()
    {
        assertPlan("SELECT DISTINCT orderpriority, orderstatus, SUM(totalprice) FROM orders GROUP BY GROUPING SETS ((orderpriority), (orderstatus))",
                output(
                        anyTree(
                                aggregation(
                                        ImmutableMap.of(),
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(
                                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                                        PARTIAL,
                                                                        project(
                                                                                ImmutableMap.of(),
                                                                                groupingSet(
                                                                                        ImmutableList.of(ImmutableList.of("orderpriority"), ImmutableList.of("orderstatus")),
                                                                                        ImmutableMap.of("totalprice", "totalprice"),
                                                                                        "groupid",
                                                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority", "orderstatus", "orderstatus"))))))))))));
    }

    @Test
    public void testDistinctWithRandom()
    {
        assertPlan("SELECT DISTINCT orderpriority, random(), SUM(totalprice) FROM orders GROUP BY orderpriority",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                        FINAL,
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        project(
                                                                ImmutableMap.of(),
                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority")))))))));
    }

    @Test
    public void testDistinctWithRandomFromGroupBy()
    {
        assertPlan("SELECT DISTINCT orderpriority, random(), sum from (select orderpriority, SUM(totalprice) as sum FROM orders GROUP BY orderpriority)",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                        FINAL,
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        project(
                                                                ImmutableMap.of(),
                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority")))))))));
    }

    // Does not trigger optimization
    @Test
    public void testDistinctOverSubsetOfGroupBy()
    {
        assertPlan("SELECT DISTINCT orderpriority, sum FROM (SELECT orderpriority, orderstatus, SUM(totalprice) AS sum FROM orders GROUP BY orderpriority, orderstatus)",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of(),
                                        project(
                                                aggregation(
                                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(
                                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                                        PARTIAL,
                                                                        project(
                                                                                ImmutableMap.of(),
                                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority", "orderstatus", "orderstatus")))))))))));
    }

    // Does not trigger
    @Test
    public void testDistinctExpressionWithGroupBy()
    {
        assertPlan("SELECT DISTINCT orderkey+1 AS orderkey, sum FROM (SELECT orderkey, SUM(totalprice) AS sum FROM orders GROUP BY orderkey)",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of(),
                                        FINAL,
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of(),
                                                        PARTIAL,
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("expr", expression("orderkey+1")),
                                                                        aggregation(
                                                                                ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                                                SINGLE,
                                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderkey", "orderkey")))))))))));
    }

    // Does not trigger
    @Test
    public void testJoinWithGroupByKey()
    {
        assertPlan("select distinct orderkey, avg, tax from (select orderkey, sum(totalprice) avg from orders group by orderkey) as t1 join lineitem using(orderkey)",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of(),
                                        anyTree(
                                                join(
                                                        INNER,
                                                        ImmutableList.of(equiJoinClause("l_orderkey", "orderkey")),
                                                        project(
                                                                ImmutableMap.of(),
                                                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey", "tax", "tax"))),
                                                        project(
                                                                aggregation(
                                                                        ImmutableMap.of("finallsum", functionCall("sum", ImmutableList.of("partialsum"))),
                                                                        FINAL,
                                                                        anyTree(
                                                                                aggregation(
                                                                                        ImmutableMap.of("partialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                                                        PARTIAL,
                                                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderkey", "orderkey"))))))))))));
    }

    // Does not trigger
    @Test
    public void testJoinWithGroupByOnDifferentKey()
    {
        assertPlan("select distinct orderstatus, orderkey from (select orderstatus, max_by(orderkey, totalprice) orderkey from orders group by orderstatus) as t1 join lineitem using(orderkey)",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of(),
                                        anyTree(
                                                join(
                                                        INNER,
                                                        ImmutableList.of(equiJoinClause("max_by", "orderkey_10")),
                                                        project(
                                                                aggregation(
                                                                        ImmutableMap.of("max_by", functionCall("max_by", ImmutableList.of("max_by_24"))),
                                                                        FINAL,
                                                                        anyTree(
                                                                                aggregation(
                                                                                        ImmutableMap.of("max_by_24", functionCall("max_by", ImmutableList.of("orderkey", "totalprice"))),
                                                                                        PARTIAL,
                                                                                        project(
                                                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderkey", "orderkey"))))))),
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of(),
                                                                        tableScan("lineitem", ImmutableMap.of("orderkey_10", "orderkey"))))))))));
    }

    @Test
    public void testAggregationOverDistinct()
    {
        assertPlan("select orderstatus, max_by(orderpriority, sum) from (select distinct orderstatus, orderpriority, sum(totalprice) as sum from orders group by orderstatus, orderpriority) group by orderstatus",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of("max_by", functionCall("max_by", ImmutableList.of("orderpriority", "sum"))),
                                        project(
                                                aggregation(
                                                        ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(
                                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                                        PARTIAL,
                                                                        project(
                                                                                ImmutableMap.of(),
                                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority", "orderstatus", "orderstatus")))))))))));
    }
}
