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
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_CONDITIONAL_AGGREGATION_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestRewriteIfOverAggregation
        extends BasePlanTest
{
    private Session enableOptimization()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_CONDITIONAL_AGGREGATION_ENABLED, "true")
                .build();
    }

    @Test
    public void testConditionOnGrouping()
    {
        assertPlan("SELECT orderstatus, shippriority, IF(GROUPING(orderstatus, shippriority) = 0, sum(totalprice)) "
                        + "FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderstatus, shippriority))",
                enableOptimization(),
                anyTree(
                        aggregation(
                                ImmutableMap.of("pricesum", functionCall("sum", ImmutableList.of("totalprice"))),
                                project(
                                        ImmutableMap.of("mask", expression("array[1, 0][groupid+1]=0")),
                                        groupingSet(
                                                ImmutableList.of(ImmutableList.of("orderstatus"), ImmutableList.of("orderstatus", "shippriority")),
                                                ImmutableMap.of("totalprice", "totalprice"),
                                                "groupid",
                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderstatus", "orderstatus", "shippriority", "shippriority")))))));
    }

    // Should not be rewritten
    @Test
    public void testConditionOnAggregation()
    {
        assertPlan("select orderpriority, if(count(1)>3000, avg(totalprice)) from orders group by orderpriority ",
                enableOptimization(),
                anyTree(
                        project(
                                ImmutableMap.of("ifexp", expression("if(count > 3000, avg, null)")),
                                aggregation(
                                        ImmutableMap.of("avg", functionCall("avg", ImmutableList.of("partial_avg")), "count", functionCall("count", ImmutableList.of("partial_count"))),
                                        exchange(
                                                aggregation(
                                                        ImmutableMap.of("partial_avg", functionCall("avg", ImmutableList.of("totalprice")), "partial_count", functionCall("count", ImmutableList.of())),
                                                        project(
                                                                ImmutableMap.of("totalprice", expression("totalprice"), "orderpriority", expression("orderpriority")),
                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority")))))))));
    }

    @Test
    public void testMultipleArgumentsAggregation()
    {
        assertPlan("SELECT orderstatus, shippriority, IF(GROUPING(orderstatus, shippriority) = 0, max_by(shippriority, totalprice)) "
                        + "FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderstatus, shippriority))",
                enableOptimization(),
                anyTree(
                        aggregation(
                                ImmutableMap.of("result", functionCall("max_by", ImmutableList.of("shippriority", "totalprice"))),
                                project(
                                        ImmutableMap.of("mask", expression("array[1, 0][groupid+1]=0")),
                                        groupingSet(
                                                ImmutableList.of(ImmutableList.of("orderstatus"), ImmutableList.of("orderstatus", "shippriority")),
                                                ImmutableMap.of("totalprice", "totalprice", "shippriority", "shippriority"),
                                                "groupid",
                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderstatus", "orderstatus", "shippriority", "shippriority")))))));
    }
}
