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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER;
import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_STRATEGY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.GroupingSetDescriptor;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

public class TestMergePartialAggregationsWithFilter
        extends BasePlanTest
{
    private Session enableOptimization()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "true")
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .build();
    }

    private Session disableOptimization()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "false")
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .build();
    }

    @Test
    public void testOptimizationApplied()
    {
        assertPlan("SELECT partkey, sum(quantity), sum(quantity) filter (where orderkey > 0) from lineitem group by partkey",
                enableOptimization(),
                anyTree(
                        aggregation(
                                singleGroupingSet("partkey"),
                                ImmutableMap.of(Optional.of("finalSum"), functionCall("sum", ImmutableList.of("partialSum")), Optional.of("maskFinalSum"), functionCall("sum", ImmutableList.of("maskPartialSum"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                project(
                                        ImmutableMap.of("maskPartialSum", expression("IF(expr, partialSum, null)")),
                                        anyTree(
                                                aggregation(
                                                        singleGroupingSet("partkey", "expr"),
                                                        ImmutableMap.of(Optional.of("partialSum"), functionCall("sum", ImmutableList.of("quantity"))),
                                                        ImmutableMap.of(),
                                                        Optional.empty(),
                                                        AggregationNode.Step.PARTIAL,
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("expr", expression("orderkey > 0")),
                                                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "partkey", "partkey", "quantity", "quantity"))))))))),
                false);
    }

    @Test
    public void testOptimizationDisabled()
    {
        assertPlan("SELECT partkey, sum(quantity), sum(quantity) filter (where orderkey > 0) from lineitem group by partkey",
                disableOptimization(),
                anyTree(
                        aggregation(
                                singleGroupingSet("partkey"),
                                ImmutableMap.of(Optional.of("finalSum"), functionCall("sum", ImmutableList.of("partialSum")), Optional.of("maskFinalSum"), functionCall("sum", ImmutableList.of("maskPartialSum"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                anyTree(
                                        aggregation(
                                                singleGroupingSet("partkey"),
                                                ImmutableMap.of(Optional.of("partialSum"), functionCall("sum", ImmutableList.of("quantity")), Optional.of("maskPartialSum"), functionCall("sum", ImmutableList.of("quantity"))),
                                                ImmutableMap.of(new Symbol("maskPartialSum"), new Symbol("expr")),
                                                Optional.empty(),
                                                AggregationNode.Step.PARTIAL,
                                                project(
                                                        ImmutableMap.of("expr", expression("orderkey > 0")),
                                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "partkey", "partkey", "quantity", "quantity"))))))),
                false);
    }

    @Test
    public void testMultipleAggregations()
    {
        assertPlan("SELECT partkey, sum(quantity), sum(quantity) filter (where orderkey > 0), avg(quantity), avg(quantity) filter (where orderkey > 0) from lineitem group by partkey",
                enableOptimization(),
                anyTree(
                        aggregation(
                                singleGroupingSet("partkey"),
                                ImmutableMap.of(Optional.of("finalSum"), functionCall("sum", ImmutableList.of("partialSum")), Optional.of("maskFinalSum"), functionCall("sum", ImmutableList.of("maskPartialSum")),
                                        Optional.of("finalAvg"), functionCall("avg", ImmutableList.of("partialAvg")), Optional.of("maskFinalAvg"), functionCall("avg", ImmutableList.of("maskPartialAvg"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                project(
                                        ImmutableMap.of("maskPartialSum", expression("IF(expr, partialSum, null)"), "maskPartialAvg", expression("IF(expr, partialAvg, null)")),
                                        anyTree(
                                                aggregation(
                                                        singleGroupingSet("partkey", "expr"),
                                                        ImmutableMap.of(Optional.of("partialSum"), functionCall("sum", ImmutableList.of("quantity")), Optional.of("partialAvg"), functionCall("avg", ImmutableList.of("quantity"))),
                                                        ImmutableMap.of(),
                                                        Optional.empty(),
                                                        AggregationNode.Step.PARTIAL,
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("expr", expression("orderkey > 0")),
                                                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "partkey", "partkey", "quantity", "quantity"))))))))),
                false);
    }

    @Test
    public void testAggregationsMultipleLevel()
    {
        assertPlan("select partkey, avg(sum), avg(sum) filter (where suppkey > 0), avg(filtersum) from (select partkey, suppkey, sum(quantity) sum, sum(quantity) filter (where orderkey > 0) filtersum from lineitem group by partkey, suppkey) t group by partkey",
                enableOptimization(),
                anyTree(
                        aggregation(
                                singleGroupingSet("partkey"),
                                ImmutableMap.of(Optional.of("finalAvg"), functionCall("avg", ImmutableList.of("partialAvg")), Optional.of("maskFinalAvg"), functionCall("avg", ImmutableList.of("maskPartialAvg")),
                                        Optional.of("finalFilterAvg"), functionCall("avg", ImmutableList.of("partialFilterAvg"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                project(
                                        ImmutableMap.of("maskPartialAvg", expression("IF(expr_2, partialAvg, null)")),
                                        anyTree(
                                                aggregation(
                                                        singleGroupingSet("partkey", "expr_2"),
                                                        ImmutableMap.of(Optional.of("partialAvg"), functionCall("avg", ImmutableList.of("finalSum")), Optional.of("partialFilterAvg"), functionCall("avg", ImmutableList.of("maskFinalSum"))),
                                                        ImmutableMap.of(),
                                                        Optional.empty(),
                                                        AggregationNode.Step.PARTIAL,
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("expr_2", expression("suppkey > 0")),
                                                                        aggregation(
                                                                                singleGroupingSet("partkey", "suppkey"),
                                                                                ImmutableMap.of(Optional.of("finalSum"), functionCall("sum", ImmutableList.of("partialSum")), Optional.of("maskFinalSum"), functionCall("sum", ImmutableList.of("maskPartialSum"))),
                                                                                ImmutableMap.of(),
                                                                                Optional.empty(),
                                                                                AggregationNode.Step.FINAL,
                                                                                project(
                                                                                        ImmutableMap.of("maskPartialSum", expression("IF(expr, partialSum, null)")),
                                                                                        anyTree(
                                                                                                aggregation(
                                                                                                        singleGroupingSet("partkey", "suppkey", "expr"),
                                                                                                        ImmutableMap.of(Optional.of("partialSum"), functionCall("sum", ImmutableList.of("quantity"))),
                                                                                                        ImmutableMap.of(),
                                                                                                        Optional.empty(),
                                                                                                        AggregationNode.Step.PARTIAL,
                                                                                                        anyTree(
                                                                                                                project(
                                                                                                                        ImmutableMap.of("expr", expression("orderkey > 0")),
                                                                                                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "partkey", "partkey", "quantity", "quantity", "suppkey", "suppkey"))))))))))))))),
                false);
    }

    @Test
    public void testGlobalOptimization()
    {
        assertPlan("SELECT sum(quantity), sum(quantity) filter (where orderkey > 0) from lineitem",
                enableOptimization(),
                anyTree(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("finalSum"), functionCall("sum", ImmutableList.of("partialSum")), Optional.of("maskFinalSum"), functionCall("sum", ImmutableList.of("maskPartialSum"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                anyTree(
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(Optional.of("partialSum"), functionCall("sum", ImmutableList.of("quantity")), Optional.of("maskPartialSum"), functionCall("sum", ImmutableList.of("quantity"))),
                                                ImmutableMap.of(new Symbol("maskPartialSum"), new Symbol("expr")),
                                                Optional.empty(),
                                                AggregationNode.Step.PARTIAL,
                                                project(
                                                        ImmutableMap.of("expr", expression("orderkey > 0")),
                                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "quantity", "quantity"))))))),
                false);
    }

    @Test
    public void testHasOrderBy()
    {
        assertPlan("select partkey, array_agg(suppkey order by suppkey), array_agg(suppkey order by suppkey) filter (where orderkey > 0) from lineitem group by partkey",
                enableOptimization(),
                anyTree(
                        aggregation(
                                singleGroupingSet("partkey"),
                                ImmutableMap.of(Optional.of("array_agg"), functionCall("array_agg", ImmutableList.of("suppkey"), ImmutableList.of(sort("suppkey", ASCENDING, LAST))),
                                        Optional.of("array_agg_filter"), functionCall("array_agg", ImmutableList.of("suppkey"), ImmutableList.of(sort("suppkey", ASCENDING, LAST)))),
                                ImmutableMap.of(new Symbol("array_agg_filter"), new Symbol("expr")),
                                Optional.empty(),
                                AggregationNode.Step.SINGLE,
                                anyTree(
                                        project(
                                                ImmutableMap.of("expr", expression("orderkey > 0")),
                                                tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "partkey", "partkey", "suppkey", "suppkey")))))),
                false);
    }

    @Test
    public void testGroupingSets()
    {
        assertPlan("SELECT partkey, sum(quantity), sum(quantity) filter (where orderkey > 0) from lineitem group by grouping sets((), (partkey))",
                enableOptimization(),
                anyTree(
                        aggregation(
                                new GroupingSetDescriptor(ImmutableList.of("partkey$gid", "groupid"), 2, ImmutableSet.of(0)),
                                ImmutableMap.of(Optional.of("finalSum"), functionCall("sum", ImmutableList.of("partialSum")), Optional.of("maskFinalSum"), functionCall("sum", ImmutableList.of("maskPartialSum"))),
                                ImmutableMap.of(),
                                Optional.of(new Symbol("groupid")),
                                AggregationNode.Step.FINAL,
                                project(
                                        ImmutableMap.of("maskPartialSum", expression("IF(expr, partialSum, null)")),
                                        anyTree(
                                                aggregation(
                                                        new GroupingSetDescriptor(ImmutableList.of("partkey$gid", "groupid", "expr"), 2, ImmutableSet.of(0)),
                                                        ImmutableMap.of(Optional.of("partialSum"), functionCall("sum", ImmutableList.of("quantity"))),
                                                        ImmutableMap.of(),
                                                        Optional.of(new Symbol("groupid")),
                                                        AggregationNode.Step.PARTIAL,
                                                        anyTree(
                                                                groupingSet(
                                                                        ImmutableList.of(ImmutableList.of(), ImmutableList.of("partkey")),
                                                                        ImmutableMap.of("quantity", "quantity", "expr", "expr"),
                                                                        "groupid",
                                                                        ImmutableMap.of("partkey$gid", expression("partkey")),
                                                                        project(
                                                                                ImmutableMap.of("expr", expression("orderkey > 0")),
                                                                                tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "partkey", "partkey", "quantity", "quantity")))))))))),
                false);
    }

    @Test
    public void testCalledOnNull()
    {
        assertPlan("SELECT partkey, count(*), count(*) filter (where orderkey > 0) from lineitem group by partkey",
                enableOptimization(),
                anyTree(
                        aggregation(
                                singleGroupingSet("partkey"),
                                ImmutableMap.of(Optional.of("finalCnt"), functionCall("count", ImmutableList.of("partialCnt")), Optional.of("maskFinalCnt"), functionCall("count", ImmutableList.of("maskPartialCnt"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                anyTree(
                                        aggregation(
                                                singleGroupingSet("partkey"),
                                                ImmutableMap.of(Optional.of("partialCnt"), functionCall("count", ImmutableList.of()), Optional.of("maskPartialCnt"), functionCall("count", ImmutableList.of())),
                                                ImmutableMap.of(new Symbol("maskPartialCnt"), new Symbol("expr")),
                                                Optional.empty(),
                                                AggregationNode.Step.PARTIAL,
                                                project(
                                                        ImmutableMap.of("expr", expression("orderkey > 0")),
                                                        tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "partkey", "partkey"))))))),
                false);
    }
}
