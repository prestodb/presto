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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.EXPLOIT_CONSTRAINTS;
import static com.facebook.presto.SystemSessionProperties.IN_PREDICATES_AS_INNER_JOINS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestEarlyOutJoins
        extends BasePlanTest
{
    private ImmutableMap<String, String> nationColumns = ImmutableMap.<String, String>builder()
            .put("regionkey", "regionkey")
            .put("nationkey", "nationkey")
            .put("name", "name")
            .put("comment", "comment")
            .build();

    private ImmutableMap<String, String> orderColumns = ImmutableMap.<String, String>builder()
            .put("orderpriority", "orderpriority")
            .put("orderstatus", "orderstatus")
            .put("totalprice", "totalprice")
            .put("orderkey", "orderkey")
            .put("custkey", "custkey")
            .put("orderdate", "orderdate")
            .put("comment", "comment")
            .put("shippriority", "shippriority")
            .put("clerk", "clerk")
            .build();

    public TestEarlyOutJoins()
    {
        super(ImmutableMap.of(EXPLOIT_CONSTRAINTS, Boolean.toString(true),
                IN_PREDICATES_AS_INNER_JOINS_ENABLED, Boolean.toString(true),
                JOIN_REORDERING_STRATEGY, AUTOMATIC.name()));
    }

    @Test
    public void testDistinctInnerRewrite()
    {
        // Rewrite to distinct + inner join and then reorder the join (orders >> nation)

        String query = "select * from nation where nationkey in (select custkey from orders)";
        assertPlan(query,
                output(
                        project(
                                aggregation(ImmutableMap.of(),
                                        anyTree(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("custkey", "nationkey")),
                                                        anyTree(
                                                                tableScan("orders", ImmutableMap.of("custkey", "custkey"))),
                                                        anyTree(
                                                                assignUniqueId(
                                                                        "unique",
                                                                        tableScan("nation", nationColumns)))))))));
    }

    @Test
    public void testDistinctInnerToLeftEarlyOutRewrite()
    {
        // Rewrite inner join to semi join
        String query = "select distinct o.custkey, o.totalprice from orders o, nation n where o.custkey = n.nationkey";
        assertPlan(query,
                output(
                        anyTree(
                                semiJoin("custkey", "nationkey", "semijoinvariable$eoj",
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("custkey", "custkey", "totalprice", "totalprice"))),
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))));

        // Negative test - too many columns read from nation
        query = "select distinct o.custkey, o.totalprice, n.nationkey, n.name from orders o, nation n where o.custkey = n.nationkey";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("custkey", "nationkey")),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("custkey", "custkey", "totalprice", "totalprice"))),
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey", "name", "name")))))));

        // Trasnform to distinct + inner join and then transform back to semi join
        // The join inputs were not reordered and the join was cardinality reducing
        query = "select * from orders where custkey in (select custkey from customer where name = 'Customer#000156251')";
        assertPlan(query,
                output(
                        project(
                                anyTree(
                                        semiJoin("custkey", "custkey_1", "semijoinvariable$eoj",
                                                anyTree(
                                                        tableScan("orders", orderColumns)),
                                                anyTree(
                                                        tableScan("customer", ImmutableMap.of("custkey_1", "custkey", "name", "name"))))))));
    }

    @Test
    public void testDistinctInnerToRightEarlyOutRewrite()
    {
        // Rewrite to distinct + inner join and then push aggregation below the left input
        // The join inputs were reordered and the join was not cardinality reducing
        String query = "select orderkey from orders where orderkey in (select orderkey from lineitem)";
        assertPlan(query,
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("orderkey_1", "orderkey")),
                                project(
                                        aggregation(ImmutableMap.of(),
                                                tableScan("lineitem", ImmutableMap.of("orderkey_1", "orderkey")))),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))));

        Session sessionWithIncreasedByteReductionThreshold = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD, Double.toString(2))
                .build();

        // Same as previous query, except the aggregation is not pushed below the join since the join is not
        // considered cardinality reducing due to the altered value for parameter EARLY_OUT_JOIN_TRANSFORMATION_BYTE_REDUCTION_THRESHOLD
        assertPlanWithSession(query,
                sessionWithIncreasedByteReductionThreshold,
                false,
                output(
                        anyTree(
                                aggregation(ImmutableMap.of(),
                                        anyTree(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("orderkey_1", "orderkey")),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("orderkey_1", "orderkey"))),
                                                        anyTree(
                                                                assignUniqueId("unique",
                                                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))))))));

        // Aggregation pushed down the left input of the join, but not the right since the output contains o.custkey (not a join key in the output)
        query = "select distinct l.orderkey, l.partkey, o.custkey from lineitem l, orders o where l.orderkey = o.orderkey";
        assertPlan(query,
                output(
                        anyTree(
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("orderkey", "orderkey_0")),
                                        anyTree(
                                                aggregation(ImmutableMap.of(),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("partkey", "partkey", "orderkey", "orderkey"))))),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("custkey", "custkey", "orderkey_0", "orderkey")))))));
    }

    @Test
    public void testAntiJoinScenarios()
    {
        // No NOT's or OR's or CASE's

        String query = "select * from nation where nationkey not in (select custkey from orders)";
        assertPlan(query,
                output(
                        anyTree(
                                semiJoin("nationkey", "custkey", "expr_9",
                                        anyTree(
                                                tableScan("nation", nationColumns)),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("custkey", "custkey")))))));

        query = "select * from nation where nationkey in (select custkey from orders) or nationkey in (select orderkey from lineitem)";
        assertPlan(query,
                output(
                        anyTree(
                                semiJoin("nationkey", "orderkey_11", "expr_21",
                                        semiJoin("nationkey", "custkey", "expr_9",
                                                anyTree(
                                                        tableScan("nation", nationColumns)),
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("custkey", "custkey")))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("orderkey_11", "orderkey")))))));

        query = "select * from nation where not (nationkey = any (select custkey from orders))";
        assertPlan(query,
                output(
                        anyTree(
                                semiJoin("nationkey", "custkey", "expr_9",
                                        anyTree(
                                                tableScan("nation", nationColumns)),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("custkey", "custkey")))))));

        query = "select case when (nationkey in (select custkey from orders)) then 1 else 2 end from nation";
        assertPlan(query,
                output(
                        anyTree(
                                semiJoin("nationkey", "custkey", "expr_9",
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("custkey", "custkey")))))));
    }

    @Test
    void testCombinationsOfSubqueries()
    {
        // Rewrite to distinct + inner join for both subqueries and then find optimal order for the 3-way join
        String query = "select * from nation where nationkey in (select custkey from orders) and nationkey in (select orderkey from lineitem)";
        assertPlan(query,
                output(
                        anyTree(
                                aggregation(ImmutableMap.of(),
                                        anyTree(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("orderkey_9", "nationkey")),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("orderkey_9", "orderkey"))),
                                                        anyTree(
                                                                aggregation(ImmutableMap.of(),
                                                                        project(
                                                                                join(INNER,
                                                                                        ImmutableList.of(equiJoinClause("custkey", "nationkey")),
                                                                                        anyTree(
                                                                                                tableScan("orders", ImmutableMap.of("custkey", "custkey"))),
                                                                                        anyTree(assignUniqueId("unique_34",
                                                                                                tableScan("nation", nationColumns)))))))))))));

        // Disjoint conjunctions also transformed similarly
        query = "select * from nation where nationkey in (select custkey from orders) and regionkey in (select orderkey from lineitem)";
        assertPlan(query,
                output(
                        anyTree(
                                aggregation(ImmutableMap.of(),
                                        project(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("orderkey_9", "regionkey")),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("orderkey_9", "orderkey"))),
                                                        assignUniqueId("unique",
                                                                project(
                                                                        aggregation(ImmutableMap.of(),
                                                                                anyTree(
                                                                                        join(INNER,
                                                                                                ImmutableList.of(equiJoinClause("custkey", "nationkey")),
                                                                                                anyTree(
                                                                                                        tableScan("orders", ImmutableMap.of("custkey", "custkey"))),
                                                                                                anyTree(
                                                                                                        assignUniqueId("unique_34",
                                                                                                                tableScan("nation", nationColumns))))))))))))));
    }

    @Test
    void testComplexQueries()
    {
        // Subquery produces distinct output -> is rewritten to inner join, inputs are reordered and extraneous aggregations are removed
        String query = "select * from nation where nationkey in (select custkey from orders group by custkey)";
        assertPlan(query,
                output(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("custkey", "nationkey")),
                                anyTree(
                                        aggregation(ImmutableMap.of(),
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("custkey", "custkey"))))),
                                anyTree(
                                        tableScan("nation", nationColumns)))));

        // In predicates in the having clause
        query = "select nationkey, name from nation having nationkey in (select custkey from orders)";
        assertPlan(query,
                output(
                        project(
                                aggregation(ImmutableMap.of(),
                                        anyTree(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("custkey", "nationkey")),
                                                        anyTree(
                                                                tableScan("orders", ImmutableMap.of("custkey", "custkey"))),
                                                        anyTree(
                                                                assignUniqueId("unique",
                                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey", "name", "name"))))))))));

        query = "select nationkey, name from nation having nationkey in (select custkey from orders) and nationkey in (select orderkey from lineitem)";
        assertPlan(query,
                output(
                        project(
                                aggregation(ImmutableMap.of(),
                                        project(
                                                join(INNER,
                                                        ImmutableList.of(equiJoinClause("orderkey_9", "nationkey")),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("orderkey_9", "orderkey"))),
                                                        assignUniqueId("unique",
                                                                anyTree(
                                                                        aggregation(ImmutableMap.of(),
                                                                                project(
                                                                                        join(INNER,
                                                                                                ImmutableList.of(equiJoinClause("custkey", "nationkey")),
                                                                                                anyTree(
                                                                                                        tableScan("orders", ImmutableMap.of("custkey", "custkey"))),
                                                                                                anyTree(
                                                                                                        assignUniqueId("unique_26",
                                                                                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey", "name", "name")))))))))))))));

        query = "select nationkey, name from nation having nationkey in (select custkey from orders) or nationkey in (select orderkey from lineitem)";
        assertPlan(query,
                output(
                        anyTree(
                                semiJoin("nationkey", "orderkey_9", "expr_17",
                                        semiJoin("nationkey", "custkey", "expr_7",
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("name", "name", "nationkey", "nationkey"))),
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("custkey", "custkey")))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("orderkey_9", "orderkey")))))));
    }
}
