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

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_PLAN_WITH_EMPTY_INPUT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestSimplifyPlanWithEmptyInput
        extends BasePlanTest
{
    private Session enableOptimization()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "true")
                .build();
    }

    private Session enableCteMaterialization()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();
    }

    @Test
    public void testInnerJoinWithEmptyInput()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from orders o join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        values("orderkey", "custkey", "linenumber")));
    }

    @Test
    public void testCrossJoinWithEmptyInput()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from orders o cross join (select orderkey, linenumber from lineitem where false) l",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        values("orderkey", "custkey", "linenumber")));
    }

    @Test
    public void testLeftJoinWithEmptyBuild()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from orders o left join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        project(
                                ImmutableMap.of("orderkey", expression("orderkey"), "custkey", expression("custkey"), "linenumber", expression("null")),
                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))));
    }

    @Test
    public void testLeftJoinWithEmptyProbe()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from (select orderkey, linenumber from lineitem where false) l left join orders o on l.orderkey = o.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        values("linenumber", "orderkey", "custkey")));
    }

    @Test
    public void testRightJoinWithEmptyBuild()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from orders o right join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        values("orderkey", "custkey", "linenumber")));
    }

    @Test
    public void testRightJoinWithEmptyProbe()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from (select orderkey, linenumber from lineitem where false) l right join orders o on l.orderkey = o.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        project(
                                ImmutableMap.of("orderkey", expression("orderkey"), "custkey", expression("custkey"), "linenumber", expression("null")),
                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))));
    }

    @Test
    public void testFullJoinWithEmptyProbe()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from (select orderkey, linenumber from lineitem where false) l full outer join orders o on l.orderkey = o.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        project(
                                ImmutableMap.of("orderkey", expression("orderkey"), "custkey", expression("custkey"), "linenumber", expression("null")),
                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))));
    }

    @Test
    public void testFullJoinWithEmptyBuild()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from orders o full outer join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        project(
                                ImmutableMap.of("orderkey", expression("orderkey"), "custkey", expression("custkey"), "linenumber", expression("null")),
                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))));
    }

    @Test
    public void testUnionWithEmptyInput()
    {
        assertPlan("select orderkey, partkey from lineitem union all select orderkey, custkey as partkey from orders where false",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "partkey"),
                        tableScan("lineitem", ImmutableMap.of("partkey", "partkey", "orderkey", "orderkey"))));
    }

    @Test
    public void testUnionMultipleNonEmptyInput()
    {
        assertPlan("select orderkey, partkey from lineitem union all select orderkey, custkey as partkey from orders where false union all select custkey, nationkey from customer",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "partkey"),
                        exchange(
                                tableScan("lineitem", ImmutableMap.of("partkey", "partkey", "orderkey", "orderkey")),
                                tableScan("customer", ImmutableMap.of("nationkey", "nationkey", "custkey", "custkey")))));
    }

    @Test
    public void testSemiJoinEmptyFilterSource()
    {
        assertPlan("select orderkey, partkey from lineitem where orderkey in (select orderkey from orders where false)",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "partkey"),
                        filter(
                                "false",
                                tableScan("lineitem", ImmutableMap.of("orderkey", "orderkey", "partkey", "partkey")))));
    }

    @Test
    public void testSemiJoinEmptySource()
    {
        assertPlan("select orderkey, partkey from (select orderkey, partkey from lineitem where false) where orderkey in (select orderkey from orders)",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "partkey"),
                        values("orderkey", "partkey")));
    }

    @Test
    public void testAggregationWithDefaultOutput()
    {
        assertPlan("select count(*) as count from (select orderkey from orders where false)",
                enableOptimization(),
                output(
                        ImmutableList.of("count"),
                        aggregation(
                                ImmutableMap.of("count", functionCall("count", ImmutableList.of())),
                                values())));
    }

    @Test
    public void testAggregationNoDefaultOutput()
    {
        assertPlan("select orderkey, count(*) as count from (select orderkey from orders where false) group by orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "count"),
                        values("orderkey", "count")));
    }

    @Test
    public void testInnerJoinWithOverEmptyAggregation()
    {
        assertPlan("select o.orderkey, o.custkey, l.linenumber from orders o join (select orderkey, max(linenumber) as linenumber from lineitem where false group by orderkey) l on o.orderkey = l.orderkey",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        values("orderkey", "custkey", "linenumber")));
    }

    @Test
    public void testQueryWithWindowFilterLimitOrderby()
    {
        assertPlan("with emptyorders as (select orderkey, totalprice, orderdate from orders where false) SELECT orderkey, orderdate, totalprice, " +
                        "ROW_NUMBER() OVER (ORDER BY orderdate) as row_num FROM emptyorders WHERE totalprice > 10 ORDER BY orderdate ASC LIMIT 10",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "orderdate", "totalprice", "row_number"),
                        values("orderkey", "totalprice", "orderdate", "row_number")));
    }

    @Test
    public void testQueryWithJoinWindowFilterLimitOrderby()
    {
        assertPlan("with emptyorders as (select * from orders where false) SELECT c.custkey, c.name, c.acctbal, SUM(l.quantity) OVER (PARTITION BY c.custkey) AS total_quantity " +
                        "FROM customer c JOIN emptyorders o ON c.custkey = o.custkey JOIN lineitem l ON o.orderkey = l.orderkey WHERE o.orderdate BETWEEN DATE '1995-03-01' AND " +
                        "DATE '1995-03-31' AND l.shipdate BETWEEN DATE '1995-03-01' AND DATE '1995-03-31' ORDER BY total_quantity DESC, c.custkey LIMIT 100",
                enableOptimization(),
                output(
                        ImmutableList.of("custkey", "name", "acctbal", "sum"),
                        values("custkey", "name", "acctbal", "sum")));
    }

    @Test
    public void testRowNumberWithEmptyInput()
    {
        assertPlan("select orderkey, row_number() over (partition by orderpriority), orderpriority from (select orderkey, orderpriority from orders where false)",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "rownumber", "orderpriority"),
                        values(ImmutableList.of("orderkey", "orderpriority", "rownumber"), ImmutableList.of())));
    }

    @Test
    public void testTopNRowNumberWithEmptyInput()
    {
        assertPlan("select * from (select orderkey, row_number() over (partition by orderpriority order by orderkey) row_number, orderpriority from (select orderkey, orderpriority from orders where false)) where row_number < 2",
                enableOptimization(),
                output(
                        ImmutableList.of("orderkey", "row_number", "orderpriority"),
                        values(ImmutableList.of("orderpriority", "orderkey", "row_number"), ImmutableList.of())));
    }

    @Test
    public void testCteMaterializationQueryWithJoinWindowFilterLimitOrderby()
    {
        assertPlan("with emptyorders as (select * from orders where false) SELECT c.custkey, c.name, c.acctbal, SUM(l.quantity) OVER (PARTITION BY c.custkey) AS total_quantity " +
                        "FROM customer c JOIN emptyorders o ON c.custkey = o.custkey JOIN lineitem l ON o.orderkey = l.orderkey WHERE o.orderdate BETWEEN DATE '1995-03-01' AND " +
                        "DATE '1995-03-31' AND l.shipdate BETWEEN DATE '1995-03-01' AND DATE '1995-03-31' ORDER BY total_quantity DESC, c.custkey LIMIT 100",
                enableCteMaterialization(),
                output(
                        ImmutableList.of("custkey", "name", "acctbal", "sum"),
                        values("sum", "custkey", "name", "acctbal")));
    }

    @Test
    public void testCteMaterializationQueryCrossJoinWithEmptyInput()
    {
        assertPlan("WITH t as (select o.orderkey, o.custkey, l.linenumber from orders o cross join (select orderkey, linenumber from lineitem where false) l) SELECT * FROM t",
                enableCteMaterialization(),
                output(
                        ImmutableList.of("orderkey", "custkey", "linenumber"),
                        values("orderkey", "custkey", "linenumber")));
    }

    @Test
    public void testCteMaterializationQueryWithWindowFilterLimitOrderby()
    {
        assertPlan("with emptyorders as (select orderkey, totalprice, orderdate from orders where false) SELECT orderkey, orderdate, totalprice, " +
                        "ROW_NUMBER() OVER (ORDER BY orderdate) as row_num FROM emptyorders WHERE totalprice > 10 ORDER BY orderdate ASC LIMIT 10",
                enableCteMaterialization(),
                output(
                        ImmutableList.of("orderkey", "orderdate", "totalprice", "row_number"),
                        values("orderkey", "totalprice", "orderdate", "row_number")));
    }

    @Test
    public void testNestedCteMaterializationQueryWithEmptyInput()
    {
        assertPlan("WITH t as(select orderkey, count(*) as count from (select orderkey from orders where false) group by orderkey)," +
                        " b AS (SELECT * FROM t) " +
                        "SELECT * FROM b",
                enableCteMaterialization(),
                output(
                        ImmutableList.of("orderkey", "count"),
                        values("orderkey", "count")));
    }
}
