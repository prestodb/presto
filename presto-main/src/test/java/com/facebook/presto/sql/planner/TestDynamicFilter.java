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

import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;

public class TestDynamicFilter
        extends BasePlanTest
{
    TestDynamicFilter()
    {
        // in order to test testUncorrelatedSubqueries with Dynamic Filtering, enable it
        super(ImmutableMap.of(ENABLE_DYNAMIC_FILTERING, "true"));
    }

    @Test
    public void testNonInnerJoin()
    {
        assertPlan(
                "SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                project(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(project(tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testEmptyJoinCriteria()
    {
        assertPlan(
                "SELECT o.orderkey FROM orders o CROSS JOIN lineitem l",
                anyTree(
                        join(
                                INNER, ImmutableList.of(),
                                tableScan("orders"),
                                exchange(tableScan("lineitem")))));
    }

    @Test
    public void testJoin()
    {
        assertPlan(
                "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                                Optional.empty(),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testJoinOnCast()
    {
        assertPlan(
                "SELECT o.orderkey FROM orders o, lineitem l WHERE cast(l.orderkey as int) = cast(o.orderkey as int)",
                anyTree(
                        node(
                                JoinNode.class,
                                anyTree(
                                        node(
                                                FilterNode.class,
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testJoinMultipleEquiJoinClauses()
    {
        assertPlan(
                "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey AND l.partkey = o.custkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(
                                        equiJoinClause("ORDERS_OK", "LINEITEM_OK"),
                                        equiJoinClause("ORDERS_CK", "LINEITEM_PK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK", "ORDERS_CK", "LINEITEM_PK"),
                                Optional.empty(),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey")),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey", "LINEITEM_PK", "partkey")))))));
    }

    @Test
    public void testJoinWithOrderBySameKey()
    {
        assertPlan(
                "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey ORDER BY l.orderkey ASC, o.orderkey ASC",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                                Optional.empty(),
                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")),
                                exchange(
                                        project(tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan(
                "SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("X", "Y")),
                                ImmutableMap.of("X", "Y"),
                                Optional.empty(),
                                tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                project(node(EnforceSingleRowNode.class, anyTree(tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan(
                "SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                anyTree(
                        filter(
                                "S",
                                project(
                                        semiJoin(
                                                "X",
                                                "Y",
                                                "S",
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan(
                "SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter(
                                "NOT S",
                                project(
                                        semiJoin(
                                                "X",
                                                "Y",
                                                "S",
                                                anyTree(tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testInnerInequalityJoinWithEquiJoinConjuncts()
    {
        assertPlan(
                "SELECT 1 FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey",
                anyTree(
                        anyNot(
                                FilterNode.class,
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("O_SHIPPRIORITY", "L_LINENUMBER")),
                                        Optional.of("O_ORDERKEY < L_ORDERKEY"),
                                        anyTree(tableScan("orders", ImmutableMap.of(
                                                "O_SHIPPRIORITY", "shippriority",
                                                "O_ORDERKEY", "orderkey"))),
                                        anyTree(tableScan("lineitem", ImmutableMap.of(
                                                "L_LINENUMBER", "linenumber",
                                                "L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testSubTreeJoinDFOnProbeSide()
    {
        assertPlan(
                "SELECT part.partkey from part JOIN (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) ON part.partkey = lineitem.orderkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("PART_PK", "LINEITEM_OK")),
                                ImmutableMap.of("PART_PK", "LINEITEM_OK"),
                                Optional.empty(),
                                tableScan("part", ImmutableMap.of("PART_PK", "partkey")),
                                anyTree(
                                        join(
                                                INNER,
                                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                                ImmutableMap.of("LINEITEM_OK", "ORDERS_OK"),
                                                Optional.empty(),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")),
                                                exchange(project(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))))));
    }

    @Test
    public void testSubTreeJoinDFOnBuildSide()
    {
        assertPlan(
                "SELECT part.partkey from (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) JOIN part ON lineitem.orderkey = part.partkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "PART_PK")),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                        anyTree(node(FilterNode.class, tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))),
                                        anyTree(node(FilterNode.class, tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))),
                                exchange(
                                        project(tableScan("part", ImmutableMap.of("PART_PK", "partkey")))))));
    }
}
