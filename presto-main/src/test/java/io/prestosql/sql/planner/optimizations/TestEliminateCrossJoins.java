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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.SystemSessionProperties;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;

public class TestEliminateCrossJoins
        extends BasePlanTest
{
    private static final PlanMatchPattern ORDERS_TABLESCAN = tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"));
    private static final PlanMatchPattern ORDERS_WITH_SHIPPRIORITY_TABLESCAN = tableScan(
            "orders",
            ImmutableMap.of("O_ORDERKEY", "orderkey", "O_SHIPPRIORITY", "shippriority"));
    private static final PlanMatchPattern SUPPLIER_TABLESCAN = tableScan("supplier", ImmutableMap.of("S_SUPPKEY", "suppkey"));
    private static final PlanMatchPattern PART_TABLESCAN = tableScan("part", ImmutableMap.of("P_PARTKEY", "partkey"));
    private static final PlanMatchPattern PART_WITH_NAME_TABLESCAN = tableScan("part", ImmutableMap.of("P_PARTKEY", "partkey", "P_NAME", "name"));
    private static final PlanMatchPattern LINEITEM_TABLESCAN = tableScan(
            "lineitem",
            ImmutableMap.of(
                    "L_PARTKEY", "partkey",
                    "L_ORDERKEY", "orderkey"));
    private static final PlanMatchPattern LINEITEM_WITH_RETURNFLAG_TABLESCAN = tableScan(
            "lineitem",
            ImmutableMap.of(
                    "L_PARTKEY", "partkey",
                    "L_ORDERKEY", "orderkey",
                    "L_RETURNFLAG", "returnflag"));
    private static final PlanMatchPattern LINEITEM_WITH_COMMENT_TABLESCAN = tableScan(
            "lineitem",
            ImmutableMap.of(
                    "L_PARTKEY", "partkey",
                    "L_ORDERKEY", "orderkey",
                    "L_COMMENT", "comment"));

    public TestEliminateCrossJoins()
    {
        super(ImmutableMap.of(SystemSessionProperties.JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS"));
    }

    @Test
    public void testEliminateSimpleCrossJoin()
    {
        assertPlan("SELECT * FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("L_ORDERKEY", "O_ORDERKEY")),
                                anyTree(
                                        join(INNER, ImmutableList.of(equiJoinClause("P_PARTKEY", "L_PARTKEY")),
                                                anyTree(PART_TABLESCAN),
                                                anyTree(LINEITEM_TABLESCAN))),
                                anyTree(ORDERS_TABLESCAN))));
    }

    @Test
    public void testGiveUpOnCrossJoin()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("O_ORDERKEY", "L_ORDERKEY")),
                                anyTree(
                                        join(INNER, ImmutableList.of(),
                                                tableScan("part"),
                                                anyTree(tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))),
                                anyTree(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))));
    }

    @Test
    public void testEliminateCrossJoinWithNonEqualityCondition()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l " +
                        "WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND p.partkey <> o.orderkey AND p.name < l.comment",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("L_ORDERKEY", "O_ORDERKEY")),
                                anyTree(
                                        join(INNER, ImmutableList.of(equiJoinClause("P_PARTKEY", "L_PARTKEY")), Optional.of("P_NAME < cast(L_COMMENT AS varchar(55))"),
                                                anyTree(PART_WITH_NAME_TABLESCAN),
                                                anyTree(filter("L_PARTKEY <> L_ORDERKEY", LINEITEM_WITH_COMMENT_TABLESCAN)))),
                                anyTree(ORDERS_TABLESCAN))));
    }

    @Test
    public void testEliminateCrossJoinPreserveFilters()
    {
        assertPlan("SELECT o.orderkey FROM part p, orders o, lineitem l " +
                        "WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND l.returnflag = 'R' AND shippriority >= 10",
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("L_ORDERKEY", "O_ORDERKEY")),
                        anyTree(
                                join(INNER, ImmutableList.of(equiJoinClause("P_PARTKEY", "L_PARTKEY")),
                                        anyTree(PART_TABLESCAN),
                                        anyTree(filter("L_RETURNFLAG = 'R'", LINEITEM_WITH_RETURNFLAG_TABLESCAN)))),
                        anyTree(filter("O_SHIPPRIORITY >= 10", ORDERS_WITH_SHIPPRIORITY_TABLESCAN)))));
    }
}
