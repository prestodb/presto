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

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.RANDOMIZE_OUTER_JOIN_NULL_KEY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestRandomizeNullKeyInOuterJoin
        extends BasePlanTest
{
    private Session enableOptimization()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
    }

    @Test
    public void testLeftJoin()
    {
        assertPlan("SELECT * FROM orders LEFT JOIN lineitem ON orders.orderkey = lineitem.orderkey",
                enableOptimization(),
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("leftRandom", "rightRandom")),
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftCol", expression("leftCol"), "leftRandom", expression("coalesce(cast(leftCol as varchar), 'l' || cast(random(100) as varchar))")),
                                                tableScan("orders", ImmutableMap.of("leftCol", "orderkey")))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("rightCol", expression("rightCol"), "rightRandom", expression("coalesce(cast(rightCol as varchar), 'r' || cast(random(100) as varchar))")),
                                                tableScan("lineitem", ImmutableMap.of("rightCol", "orderkey")))))),
                false);
    }

    @Test
    public void testRightJoin()
    {
        assertPlan("SELECT * FROM orders RIGHT JOIN lineitem ON orders.orderkey = lineitem.orderkey ",
                enableOptimization(),
                anyTree(
                        join(
                                RIGHT,
                                ImmutableList.of(equiJoinClause("leftRandom", "rightRandom")),
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftCol", expression("leftCol"), "leftRandom", expression("coalesce(cast(leftCol as varchar), 'l' || cast(random(100) as varchar))")),
                                                tableScan("orders", ImmutableMap.of("leftCol", "orderkey")))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("rightCol", expression("rightCol"), "rightRandom", expression("coalesce(cast(rightCol as varchar), 'r' || cast(random(100) as varchar))")),
                                                tableScan("lineitem", ImmutableMap.of("rightCol", "orderkey")))))),
                false);
    }

    @Test
    public void testLeftJoinOnSameKey()
    {
        assertPlan("select * from partsupp ps left join part p on ps.partkey = p.partkey left join lineitem l on ps.partkey = l.partkey",
                enableOptimization(),
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("ps_partkey_random", "l_partkey_random")),
                                join(
                                        LEFT,
                                        ImmutableList.of(equiJoinClause("ps_partkey_random", "p_partkey_random")),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("ps_partkey_random", expression("coalesce(cast(ps_partkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                        tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey")))),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("p_partkey_random", expression("coalesce(cast(p_partkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                        tableScan("part", ImmutableMap.of("p_partkey", "partkey"))))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("l_partkey_random", expression("coalesce(cast(l_partkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey")))))),
                false);
    }

    @Test
    public void testLeftJoinOnDifferentKey()
    {
        assertPlan("select * from part p left join lineitem l on p.partkey = l.partkey left join orders o on l.orderkey = o.orderkey",
                enableOptimization(),
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("l_orderkey_random", "o_orderkey_random")),
                                anyTree(
                                        project(ImmutableMap.of("l_orderkey_random", expression("coalesce(cast(l_orderkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                join(
                                                        LEFT,
                                                        ImmutableList.of(equiJoinClause("p_partkey_random", "l_partkey_random")),
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("p_partkey_random", expression("coalesce(cast(p_partkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                                        tableScan("part", ImmutableMap.of("p_partkey", "partkey")))),
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("l_partkey_random", expression("coalesce(cast(l_partkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                                        tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey", "l_orderkey", "orderkey"))))))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("o_orderkey_random", expression("coalesce(cast(o_orderkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey")))))),
                false);
    }

    @Test
    public void testLeftJoinOnMixedKey()
    {
        assertPlan("select * from partsupp ps left join part p on ps.partkey = p.partkey left join lineitem l on ps.partkey = l.partkey left join orders o on l.orderkey = o.orderkey",
                enableOptimization(),
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("l_orderkey_random", "o_orderkey_random")),
                                anyTree(
                                        project(ImmutableMap.of("l_orderkey_random", expression("coalesce(cast(l_orderkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                join(
                                                        LEFT,
                                                        ImmutableList.of(equiJoinClause("ps_partkey_random", "l_partkey_random")),
                                                        join(
                                                                LEFT,
                                                                ImmutableList.of(equiJoinClause("ps_partkey_random", "p_partkey_random")),
                                                                anyTree(
                                                                        project(
                                                                                ImmutableMap.of("ps_partkey_random", expression("coalesce(cast(ps_partkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                                                tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey")))),
                                                                anyTree(
                                                                        project(
                                                                                ImmutableMap.of("p_partkey_random", expression("coalesce(cast(p_partkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                                                tableScan("part", ImmutableMap.of("p_partkey", "partkey"))))),
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("l_partkey_random", expression("coalesce(cast(l_partkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                                        tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey", "l_orderkey", "orderkey"))))))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("o_orderkey_random", expression("coalesce(cast(o_orderkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey")))))),
                false);
    }

    @Test
    public void testCrossJoin()
    {
        assertPlan("SELECT * FROM orders CROSS JOIN lineitem",
                enableOptimization(),
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(),
                                tableScan("orders", ImmutableMap.of("leftCol", "orderkey")),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("rightCol", "orderkey"))))),
                false);
    }

    @Test
    public void testCrossJoinOverLeftJoin()
    {
        assertPlan("select * from partsupp ps left join part p on ps.partkey = p.partkey CROSS JOIN lineitem l",
                enableOptimization(),
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(),
                                join(
                                        LEFT,
                                        ImmutableList.of(equiJoinClause("ps_partkey_random", "p_partkey_random")),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("ps_partkey", expression("ps_partkey"), "ps_partkey_random", expression("coalesce(cast(ps_partkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                        tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey")))),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("p_partkey", expression("p_partkey"), "p_partkey_random", expression("coalesce(cast(p_partkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                        tableScan("part", ImmutableMap.of("p_partkey", "partkey"))))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey"))))),
                false);
    }
}
