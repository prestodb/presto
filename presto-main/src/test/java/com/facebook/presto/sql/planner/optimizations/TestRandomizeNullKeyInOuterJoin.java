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
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.SystemSessionProperties.RANDOMIZE_OUTER_JOIN_NULL_KEY;
import static com.facebook.presto.SystemSessionProperties.RANDOMIZE_OUTER_JOIN_NULL_KEY_STRATEGY;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRandomizeNullKeyInOuterJoin
        extends BasePlanTest
{
    private Session getSessionAlwaysEnabled()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
    }

    private Session getSessionEnabledWhenJoinKeyFromOuterJoin()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY, "false")
                .setSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY_STRATEGY, "key_from_outer_join")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
    }

    @Test
    public void testLeftJoin()
    {
        assertPlan("SELECT * FROM orders LEFT JOIN lineitem ON orders.orderkey = lineitem.orderkey",
                getSessionAlwaysEnabled(),
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
    public void testLeftJoinVarchar()
    {
        assertPlan("SELECT * FROM (values '3') t1(k) LEFT JOIN (values '2', '3')t2(k) ON t1.k = t2.k",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("leftRandom", "rightRandom"), equiJoinClause("leftIsNull", "rightIsNull")),
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftCol", expression("leftCol"), "leftRandom", expression("coalesce(leftCol, 'l' || cast(random(100) as varchar))"), "leftIsNull", expression("leftCol is NULL")),
                                                values("leftCol"))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("rightCol", expression("rightCol"), "rightRandom", expression("coalesce(rightCol, 'r' || cast(random(100) as varchar))"), "rightIsNull", expression("rightCol is NULL")),
                                                values("rightCol"))))),
                false);
    }

    @Test
    public void testMixedVarcharAndIntKeys()
    {
        assertPlan("SELECT * FROM (values ('3', cast(0 as bigint))) t1(k1, k2) LEFT JOIN (values ('2', cast(1 as bigint)), ('3', 1))t2(k1, k2) ON t1.k1 = t2.k1 and t1.k2 = t2.k2",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("leftVarcharRandom", "rightVarcharRandom"),
                                        equiJoinClause("leftVarcharIsNull", "rightVarcharIsNull"),
                                        equiJoinClause("leftBigIntRandom", "rightBigIntRandom")),
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftVarcharRandom", expression("coalesce(leftColVarchar, 'l' || cast(random(100) as varchar))"),
                                                        "leftVarcharIsNull", expression("leftColVarchar is NULL"),
                                                        "leftBigIntRandom", expression("coalesce(cast(leftColBigInt as varchar), 'l' || cast(random(100) as varchar))")),
                                                values("leftColVarchar", "leftColBigInt"))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("rightVarcharRandom", expression("coalesce(rightColVarchar, 'r' || cast(random(100) as varchar))"),
                                                        "rightVarcharIsNull", expression("rightColVarchar is NULL"),
                                                        "rightBigIntRandom", expression("coalesce(cast(rightColBigInt as varchar), 'r' || cast(random(100) as varchar))")),
                                                values("rightColVarchar", "rightColBigInt"))))),
                false);
    }

    @Test
    public void testRightJoin()
    {
        assertPlan("SELECT * FROM orders RIGHT JOIN lineitem ON orders.orderkey = lineitem.orderkey ",
                getSessionAlwaysEnabled(),
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
                getSessionAlwaysEnabled(),
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
    public void testLeftJoinOnSameKeyJoinAsRightSideInput()
    {
        assertPlan("select * from partsupp ps left join (select p.name, l.orderkey, l.partkey as partkey from part p left join lineitem l on p.partkey = l.partkey) pl on ps.partkey = pl.partkey",
                Session.builder(getSessionAlwaysEnabled()).setSystemProperty(OPTIMIZE_HASH_GENERATION, "false").build(),
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("ps_partkey_random", "l_partkey_random")),
                                anyTree(
                                        project(
                                                ImmutableMap.of("ps_partkey_random", expression("coalesce(cast(ps_partkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey")))),
                                anyTree(
                                        join(
                                                LEFT,
                                                ImmutableList.of(equiJoinClause("p_partkey_random", "l_partkey_random")),
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("p_partkey_random", expression("coalesce(cast(p_partkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                                tableScan("part", ImmutableMap.of("p_partkey", "partkey", "name", "name")))),
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("l_partkey_random", expression("coalesce(cast(l_partkey as varchar), 'r' || cast(random(100) as varchar))")),
                                                                tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey", "orderkey", "orderkey")))))))),
                false);
    }

    @Test
    public void testLeftJoinOnDifferentKey()
    {
        assertPlan("select * from part p left join lineitem l on p.partkey = l.partkey left join orders o on l.orderkey = o.orderkey",
                getSessionAlwaysEnabled(),
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
                getSessionAlwaysEnabled(),
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
    public void testJoinKeyFromOuterJoin()
    {
        assertPlan("select * from partsupp ps left join part p on ps.partkey = p.partkey left join lineitem l on ps.partkey = l.partkey left join orders o on l.orderkey = o.orderkey",
                getSessionEnabledWhenJoinKeyFromOuterJoin(),
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("l_orderkey_random", "o_orderkey_random")),
                                anyTree(
                                        project(ImmutableMap.of("l_orderkey_random", expression("coalesce(cast(l_orderkey as varchar), 'l' || cast(random(100) as varchar))")),
                                                join(
                                                        LEFT,
                                                        ImmutableList.of(equiJoinClause("ps_partkey", "l_partkey")),
                                                        join(
                                                                LEFT,
                                                                ImmutableList.of(equiJoinClause("ps_partkey", "p_partkey")),
                                                                anyTree(
                                                                        tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey"))),
                                                                anyTree(
                                                                        tableScan("part", ImmutableMap.of("p_partkey", "partkey")))),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey", "l_orderkey", "orderkey")))))),
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
                getSessionAlwaysEnabled(),
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
                getSessionAlwaysEnabled(),
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
