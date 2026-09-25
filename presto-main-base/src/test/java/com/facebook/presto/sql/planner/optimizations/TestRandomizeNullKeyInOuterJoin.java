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

import java.util.Optional;

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
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
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
                                ImmutableList.of(equiJoinClause("leftRandom", "rightCol")),
                                Optional.of("NOT (leftCol IS NULL)"),
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftCol", expression("leftCol"), "leftRandom", expression("coalesce(leftCol, -random(100))")),
                                                tableScan("orders", ImmutableMap.of("leftCol", "orderkey")))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("rightCol", "orderkey"))))),
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
                                ImmutableList.of(equiJoinClause("leftRandom", "rightRandom")),
                                Optional.of("NOT (leftCol IS NULL)"),
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftCol", expression("leftCol"), "leftRandom", expression("coalesce(leftCol, cast(random(100) as varchar))")),
                                                values("leftCol"))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("rightRandom", expression("cast(rightCol as varchar)")),
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
                                        equiJoinClause("leftBigIntRandom", "rightColBigInt")),
                                Optional.of("(NOT (leftColVarchar IS NULL)) AND (NOT (leftColBigInt IS NULL))"),
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftVarcharRandom", expression("coalesce(leftColVarchar, cast(random(100) as varchar))"),
                                                        "leftBigIntRandom", expression("coalesce(leftColBigInt, -random(100))")),
                                                values("leftColVarchar", "leftColBigInt"))),
                                anyTree(
                                        project(
                                                ImmutableMap.of("rightVarcharRandom", expression("cast(rightColVarchar as varchar)")),
                                                values("rightColVarchar", "rightColBigInt"))))),
                false);
    }

    @Test
    public void testRightJoin()
    {
        // For a RIGHT join the left (non-preserved) side's `leftCol IS NOT NULL` guard is pushed below the
        // join into a filter over the left scan, so the join itself carries no extra filter.
        assertPlan("SELECT * FROM orders RIGHT JOIN lineitem ON orders.orderkey = lineitem.orderkey ",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                RIGHT,
                                ImmutableList.of(equiJoinClause("leftRandom", "rightCol")),
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftRandom", expression("coalesce(leftCol, -random(100))")),
                                                filter(
                                                        "NOT (leftCol IS NULL)",
                                                        tableScan("orders", ImmutableMap.of("leftCol", "orderkey"))))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("rightCol", "orderkey"))))),
                false);
    }

    @Test
    public void testLeftJoinOnSameKey()
    {
        // Same left key (ps.partkey) feeds both joins, so both reuse the one randomized variable and only the
        // right keys (p.partkey, l.partkey) stay raw. Each join is guarded by `ps.partkey IS NOT NULL`.
        assertPlan("select * from partsupp ps left join part p on ps.partkey = p.partkey left join lineitem l on ps.partkey = l.partkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("ps_partkey_random", "l_partkey")),
                                Optional.of("NOT (ps_partkey IS NULL)"),
                                join(
                                        LEFT,
                                        ImmutableList.of(equiJoinClause("ps_partkey_random", "p_partkey")),
                                        Optional.of("NOT (ps_partkey IS NULL)"),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("ps_partkey_random", expression("coalesce(ps_partkey, -random(100))")),
                                                        tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey")))),
                                        anyTree(
                                                tableScan("part", ImmutableMap.of("p_partkey", "partkey")))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey"))))),
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
                                ImmutableList.of(equiJoinClause("ps_partkey_random", "l_partkey")),
                                Optional.of("NOT (ps_partkey IS NULL)"),
                                anyTree(
                                        project(
                                                ImmutableMap.of("ps_partkey_random", expression("coalesce(ps_partkey, -random(100))")),
                                                tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey")))),
                                anyTree(
                                        join(
                                                LEFT,
                                                ImmutableList.of(equiJoinClause("p_partkey_random", "l_partkey")),
                                                Optional.of("NOT (p_partkey IS NULL)"),
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("p_partkey_random", expression("coalesce(p_partkey, -random(100))")),
                                                                tableScan("part", ImmutableMap.of("p_partkey", "partkey", "name", "name")))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey", "orderkey", "orderkey"))))))),
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
                                ImmutableList.of(equiJoinClause("l_orderkey_random", "o_orderkey")),
                                Optional.of("NOT (l_orderkey IS NULL)"),
                                anyTree(
                                        project(ImmutableMap.of("l_orderkey_random", expression("coalesce(l_orderkey, -random(100))")),
                                                join(
                                                        LEFT,
                                                        ImmutableList.of(equiJoinClause("p_partkey_random", "l_partkey")),
                                                        Optional.of("NOT (p_partkey IS NULL)"),
                                                        anyTree(
                                                                project(
                                                                        ImmutableMap.of("p_partkey_random", expression("coalesce(p_partkey, -random(100))")),
                                                                        tableScan("part", ImmutableMap.of("p_partkey", "partkey")))),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey", "l_orderkey", "orderkey")))))),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey"))))),
                false);
    }

    @Test
    public void testLeftJoinOnMixedKey()
    {
        assertPlan("select * from partsupp ps left join part p on ps.partkey = p.partkey left join lineitem l on ps.partkey = l.partkey left join orders o on l.orderkey = o.orderkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("l_orderkey_random", "o_orderkey")),
                                Optional.of("NOT (l_orderkey IS NULL)"),
                                anyTree(
                                        project(ImmutableMap.of("l_orderkey_random", expression("coalesce(l_orderkey, -random(100))")),
                                                join(
                                                        LEFT,
                                                        ImmutableList.of(equiJoinClause("ps_partkey_random", "l_partkey")),
                                                        Optional.of("NOT (ps_partkey IS NULL)"),
                                                        join(
                                                                LEFT,
                                                                ImmutableList.of(equiJoinClause("ps_partkey_random", "p_partkey")),
                                                                Optional.of("NOT (ps_partkey IS NULL)"),
                                                                anyTree(
                                                                        project(
                                                                                ImmutableMap.of("ps_partkey_random", expression("coalesce(ps_partkey, -random(100))")),
                                                                                tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey")))),
                                                                anyTree(
                                                                        tableScan("part", ImmutableMap.of("p_partkey", "partkey")))),
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey", "l_orderkey", "orderkey")))))),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey"))))),
                false);
    }

    @Test
    public void testJoinKeyFromOuterJoin()
    {
        assertPlan("select * from partsupp ps left join part p on ps.partkey = p.partkey left join lineitem l on ps.partkey = l.partkey left join orders o on l.orderkey = o.orderkey",
                getSessionEnabledWhenJoinKeyFromOuterJoin(),
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("l_orderkey_random", "o_orderkey")),
                                Optional.of("NOT (l_orderkey IS NULL)"),
                                anyTree(
                                        project(ImmutableMap.of("l_orderkey_random", expression("coalesce(l_orderkey, -random(100))")),
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
                                        tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey"))))),
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
                                        ImmutableList.of(equiJoinClause("ps_partkey_random", "p_partkey")),
                                        Optional.of("NOT (ps_partkey IS NULL)"),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("ps_partkey", expression("ps_partkey"), "ps_partkey_random", expression("coalesce(ps_partkey, -random(100))")),
                                                        tableScan("partsupp", ImmutableMap.of("ps_partkey", "partkey")))),
                                        anyTree(
                                                tableScan("part", ImmutableMap.of("p_partkey", "partkey")))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("l_partkey", "partkey"))))),
                false);
    }
}
