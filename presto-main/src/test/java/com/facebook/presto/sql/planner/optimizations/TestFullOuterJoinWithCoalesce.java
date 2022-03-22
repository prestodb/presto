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

import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;

public class TestFullOuterJoinWithCoalesce
        extends BasePlanTest
{
    @Test
    public void testFullOuterJoinWithCoalesce()
    {
        assertDistributedPlan("SELECT coalesce(r.a, ts.a) " +
                        "FROM (" +
                        "   SELECT coalesce(t.a, s.a) AS a " +
                        "   FROM (VALUES (1), (2), (3)) t(a) " +
                        "   FULL OUTER JOIN (VALUES (1), (4)) s(a)" +
                        "   ON t.a = s.a) ts " +
                        "FULL OUTER JOIN (VALUES (2), (5)) r(a) " +
                        "ON ts.a = r.a",
                anyTree(
                        project(
                                ImmutableMap.of("expr", expression("coalesce(r, ts)")),
                                join(
                                        FULL,
                                        ImmutableList.of(equiJoinClause("ts", "r")),
                                        project(
                                                project(
                                                        ImmutableMap.of("ts", expression("coalesce(t, s)")),
                                                        join(
                                                                FULL,
                                                                ImmutableList.of(equiJoinClause("t", "s")),
                                                                exchange(REMOTE_STREAMING, REPARTITION, anyTree(values(ImmutableList.of("t")))),
                                                                exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("s"))))))),
                                        exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("r"))))))));
    }

    @Test
    public void testDuplicateJoinClause()
    {
        assertDistributedPlan("SELECT coalesce(r.a, ts.a) " +
                        "FROM (" +
                        "   SELECT coalesce(t.a, s.a) AS a " +
                        "   FROM (VALUES (1), (2), (3)) t(a) " +
                        "   FULL OUTER JOIN (VALUES (1), (4)) s(a)" +
                        "   ON t.a = s.a AND t.a = s.a) ts " +
                        "FULL OUTER JOIN (VALUES (2), (5)) r(a) " +
                        "ON ts.a = r.a",
                anyTree(
                        project(
                                ImmutableMap.of("expr", expression("coalesce(r, ts)")),
                                join(
                                        FULL,
                                        ImmutableList.of(equiJoinClause("ts", "r")),
                                        project(
                                                project(
                                                        ImmutableMap.of("ts", expression("coalesce(t, s)")),
                                                        join(
                                                                FULL,
                                                                ImmutableList.of(equiJoinClause("t", "s"), equiJoinClause("t", "s")),
                                                                exchange(REMOTE_STREAMING, REPARTITION, anyTree(values(ImmutableList.of("t")))),
                                                                exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("s"))))))),
                                        exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("r"))))))));
    }

    @Test
    public void testDuplicatePartitionColumn()
    {
        assertDistributedPlan("SELECT coalesce(r.a, ts.a), coalesce(ts.b, r.b) " +
                        "FROM (" +
                        "   SELECT coalesce(t.a, s.a) AS a, coalesce(t.a, s.b) AS b" +
                        "   FROM (VALUES (1), (2), (3)) t(a) " +
                        "   FULL OUTER JOIN (VALUES (1, 1), (4, 4)) s(a, b)" +
                        "   ON t.a = s.a AND t.a = s.b) ts " +
                        "FULL OUTER JOIN (VALUES (2, 2), (5, 5)) r(a, b) " +
                        "ON ts.a = r.a and ts.b = r.b",
                anyTree(
                        project(
                                ImmutableMap.of("tsra", expression("coalesce(ra, tsa)"), "tsrb", expression("coalesce(tsb, rb)")),
                                join(
                                        FULL,
                                        ImmutableList.of(equiJoinClause("tsa", "ra"), equiJoinClause("tsb", "rb")),
                                        project(
                                                project(
                                                        ImmutableMap.of("tsa", expression("coalesce(ta, sa)"), "tsb", expression("coalesce(ta, sb)")),
                                                        join(
                                                                FULL,
                                                                ImmutableList.of(equiJoinClause("ta", "sa"), equiJoinClause("ta", "sb")),
                                                                exchange(REMOTE_STREAMING, REPARTITION, anyTree(values(ImmutableList.of("ta")))),
                                                                exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("sa", "sb"))))))),
                                        exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("ra", "rb"))))))));
    }

    @Test
    public void testCoalesceWithManyArgumentsAndGroupBy()
    {
        assertDistributedPlan("SELECT coalesce(t.a, s.a, r.a) " +
                        "FROM (VALUES (1), (2), (3)) t(a) " +
                        "FULL OUTER JOIN (VALUES (1), (4)) s(a) " +
                        "ON t.a = s.a " +
                        "FULL OUTER JOIN (VALUES (2), (5)) r(a) " +
                        "ON t.a = r.a " +
                        "GROUP BY 1",
                anyTree(exchange(
                        REMOTE_STREAMING,
                        REPARTITION,
                        aggregation(
                                ImmutableMap.of(),
                                PARTIAL,
                                project(
                                        project(
                                                ImmutableMap.of("expr", expression("coalesce(t, s, r)")),
                                                join(
                                                        FULL,
                                                        ImmutableList.of(equiJoinClause("t", "r")),
                                                        anyTree(
                                                                join(
                                                                        FULL,
                                                                        ImmutableList.of(equiJoinClause("t", "s")),
                                                                        exchange(REMOTE_STREAMING, REPARTITION, anyTree(values(ImmutableList.of("t")))),
                                                                        exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("s")))))),
                                                        exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("r")))))))))));
    }

    @Test
    public void testCoalesceWithNonSymbolArguments()
    {
        assertDistributedPlan("SELECT coalesce(t.a, s.a + 1, r.a) " +
                        "FROM (VALUES (1), (2), (3)) t(a) " +
                        "FULL OUTER JOIN (VALUES (1), (4)) s(a) " +
                        "ON t.a = s.a " +
                        "FULL OUTER JOIN (VALUES (2), (5)) r(a) " +
                        "ON t.a = r.a " +
                        "GROUP BY 1",
                anyTree(exchange(
                        REMOTE_STREAMING,
                        REPARTITION,
                        aggregation(
                                ImmutableMap.of(),
                                PARTIAL,
                                project(
                                        project(
                                                ImmutableMap.of("expr", expression("coalesce(t, s + 1, r)")),
                                                join(
                                                        FULL,
                                                        ImmutableList.of(equiJoinClause("t", "r")),
                                                        anyTree(
                                                                join(
                                                                        FULL,
                                                                        ImmutableList.of(equiJoinClause("t", "s")),
                                                                        exchange(REMOTE_STREAMING, REPARTITION, anyTree(values(ImmutableList.of("t")))),
                                                                        exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("s")))))),
                                                        exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("r")))))))))));
    }
}
