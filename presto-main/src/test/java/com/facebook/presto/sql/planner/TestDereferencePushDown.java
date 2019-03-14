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

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.Ordering;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

public class TestDereferencePushDown
        extends BasePlanTest
{
    @Test
    public void testDereferencePushdownJoin()
    {
        assertPlan("WITH t(msg) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT b.msg.x FROM t a, t b WHERE a.msg.y = b.msg.y",
                output(ImmutableList.of("b_x"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y")),
                                                values("msg"))
                                ), anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                values("msg"))))));

        assertPlan("WITH t(msg) AS ( SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT a.msg.y FROM t a JOIN t b ON a.msg.y = b.msg.y WHERE a.msg.x > bigint '5'",
                output(ImmutableList.of("a_y"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y")),
                                                filter("msg.x > bigint '5'",
                                                        values("msg")))
                                ), anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y")),
                                                values("msg"))))));

        assertPlan("WITH t(msg) AS ( SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT b.msg.x FROM t a JOIN t b ON a.msg.y = b.msg.y WHERE a.msg.x + b.msg.x < bigint '10'",
                output(ImmutableList.of("b_x"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                Optional.of("a_x + b_x < bigint '10'"),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x")),
                                                        values("msg"))
                                ), anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                values("msg"))))));
    }

    @Test
    public void testDereferencePushdownSort()
    {
        ImmutableList<Ordering> orderBy = ImmutableList.of(sort("b_x", ASCENDING, LAST));
        assertPlan("WITH t(msg) AS ( SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT a.msg.x FROM t a JOIN t b ON a.msg.y = b.msg.y WHERE a.msg.x < bigint '10' ORDER BY b.msg.x",
                output(ImmutableList.of("expr"),
                        project(ImmutableMap.of("expr", expression("a_x")),
                            exchange(LOCAL, GATHER, orderBy,
                                    sort(orderBy,
                                            exchange(LOCAL, REPARTITION,
                                                    join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                                            anyTree(
                                                                    project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x")),
                                                                            filter("msg.x < bigint '10'",
                                                                                    values("msg")))
                                                            ), anyTree(
                                                                    project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                                            values("msg"))))))))));
    }

    @Test
    public void testDereferencePushdownUnnest()
    {
        assertPlan("WITH t(msg, array) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)), ARRAY[1, 2, 3]))) " +
                    "SELECT a.msg.x FROM t a JOIN t b ON a.msg.y = b.msg.y CROSS JOIN UNNEST (a.array) WHERE a.msg.x + b.msg.x < bigint '10'",
                output(ImmutableList.of("expr"),
                        project(ImmutableMap.of("expr", expression("a_x")),
                                unnest(
                                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                                Optional.of("a_x + b_x < bigint '10'"),
                                                anyTree(
                                                        project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x"), "a_z", expression("array")),
                                                                values("msg", "array"))
                                                ), anyTree(
                                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                                values("msg"))))))));
    }
}
