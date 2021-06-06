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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.distinctLimit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveUnreferencedJoin
        extends BaseRuleTest
{
    @Test
    public void testAggLeftJoin()
    {
        tester().assertThat(new RemoveUnreferencedJoin.RemoveJoinFromAggregation())
                .on(p -> p.aggregation(a -> a
                        .source(p.join(
                                JoinNode.Type.LEFT,
                                p.values(p.variable("COL1")),
                                p.values(p.variable("COL2")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL1")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(
                                p.variable(p.variable("COL1")),
                                p.rowExpression("count(COL1)"))
                        .globalGrouping()))
                .matches(
                        aggregation(
                                ImmutableMap.of("count", functionCall("count", ImmutableList.of("COL1"))),
                                project(ImmutableMap.of("COL1", expression("COL1")),
                                        values("COL1"))));
    }

    @Test
    public void testAggRightJoin()
    {
        tester().assertThat(new RemoveUnreferencedJoin.RemoveJoinFromAggregation())
                .on(p -> p.aggregation(a -> a
                        .source(p.join(
                                JoinNode.Type.RIGHT,
                                p.values(p.variable("COL1")),
                                p.values(p.variable("COL2")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL1"), p.variable("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                        .addAggregation(
                                p.variable(p.variable("COL2")),
                                p.rowExpression("count(COL2)"))
                        .globalGrouping()))
                .matches(
                        aggregation(
                                ImmutableMap.of("count", functionCall("count", ImmutableList.of("COL2"))),
                                project(ImmutableMap.of("COL2", expression("COL2")),
                                        values("COL2"))));
    }

    @Test
    public void testDistinctLimitLeftJoin()
    {
        tester().assertThat(new RemoveUnreferencedJoin.RemoveJoinFromDistinctLimit())
                .on(p -> p.distinctLimit(
                        1,
                        ImmutableList.of(p.variable("COL1")),
                        p.join(
                                JoinNode.Type.LEFT,
                                p.values(p.variable("COL1")),
                                p.values(p.variable("COL2")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL1")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())))
                .matches(
                        distinctLimit(1,
                                false,
                                project(ImmutableMap.of("COL1", expression("COL1")),
                                        values("COL1"))));
    }

    @Test
    public void testDistinctLimitRightJoin()
    {
        tester().assertThat(new RemoveUnreferencedJoin.RemoveJoinFromDistinctLimit())
                .on(p -> p.distinctLimit(
                        1,
                        ImmutableList.of(p.variable("COL2")),
                        p.join(
                                JoinNode.Type.RIGHT,
                                p.values(p.variable("COL1")),
                                p.values(p.variable("COL2")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())))
                .matches(
                        distinctLimit(1,
                                false,
                                project(ImmutableMap.of("COL2", expression("COL2")),
                                        values("COL2"))));
    }

    @Test
    public void testDistinctLimitLeftJoinNotFire()
    {
        tester().assertThat(new RemoveUnreferencedJoin.RemoveJoinFromDistinctLimit())
                .on(p -> p.distinctLimit(
                        1,
                        ImmutableList.of(p.variable("COL2")),
                        p.join(
                                JoinNode.Type.LEFT,
                                p.values(p.variable("COL1")),
                                p.values(p.variable("COL2")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL1")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())))
                .doesNotFire();
    }

    @Test
    public void testDistinctLimitRightJoinNotFire()
    {
        tester().assertThat(new RemoveUnreferencedJoin.RemoveJoinFromDistinctLimit())
                .on(p -> p.distinctLimit(
                        1,
                        ImmutableList.of(p.variable("COL1")),
                        p.join(
                                JoinNode.Type.RIGHT,
                                p.values(p.variable("COL1")),
                                p.values(p.variable("COL2")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("COL1"), p.variable("COL2"))),
                                ImmutableList.of(p.variable("COL2")),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())))
                .doesNotFire();
    }
}
