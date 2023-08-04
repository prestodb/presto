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
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestLeftJoinNullFilterToSemiJoin
        extends BaseRuleTest
{
    @Test
    public void testTrigger()
    {
        tester().assertThat(new LeftJoinNullFilterToSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("right_k1 is null"),
                            p.join(JoinNode.Type.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1")),
                                    new JoinNode.EquiJoinClause(p.variable("left_k1"), p.variable("right_k1"))));
                })
                .matches(
                        project(
                                filter(
                                        "not(COALESCE(semijoinoutput, false))",
                                        semiJoin(
                                                "left_k1",
                                                "right_k1",
                                                "semijoinoutput",
                                                values("left_k1", "left_k2"),
                                                values("right_k1")))));
    }

    @Test
    public void testNotTriggerWithFilter()
    {
        tester().assertThat(new LeftJoinNullFilterToSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("right_k1 is null"),
                            p.join(JoinNode.Type.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1")),
                                    p.rowExpression("left_k2 > 10"),
                                    new JoinNode.EquiJoinClause(p.variable("left_k1"), p.variable("right_k1"))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerNotNull()
    {
        tester().assertThat(new LeftJoinNullFilterToSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("right_k1 is not null"),
                            p.join(JoinNode.Type.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1")),
                                    new JoinNode.EquiJoinClause(p.variable("left_k1"), p.variable("right_k1"))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerOtherOutputUsed()
    {
        tester().assertThat(new LeftJoinNullFilterToSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("right_k1 is null"),
                            p.join(JoinNode.Type.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2")),
                                    new JoinNode.EquiJoinClause(p.variable("left_k1"), p.variable("right_k1"))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerOutputUsedInFilter()
    {
        tester().assertThat(new LeftJoinNullFilterToSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("right_k1 is null or right_k1 > 2"),
                            p.join(JoinNode.Type.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1")),
                                    new JoinNode.EquiJoinClause(p.variable("left_k1"), p.variable("right_k1"))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerOutputUsedInFilter2()
    {
        tester().assertThat(new LeftJoinNullFilterToSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("right_k1 is null and right_k1 > 2"),
                            p.join(JoinNode.Type.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1")),
                                    new JoinNode.EquiJoinClause(p.variable("left_k1"), p.variable("right_k1"))));
                }).doesNotFire();
    }

    @Test
    public void testNotTriggerOtherOutputUsedInFilter()
    {
        tester().assertThat(new LeftJoinNullFilterToSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("right_k1 is null or left_k2 > 2"),
                            p.join(JoinNode.Type.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1")),
                                    new JoinNode.EquiJoinClause(p.variable("left_k1"), p.variable("right_k1"))));
                }).doesNotFire();
    }

    @Test
    public void testTriggerForFilterWithAnd()
    {
        tester().assertThat(new LeftJoinNullFilterToSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.filter(
                            p.rowExpression("right_k1 is null and left_k2 > 2"),
                            p.join(JoinNode.Type.LEFT,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1")),
                                    new JoinNode.EquiJoinClause(p.variable("left_k1"), p.variable("right_k1"))));
                })
                .matches(
                        filter(
                                "left_k2 > 2",
                                project(
                                        filter(
                                                "not(COALESCE(semijoinoutput, false))",
                                                semiJoin(
                                                        "left_k1",
                                                        "right_k1",
                                                        "semijoinoutput",
                                                        values("left_k1", "left_k2"),
                                                        values("right_k1"))))));
    }
}
