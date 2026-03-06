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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_COALESCE_OVER_JOIN_KEYS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestSimplifyCoalesceOverJoinKeys
        extends BaseRuleTest
{
    @Test
    public void testLeftJoinCoalesceLeftRight()
    {
        // COALESCE(l.x, r.y) on LEFT JOIN l.x = r.y -> l.x
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftKey, rightKey))
                                    .build(),
                            p.join(JoinType.LEFT,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        project(ImmutableMap.of("output", expression("left_key")),
                                join(JoinType.LEFT, ImmutableList.of(equiJoinClause("left_key", "right_key")), Optional.empty(),
                                        values("left_key"),
                                        values("right_key"))));
    }

    @Test
    public void testLeftJoinCoalesceRightLeft()
    {
        // COALESCE(r.y, l.x) on LEFT JOIN l.x = r.y -> l.x (left key is always non-null)
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, rightKey, leftKey))
                                    .build(),
                            p.join(JoinType.LEFT,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        project(ImmutableMap.of("output", expression("left_key")),
                                join(JoinType.LEFT, ImmutableList.of(equiJoinClause("left_key", "right_key")), Optional.empty(),
                                        values("left_key"),
                                        values("right_key"))));
    }

    @Test
    public void testRightJoinCoalesceLeftRight()
    {
        // COALESCE(l.x, r.y) on RIGHT JOIN l.x = r.y -> r.y (right key is always non-null)
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftKey, rightKey))
                                    .build(),
                            p.join(JoinType.RIGHT,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        project(ImmutableMap.of("output", expression("right_key")),
                                join(JoinType.RIGHT, ImmutableList.of(equiJoinClause("left_key", "right_key")), Optional.empty(),
                                        values("left_key"),
                                        values("right_key"))));
    }

    @Test
    public void testRightJoinCoalesceRightLeft()
    {
        // COALESCE(r.y, l.x) on RIGHT JOIN l.x = r.y -> r.y
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, rightKey, leftKey))
                                    .build(),
                            p.join(JoinType.RIGHT,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        project(ImmutableMap.of("output", expression("right_key")),
                                join(JoinType.RIGHT, ImmutableList.of(equiJoinClause("left_key", "right_key")), Optional.empty(),
                                        values("left_key"),
                                        values("right_key"))));
    }

    @Test
    public void testInnerJoinCoalesceLeftRight()
    {
        // COALESCE(l.x, r.y) on INNER JOIN l.x = r.y -> l.x (first arg, since both non-null)
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftKey, rightKey))
                                    .build(),
                            p.join(JoinType.INNER,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        project(ImmutableMap.of("output", expression("left_key")),
                                join(JoinType.INNER, ImmutableList.of(equiJoinClause("left_key", "right_key")), Optional.empty(),
                                        values("left_key"),
                                        values("right_key"))));
    }

    @Test
    public void testInnerJoinCoalesceRightLeft()
    {
        // COALESCE(r.y, l.x) on INNER JOIN l.x = r.y -> r.y (first arg, since both non-null)
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, rightKey, leftKey))
                                    .build(),
                            p.join(JoinType.INNER,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        project(ImmutableMap.of("output", expression("right_key")),
                                join(JoinType.INNER, ImmutableList.of(equiJoinClause("left_key", "right_key")), Optional.empty(),
                                        values("left_key"),
                                        values("right_key"))));
    }

    @Test
    public void testDoesNotFireOnFullJoin()
    {
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftKey, rightKey))
                                    .build(),
                            p.join(JoinType.FULL,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnCrossJoin()
    {
        // Cross join has no equi-join criteria
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftKey, rightKey))
                                    .build(),
                            p.join(JoinType.INNER,
                                    p.values(leftKey),
                                    p.values(rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonJoinKeyCoalesce()
    {
        // COALESCE(l.val, r.val) where val columns are NOT join keys
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression leftVal = p.variable("left_val", BIGINT);
                    VariableReferenceExpression rightVal = p.variable("right_val", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftVal, rightVal))
                                    .build(),
                            p.join(JoinType.LEFT,
                                    p.values(leftKey, leftVal),
                                    p.values(rightKey, rightVal),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "false")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftKey, rightKey))
                                    .build(),
                            p.join(JoinType.LEFT,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonCoalesceProject()
    {
        // Project with identity assignments (no COALESCE)
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(leftKey, leftKey)
                                    .put(rightKey, rightKey)
                                    .build(),
                            p.join(JoinType.LEFT,
                                    p.values(leftKey),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }

    @Test
    public void testMultipleJoinKeys()
    {
        // Multiple join keys, COALESCE on both
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey1 = p.variable("left_key1", BIGINT);
                    VariableReferenceExpression rightKey1 = p.variable("right_key1", BIGINT);
                    VariableReferenceExpression leftKey2 = p.variable("left_key2", BIGINT);
                    VariableReferenceExpression rightKey2 = p.variable("right_key2", BIGINT);
                    VariableReferenceExpression output1 = p.variable("output1", BIGINT);
                    VariableReferenceExpression output2 = p.variable("output2", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output1, new SpecialFormExpression(COALESCE, BIGINT, leftKey1, rightKey1))
                                    .put(output2, new SpecialFormExpression(COALESCE, BIGINT, leftKey2, rightKey2))
                                    .build(),
                            p.join(JoinType.LEFT,
                                    p.values(leftKey1, leftKey2),
                                    p.values(rightKey1, rightKey2),
                                    new EquiJoinClause(leftKey1, rightKey1),
                                    new EquiJoinClause(leftKey2, rightKey2)));
                })
                .matches(
                        project(ImmutableMap.of("output1", expression("left_key1"), "output2", expression("left_key2")),
                                join(JoinType.LEFT,
                                        ImmutableList.of(equiJoinClause("left_key1", "right_key1"), equiJoinClause("left_key2", "right_key2")),
                                        Optional.empty(),
                                        values("left_key1", "left_key2"),
                                        values("right_key1", "right_key2"))));
    }

    @Test
    public void testMixedCoalesceAndIdentity()
    {
        // One assignment is COALESCE over join keys, another is identity
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression leftVal = p.variable("left_val", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftKey, rightKey))
                                    .put(leftVal, leftVal)
                                    .build(),
                            p.join(JoinType.LEFT,
                                    p.values(leftKey, leftVal),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .matches(
                        project(ImmutableMap.of("output", expression("left_key"), "left_val", expression("left_val")),
                                join(JoinType.LEFT, ImmutableList.of(equiJoinClause("left_key", "right_key")), Optional.empty(),
                                        values("left_key", "left_val"),
                                        values("right_key"))));
    }

    @Test
    public void testDoesNotFireOnThreeArgCoalesce()
    {
        // COALESCE with 3 arguments -- should not simplify
        tester().assertThat(new SimplifyCoalesceOverJoinKeys())
                .setSystemProperty(SIMPLIFY_COALESCE_OVER_JOIN_KEYS, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression extra = p.variable("extra", BIGINT);
                    VariableReferenceExpression output = p.variable("output", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(output, new SpecialFormExpression(COALESCE, BIGINT, leftKey, rightKey, extra))
                                    .build(),
                            p.join(JoinType.LEFT,
                                    p.values(leftKey, extra),
                                    p.values(rightKey),
                                    new EquiJoinClause(leftKey, rightKey)));
                })
                .doesNotFire();
    }
}
