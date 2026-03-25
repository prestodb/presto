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
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_PROJECTION_THROUGH_CROSS_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushProjectionThroughCrossJoin
        extends BaseRuleTest
{
    @Test
    public void testPushLeftOnlyProjection()
    {
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a_plus_1", BIGINT), p.rowExpression("a + BIGINT '1'"))
                                    .put(b, b)
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a_plus_1", expression("a_plus_1"),
                                        "b", expression("b")),
                                join(
                                        project(
                                                ImmutableMap.of("a_plus_1", expression("a + BIGINT '1'")),
                                                values("a")),
                                        values("b"))));
    }

    @Test
    public void testPushRightOnlyProjection()
    {
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(a, a)
                                    .put(p.variable("b_plus_1", BIGINT), p.rowExpression("b + BIGINT '1'"))
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression("a"),
                                        "b_plus_1", expression("b_plus_1")),
                                join(
                                        values("a"),
                                        project(
                                                ImmutableMap.of("b_plus_1", expression("b + BIGINT '1'")),
                                                values("b")))));
    }

    @Test
    public void testPushBothSides()
    {
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a_plus_1", BIGINT), p.rowExpression("a + BIGINT '1'"))
                                    .put(p.variable("b_plus_1", BIGINT), p.rowExpression("b + BIGINT '1'"))
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a_plus_1", expression("a_plus_1"),
                                        "b_plus_1", expression("b_plus_1")),
                                join(
                                        project(
                                                ImmutableMap.of("a_plus_1", expression("a + BIGINT '1'")),
                                                values("a")),
                                        project(
                                                ImmutableMap.of("b_plus_1", expression("b + BIGINT '1'")),
                                                values("b")))));
    }

    @Test
    public void testDoesNotFireOnMixedProjections()
    {
        // All projections reference both sides — nothing to push
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a_plus_b", BIGINT), p.rowExpression("a + b"))
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnConstantProjection()
    {
        // Constant expressions don't reference either side — nothing to push
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("const_val", BIGINT), p.rowExpression("BIGINT '42'"))
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnIdentityOnly()
    {
        // All projections are identity — nothing to push
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(a, a)
                                    .put(b, b)
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "false")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a_plus_1", BIGINT), p.rowExpression("a + BIGINT '1'"))
                                    .put(b, b)
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonCrossJoin()
    {
        // Join with criteria is not a cross join
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a_plus_1", BIGINT), p.rowExpression("a + BIGINT '1'"))
                                    .put(b, b)
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b),
                                    new com.facebook.presto.spi.plan.EquiJoinClause(a, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testMixedPushableAndNonPushable()
    {
        // One left-only, one mixed — only the left-only should be pushed
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a_plus_1", BIGINT), p.rowExpression("a + BIGINT '1'"))
                                    .put(p.variable("a_plus_b", BIGINT), p.rowExpression("a + b"))
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a_plus_1", expression("a_plus_1"),
                                        "a_plus_b", expression("a + b")),
                                join(
                                        project(
                                                ImmutableMap.of(
                                                        "a_plus_1", expression("a + BIGINT '1'"),
                                                        "a", expression("a")),
                                                values("a")),
                                        values("b"))));
    }

    @Test
    public void testDoesNotFireOnJoinWithFilter()
    {
        // Inner join with a filter (no equi-join keys) is not a cross join
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a_plus_1", BIGINT), p.rowExpression("a + BIGINT '1'"))
                                    .put(b, b)
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b),
                                    p.rowExpression("a > b")));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushNonDeterministicExpression()
    {
        // random() is non-deterministic — pushing it below the cross join would change
        // semantics (computed once then replicated vs computed per output row)
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("rand_val", DOUBLE), p.rowExpression("random()"))
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .doesNotFire();
    }

    @Test
    public void testPushesDeterministicButKeepsNonDeterministic()
    {
        // a + 1 is deterministic and references only left — should be pushed
        // random() is non-deterministic — should stay above
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("a_plus_1", BIGINT), p.rowExpression("a + BIGINT '1'"))
                                    .put(p.variable("rand_val", DOUBLE), p.rowExpression("random()"))
                                    .build(),
                            p.join(
                                    JoinType.INNER,
                                    p.values(a),
                                    p.values(b)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a_plus_1", expression("a_plus_1"),
                                        "rand_val", expression("random()")),
                                join(
                                        project(
                                                ImmutableMap.of("a_plus_1", expression("a + BIGINT '1'")),
                                                values("a")),
                                        values("b"))));
    }

    @Test
    public void testCascadingProjectionsBothPushLeft()
    {
        // Project(y = x + 1) -> Project(x = a + 1, b = b) -> CrossJoin
        // Both x = a + 1 and y = x + 1 are transitively left-only
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("y", BIGINT), p.rowExpression("x + BIGINT '1'"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(x, p.rowExpression("a + BIGINT '1'"))
                                            .put(b, b)
                                            .build(),
                                    p.join(
                                            JoinType.INNER,
                                            p.values(a),
                                            p.values(b))));
                })
                .matches(
                        project(
                                ImmutableMap.of("y", expression("y")),
                                join(
                                        project(
                                                ImmutableMap.of("y", expression("x + BIGINT '1'")),
                                                project(
                                                        ImmutableMap.of("x", expression("a + BIGINT '1'")),
                                                        values("a"))),
                                        values("b"))));
    }

    @Test
    public void testCascadingProjectionsPushBothSides()
    {
        // Intermediate pushes to left and right, top pushes transitively to both sides
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("x_plus_1", BIGINT), p.rowExpression("x + BIGINT '1'"))
                                    .put(p.variable("y_plus_1", BIGINT), p.rowExpression("y + BIGINT '1'"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(x, p.rowExpression("a + BIGINT '1'"))
                                            .put(y, p.rowExpression("b + BIGINT '1'"))
                                            .build(),
                                    p.join(
                                            JoinType.INNER,
                                            p.values(a),
                                            p.values(b))));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "x_plus_1", expression("x_plus_1"),
                                        "y_plus_1", expression("y_plus_1")),
                                join(
                                        project(
                                                ImmutableMap.of("x_plus_1", expression("x + BIGINT '1'")),
                                                project(
                                                        ImmutableMap.of("x", expression("a + BIGINT '1'")),
                                                        values("a"))),
                                        project(
                                                ImmutableMap.of("y_plus_1", expression("y + BIGINT '1'")),
                                                project(
                                                        ImmutableMap.of("y", expression("b + BIGINT '1'")),
                                                        values("b"))))));
    }

    @Test
    public void testCascadingProjectionsIntermediateMixedKeepsAbove()
    {
        // Intermediate has left-only (x = a + 1) and mixed (w = a + b) assignments.
        // Top references w which is defined above cross join, so it stays above.
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression w = p.variable("w", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("result", BIGINT), p.rowExpression("x + w"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(x, p.rowExpression("a + BIGINT '1'"))
                                            .put(w, p.rowExpression("a + b"))
                                            .build(),
                                    p.join(
                                            JoinType.INNER,
                                            p.values(a),
                                            p.values(b))));
                })
                .matches(
                        project(
                                ImmutableMap.of("result", expression("x + w")),
                                project(
                                        ImmutableMap.of(
                                                "x", expression("x"),
                                                "w", expression("a + b")),
                                        join(
                                                project(
                                                        ImmutableMap.of("x", expression("a + BIGINT '1'")),
                                                        values("a")),
                                                values("b")))));
    }

    @Test
    public void testCascadingDoesNotFireOnAllIdentity()
    {
        // All cascading projects are identity — nothing to push
        tester().assertThat(new PushProjectionThroughCrossJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PROJECTION_THROUGH_CROSS_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(a, a)
                                    .put(b, b)
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(a, a)
                                            .put(b, b)
                                            .build(),
                                    p.join(
                                            JoinType.INNER,
                                            p.values(a),
                                            p.values(b))));
                })
                .doesNotFire();
    }
}
