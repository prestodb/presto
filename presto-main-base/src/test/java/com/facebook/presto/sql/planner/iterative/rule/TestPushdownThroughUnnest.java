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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_THROUGH_UNNEST;
import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class TestPushdownThroughUnnest
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireWithNoUnnestChild()
    {
        // Project over values (not unnest) should not fire
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p ->
                        p.project(
                                assignment(p.variable("x"), constant(3L, BIGINT)),
                                p.values(p.variable("a"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAllAssignmentsDependOnUnnest()
    {
        // When all projected expressions depend on unnest output, nothing to push
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    return p.project(
                            // y + 1 depends on unnest output y
                            assignment(p.variable("y_plus_1", BIGINT),
                                    call("y + 1", functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, y, constant(1L, BIGINT))),
                            p.unnest(
                                    p.values(x, a),
                                    ImmutableList.of(x),
                                    ImmutableMap.of(a, ImmutableList.of(y)),
                                    Optional.empty()));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenOnlyIdentityAssignments()
    {
        // When all non-unnest-dependent assignments are just identity (pass-through), don't fire
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    return p.project(
                            assignment(x, x, y, y),
                            p.unnest(
                                    p.values(x, a),
                                    ImmutableList.of(x),
                                    ImmutableMap.of(a, ImmutableList.of(y)),
                                    Optional.empty()));
                })
                .doesNotFire();
    }

    @Test
    public void testPushesNonDependentExpression()
    {
        // select x+1, y from t cross join unnest(a) t(y)
        // x+1 should be pushed below the unnest
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    return p.project(
                            assignment(
                                    p.variable("x_plus_1", BIGINT),
                                    call("x + 1", functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, x, constant(1L, BIGINT)),
                                    y, y),
                            p.unnest(
                                    p.values(x, a),
                                    ImmutableList.of(x),
                                    ImmutableMap.of(a, ImmutableList.of(y)),
                                    Optional.empty()));
                })
                .matches(
                        project(
                                ImmutableMap.of("x_plus_1", expression("x_plus_1"), "y", expression("y")),
                                unnest(
                                        ImmutableMap.of("a", ImmutableList.of("y")),
                                        project(
                                                ImmutableMap.of("x", expression("x"), "a", expression("a"), "x_plus_1", expression("x + 1")),
                                                values("x", "a")))));
    }

    @Test
    public void testPushesConstantExpression()
    {
        // Constant expressions don't depend on unnest outputs and should be pushed down
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    return p.project(
                            assignment(
                                    p.variable("const", BIGINT), constant(42L, BIGINT),
                                    y, y),
                            p.unnest(
                                    p.values(x, a),
                                    ImmutableList.of(x),
                                    ImmutableMap.of(a, ImmutableList.of(y)),
                                    Optional.empty()));
                })
                .matches(
                        project(
                                ImmutableMap.of("const", expression("const"), "y", expression("y")),
                                unnest(
                                        ImmutableMap.of("a", ImmutableList.of("y")),
                                        project(
                                                ImmutableMap.of("x", expression("x"), "a", expression("a"), "const", expression("42")),
                                                values("x", "a")))));
    }

    @Test
    public void testPushesFilterConjunctBelowUnnest()
    {
        // Project -> Filter(x > 10) -> Unnest
        // x > 10 doesn't depend on unnest output y, so it should be pushed below
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    return p.project(
                            assignment(x, x, y, y),
                            p.filter(
                                    p.rowExpression("x > BIGINT '10'"),
                                    p.unnest(
                                            p.values(x, a),
                                            ImmutableList.of(x),
                                            ImmutableMap.of(a, ImmutableList.of(y)),
                                            Optional.empty())));
                })
                .matches(
                        project(
                                ImmutableMap.of("x", expression("x"), "y", expression("y")),
                                unnest(
                                        ImmutableMap.of("a", ImmutableList.of("y")),
                                        filter("x > BIGINT '10'",
                                                values("x", "a")))));
    }

    @Test
    public void testDoesNotFireWhenFilterDependsOnUnnest()
    {
        // Project -> Filter(y > 0) -> Unnest
        // y > 0 depends on unnest output y, and project has only identity assignments
        // Nothing is pushable
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    return p.project(
                            assignment(x, x, y, y),
                            p.filter(
                                    p.rowExpression("y > BIGINT '0'"),
                                    p.unnest(
                                            p.values(x, a),
                                            ImmutableList.of(x),
                                            ImmutableMap.of(a, ImmutableList.of(y)),
                                            Optional.empty())));
                })
                .doesNotFire();
    }

    @Test
    public void testPushesMixedFilterConjuncts()
    {
        // Project -> Filter(x > 10 AND y > 0) -> Unnest
        // x > 10 can be pushed below, y > 0 must remain above
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    return p.project(
                            assignment(x, x, y, y),
                            p.filter(
                                    p.rowExpression("x > BIGINT '10' AND y > BIGINT '0'"),
                                    p.unnest(
                                            p.values(x, a),
                                            ImmutableList.of(x),
                                            ImmutableMap.of(a, ImmutableList.of(y)),
                                            Optional.empty())));
                })
                .matches(
                        project(
                                ImmutableMap.of("x", expression("x"), "y", expression("y")),
                                filter("y > BIGINT '0'",
                                        unnest(
                                                ImmutableMap.of("a", ImmutableList.of("y")),
                                                filter("x > BIGINT '10'",
                                                        values("x", "a"))))));
    }

    @Test
    public void testPushesBothProjectionAndFilter()
    {
        // Project(x+1, y) -> Filter(x > 10) -> Unnest
        // Both x+1 projection and x > 10 filter can be pushed below
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    return p.project(
                            assignment(
                                    p.variable("x_plus_1", BIGINT),
                                    call("x + 1", functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, x, constant(1L, BIGINT)),
                                    y, y),
                            p.filter(
                                    p.rowExpression("x > BIGINT '10'"),
                                    p.unnest(
                                            p.values(x, a),
                                            ImmutableList.of(x),
                                            ImmutableMap.of(a, ImmutableList.of(y)),
                                            Optional.empty())));
                })
                .matches(
                        project(
                                ImmutableMap.of("x_plus_1", expression("x_plus_1"), "y", expression("y")),
                                unnest(
                                        ImmutableMap.of("a", ImmutableList.of("y")),
                                        project(
                                                ImmutableMap.of("x", expression("x"), "a", expression("a"), "x_plus_1", expression("x + 1")),
                                                filter("x > BIGINT '10'",
                                                        values("x", "a"))))));
    }

    @Test
    public void testDoesNotPushProjectionReferencingOrdinality()
    {
        // Ordinality is an unnest-produced variable, so expressions referencing it should not be pushed
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression ord = p.variable("ord", BIGINT);
                    return p.project(
                            assignment(
                                    p.variable("ord_plus_1", BIGINT),
                                    call("ord + 1", functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, ord, constant(1L, BIGINT))),
                            p.unnest(
                                    p.values(x, a),
                                    ImmutableList.of(x),
                                    ImmutableMap.of(a, ImmutableList.of(y)),
                                    Optional.of(ord)));
                })
                .doesNotFire();
    }

    @Test
    public void testPushesWithOrdinalityPresent()
    {
        // x+1 doesn't depend on unnest output y or ordinality ord, so it should be pushed
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression ord = p.variable("ord", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("x_plus_1", BIGINT),
                                            call("x + 1", functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, x, constant(1L, BIGINT)))
                                    .put(y, y)
                                    .put(ord, ord)
                                    .build(),
                            p.unnest(
                                    p.values(x, a),
                                    ImmutableList.of(x),
                                    ImmutableMap.of(a, ImmutableList.of(y)),
                                    Optional.of(ord)));
                })
                .matches(
                        project(
                                ImmutableMap.of("x_plus_1", expression("x_plus_1"), "y", expression("y")),
                                unnest(
                                        ImmutableMap.of("a", ImmutableList.of("y")),
                                        project(
                                                ImmutableMap.of("x", expression("x"), "a", expression("a"), "x_plus_1", expression("x + 1")),
                                                values("x", "a")))));
    }

    @Test
    public void testDoesNotPushWithMultipleUnnestOutputs()
    {
        // y1 + y2 depends on unnest outputs, should not be pushed
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y1 = p.variable("y1", BIGINT);
                    VariableReferenceExpression y2 = p.variable("y2", BIGINT);
                    return p.project(
                            assignment(
                                    p.variable("y1_plus_y2", BIGINT),
                                    call("y1 + y2", functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, y1, y2)),
                            p.unnest(
                                    p.values(x, a),
                                    ImmutableList.of(x),
                                    ImmutableMap.of(a, ImmutableList.of(y1, y2)),
                                    Optional.empty()));
                })
                .doesNotFire();
    }

    @Test
    public void testPushesWithMultipleUnnestOutputs()
    {
        // x+1 depends only on replicated x, should be pushed even with multiple unnest outputs
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushdownThroughUnnest(tester().getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSHDOWN_THROUGH_UNNEST, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression y1 = p.variable("y1", BIGINT);
                    VariableReferenceExpression y2 = p.variable("y2", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("x_plus_1", BIGINT),
                                            call("x + 1", functionResolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, x, constant(1L, BIGINT)))
                                    .put(y1, y1)
                                    .put(y2, y2)
                                    .build(),
                            p.unnest(
                                    p.values(x, a),
                                    ImmutableList.of(x),
                                    ImmutableMap.of(a, ImmutableList.of(y1, y2)),
                                    Optional.empty()));
                })
                .matches(
                        project(
                                ImmutableMap.of("x_plus_1", expression("x_plus_1"), "y1", expression("y1"), "y2", expression("y2")),
                                unnest(
                                        ImmutableMap.of("a", ImmutableList.of("y1", "y2")),
                                        project(
                                                ImmutableMap.of("x", expression("x"), "a", expression("a"), "x_plus_1", expression("x + 1")),
                                                values("x", "a")))));
    }
}
