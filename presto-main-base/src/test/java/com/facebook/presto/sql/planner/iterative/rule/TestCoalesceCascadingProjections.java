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
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;

public class TestCoalesceCascadingProjections
        extends BaseRuleTest
{
    @Test
    public void testDoesNotInlineMultiplyReferencedNonTrivialExpression()
    {
        // A non-trivial deterministic child expression referenced more than once must not be inlined,
        // otherwise the coalesced projection would duplicate its computation. With no other inlining
        // target, the rule does not fire.
        tester().assertThat(new CoalesceCascadingProjections(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("doubled");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out1"), p.rowExpression("doubled + 1"))
                                    .put(p.variable("out2"), p.rowExpression("doubled + 2"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("doubled"), p.rowExpression("x * 2"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .doesNotFire();
    }

    @Test
    public void testInlinesMultiplyReferencedConstant()
    {
        // A constant is trivial to duplicate, so it is inlined even when referenced more than once.
        tester().assertThat(new CoalesceCascadingProjections(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("c");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out1"), p.rowExpression("c + 1"))
                                    .put(p.variable("out2"), p.rowExpression("c + 2"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("c"), p.rowExpression("5"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .matches(
                        // The inlined constant has no inputs, so the child collapses to a pure
                        // passthrough and is dropped; the coalesced project sits directly on values.
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression("5 + 1"))
                                        .put("out2", PlanMatchPattern.expression("5 + 2"))
                                        .build(),
                                values(ImmutableMap.of("x", 0))));
    }

    @Test
    public void testInlinesMultiplyReferencedVariableRename()
    {
        // A pure variable rename is trivial to duplicate, so it is inlined even when referenced more
        // than once; each parent reference is simply repointed at the underlying variable.
        tester().assertThat(new CoalesceCascadingProjections(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("renamed");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out1"), p.rowExpression("renamed + 1"))
                                    .put(p.variable("out2"), p.rowExpression("renamed + 2"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("renamed"), p.rowExpression("x"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .matches(
                        // After the rename is inlined the child collapses to an identity passthrough
                        // of x and is dropped; the coalesced project sits directly on values.
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression("x + 1"))
                                        .put("out2", PlanMatchPattern.expression("x + 2"))
                                        .build(),
                                values(ImmutableMap.of("x", 0))));
    }

    @Test
    public void testDoesNotInlineMultiplyReferencedNonDeterministicExpression()
    {
        // A non-deterministic child expression referenced more than once must not be inlined,
        // otherwise the side-effecting computation would be duplicated. With no other inlining
        // target, the rule does not fire.
        tester().assertThat(new CoalesceCascadingProjections(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("r");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out1"), p.rowExpression("r + 1"))
                                    .put(p.variable("out2"), p.rowExpression("r + 2"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("r"), p.rowExpression("random(100)"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .doesNotFire();
    }

    @Test
    public void testInlinesSingleUseNonDeterministicExpression()
    {
        // A single reference never duplicates computation, so even a non-deterministic child
        // expression is inlined when referenced exactly once.
        tester().assertThat(new CoalesceCascadingProjections(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("r");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out1"), p.rowExpression("r + 1"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("r"), p.rowExpression("random(100)"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .matches(
                        // The inlined expression has no inputs, so the child collapses to an empty
                        // projection and is dropped; the coalesced project sits directly on values.
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression("random(100) + 1"))
                                        .build(),
                                values(ImmutableMap.of("x", 0))));
    }

    @Test
    public void testRecursivelyCollapsesSingleUseProjectionChain()
    {
        // A three-deep projection chain whose intermediate expressions are each referenced exactly
        // once must collapse to a single project after the rule reaches fixpoint (driven here by an
        // IterativeOptimizer), inlining every single-use expression and dropping the residual
        // passthrough projects.
        //   values(x)
        //     -> doubled := x * 2
        //       -> shifted := doubled + 1
        //         -> out1 := shifted * 3
        tester().assertThat(ImmutableSet.of(new CoalesceCascadingProjections(getFunctionManager())))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("doubled");
                    p.variable("shifted");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out1"), p.rowExpression("shifted * 3"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("shifted"), p.rowExpression("doubled + 1"))
                                            .build(),
                                    p.project(
                                            Assignments.builder()
                                                    .put(p.variable("doubled"), p.rowExpression("x * 2"))
                                                    .build(),
                                            p.values(p.variable("x")))));
                })
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression("(x * 2 + 1) * 3"))
                                        .build(),
                                values(ImmutableMap.of("x", 0))));
    }

    @Test
    public void testDoesNotInlineChildOutputUsedInsideTry()
    {
        // The parent references the child output `d` only inside TRY(...). Inlining `d := x * 2` into
        // the TRY would change which errors are masked, so the rule must bail via the
        // extractTryArguments path. The child expression is deterministic, so the bailout is
        // attributable solely to TRY. With no other inlining target, the rule does not fire.
        tester().assertThat(new CoalesceCascadingProjections(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("d");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out1"), p.rowExpression("try(1 / d)"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("d"), p.rowExpression("x * 2"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnIdentityOnlyChild()
    {
        tester().assertThat(new CoalesceCascadingProjections(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p ->
                        p.project(
                                assignment(p.variable("output"), p.variable("value")),
                                p.project(
                                        identityAssignments(p.variable("value")),
                                        p.values(p.variable("value")))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new CoalesceCascadingProjections(getFunctionManager()))
                .on(p -> {
                    p.variable("x");
                    p.variable("doubled");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out1"), p.rowExpression("doubled + 1"))
                                    .put(p.variable("out2"), p.rowExpression("doubled + 2"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("doubled"), p.rowExpression("x * 2"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .doesNotFire();
    }
}
