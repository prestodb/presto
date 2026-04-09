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
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;

public class TestInlineProjections
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new InlineProjections(getFunctionManager()))
                .on(p ->
                        p.project(
                                p.project(
                                        p.values(p.variable("x")),
                                        Assignments.builder()
                                                .put(p.variable("symbol"), p.rowExpression("x"))
                                                .put(p.variable("complex"), p.rowExpression("x * 2"))
                                                .put(p.variable("literal"), p.rowExpression("1"))
                                                .put(p.variable("complex_2"), p.rowExpression("x - 1"))
                                                .build()),
                                Assignments.builder()
                                        .put(p.variable("identity"), p.rowExpression("symbol")) // identity
                                        .put(p.variable("multi_complex_1"), p.rowExpression("complex + 1")) // complex expression referenced multiple times
                                        .put(p.variable("multi_complex_2"), p.rowExpression("complex + 2")) // complex expression referenced multiple times
                                        .put(p.variable("multi_literal_1"), p.rowExpression("literal + 1")) // literal referenced multiple times
                                        .put(p.variable("multi_literal_2"), p.rowExpression("literal + 2")) // literal referenced multiple times
                                        .put(p.variable("single_complex"), p.rowExpression("complex_2 + 2")) // complex expression reference only once
                                        .put(p.variable("try"), p.rowExpression("try(complex / literal)"))
                                        .build()))
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression("x"))
                                        .put("out2", PlanMatchPattern.expression("y + 1"))
                                        .put("out3", PlanMatchPattern.expression("y + 2"))
                                        .put("out4", PlanMatchPattern.expression("1 + 1"))
                                        .put("out5", PlanMatchPattern.expression("1 + 2"))
                                        .put("out6", PlanMatchPattern.expression("x - 1 + 2"))
                                        .put("out7", PlanMatchPattern.expression("try(y / 1)"))
                                        .build(),
                                project(
                                        ImmutableMap.of(
                                                "x", PlanMatchPattern.expression("x"),
                                                "y", PlanMatchPattern.expression("x * 2")),
                                        values(ImmutableMap.of("x", 0)))));
    }

    @Test
    public void testRowExpression()
    {
        // TODO add testing to expressions that need desugaring like 'try'
        tester().assertThat(new InlineProjections(getFunctionManager()))
                .on(p -> {
                    p.variable("symbol");
                    p.variable("complex");
                    p.variable("literal");
                    p.variable("complex_2");
                    p.variable("x");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("identity"), p.rowExpression("symbol")) // identity
                                    .put(p.variable("multi_complex_1"), p.rowExpression("complex + 1")) // complex expression referenced multiple times
                                    .put(p.variable("multi_complex_2"), p.rowExpression("complex + 2")) // complex expression referenced multiple times
                                    .put(p.variable("multi_literal_1"), p.rowExpression("literal + 1")) // literal referenced multiple times
                                    .put(p.variable("multi_literal_2"), p.rowExpression("literal + 2")) // literal referenced multiple times
                                    .put(p.variable("single_complex"), p.rowExpression("complex_2 + 2")) // complex expression reference only once
                                    .build(),
                            p.project(Assignments.builder()
                                            .put(p.variable("symbol"), p.rowExpression("x"))
                                            .put(p.variable("complex"), p.rowExpression("x * 2"))
                                            .put(p.variable("literal"), p.rowExpression("1"))
                                            .put(p.variable("complex_2"), p.rowExpression("x - 1"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression("x"))
                                        .put("out2", PlanMatchPattern.expression("y + 1"))
                                        .put("out3", PlanMatchPattern.expression("y + 2"))
                                        .put("out4", PlanMatchPattern.expression("1 + 1"))
                                        .put("out5", PlanMatchPattern.expression("1 + 2"))
                                        .put("out6", PlanMatchPattern.expression("x - 1 + 2"))
                                        .build(),
                                project(
                                        ImmutableMap.of(
                                                "x", PlanMatchPattern.expression("x"),
                                                "y", PlanMatchPattern.expression("x * 2")),
                                        values(ImmutableMap.of("x", 0)))));
    }

    @Test
    public void testIdentityProjections()
    {
        tester().assertThat(new InlineProjections(getFunctionManager()))
                .on(p ->
                        p.project(
                                assignment(p.variable("output"), p.variable("value")),
                                p.project(
                                        identityAssignments(p.variable("value")),
                                        p.values(p.variable("value")))))
                .doesNotFire();
    }

    @Test
    public void testSubqueryProjections()
    {
        tester().assertThat(new InlineProjections(getFunctionManager()))
                .on(p ->
                        p.project(
                                identityAssignments(p.variable("fromOuterScope"), p.variable("value")),
                                p.project(
                                        identityAssignments(p.variable("value")),
                                        p.values(p.variable("value")))))
                .doesNotFire();
    }

    @Test
    public void testWideProjectionWithPassthroughVariables()
    {
        // Verify that InlineProjections correctly handles wide projections
        // where most parent assignments are passthrough variables that don't
        // reference inlining targets. The optimization should skip inlining
        // for these passthrough variables.
        tester().assertThat(new InlineProjections(getFunctionManager()))
                .on(p -> {
                    p.variable("x");
                    p.variable("pass1");
                    p.variable("pass2");
                    p.variable("pass3");
                    p.variable("complex_child");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out_pass1"), p.rowExpression("pass1"))  // passthrough, not a target
                                    .put(p.variable("out_pass2"), p.rowExpression("pass2"))  // passthrough, not a target
                                    .put(p.variable("out_pass3"), p.rowExpression("pass3"))  // passthrough, not a target
                                    .put(p.variable("out_complex"), p.rowExpression("complex_child + 1")) // references singleton
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("pass1"), p.rowExpression("x"))
                                            .put(p.variable("pass2"), p.rowExpression("x"))
                                            .put(p.variable("pass3"), p.rowExpression("x"))
                                            .put(p.variable("complex_child"), p.rowExpression("x * 2")) // singleton reference
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression("x"))
                                        .put("out2", PlanMatchPattern.expression("x"))
                                        .put("out3", PlanMatchPattern.expression("x"))
                                        .put("out4", PlanMatchPattern.expression("x * 2 + 1"))
                                        .build(),
                                project(
                                        ImmutableMap.of(
                                                "x", PlanMatchPattern.expression("x")),
                                        values(ImmutableMap.of("x", 0)))));
    }

    @Test
    public void testWideProjectionWithTryOverChildExpression()
    {
        // When the only non-identity inlining candidate is a complex child
        // expression referenced inside TRY, the rule should not fire.
        // The passthrough variables use identity assignments (pass1 := pass1)
        // so they are excluded from inlining by the isIdentity check.
        tester().assertThat(new InlineProjections(getFunctionManager()))
                .on(p -> {
                    p.variable("pass1");
                    p.variable("pass2");
                    p.variable("pass3");
                    p.variable("complex_child");
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("out_pass1"), p.rowExpression("pass1"))
                                    .put(p.variable("out_pass2"), p.rowExpression("pass2"))
                                    .put(p.variable("out_pass3"), p.rowExpression("pass3"))
                                    .put(p.variable("out_try"), p.rowExpression("try(complex_child)"))
                                    .build(),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("pass1"), p.rowExpression("pass1"))
                                            .put(p.variable("pass2"), p.rowExpression("pass2"))
                                            .put(p.variable("pass3"), p.rowExpression("pass3"))
                                            .put(p.variable("complex_child"), p.rowExpression("pass1 * pass1 + 1"))
                                            .build(),
                                    p.values(p.variable("pass1"), p.variable("pass2"), p.variable("pass3"))));
                })
                .doesNotFire();
    }
}
