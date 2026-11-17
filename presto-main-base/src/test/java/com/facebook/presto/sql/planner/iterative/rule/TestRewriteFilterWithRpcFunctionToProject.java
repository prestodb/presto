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

import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.RPC_FUNCTION_OPTIMIZER_ENABLED;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteFilterWithRpcFunctionToProject
        extends BaseRuleTest
{
    @Test
    public void testLocalFunctionInFilterIsLeftAlone()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager(), ImmutableSet::of))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("abs(x) > 1"),
                            p.values(p.variable("x", INTEGER)));
                })
                .doesNotFire();
    }

    @Test
    public void testNamedFunctionInFilterIsRewritten()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager(), () -> ImmutableSet.of("abs")))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("abs(x) > 1"),
                            p.values(p.variable("x", INTEGER)));
                })
                .matches(rewrittenTo("abs(x) > 1"));
    }

    @Test
    public void testNamedFunctionIsLeftAloneWhenRpcLiftingIsOff()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager(), () -> ImmutableSet.of("abs")))
                .setSystemProperty(RPC_FUNCTION_OPTIMIZER_ENABLED, "false")
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("abs(x) > 1"),
                            p.values(p.variable("x", INTEGER)));
                })
                .doesNotFire();
    }

    @Test
    public void testNameMatchIgnoresCase()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager(), () -> ImmutableSet.of("abs")))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("ABS(x) > 1"),
                            p.values(p.variable("x", INTEGER)));
                })
                .matches(rewrittenTo("abs(x) > 1"));
    }

    @Test
    public void testNamedFunctionNestedInPredicateIsRewritten()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager(), () -> ImmutableSet.of("abs")))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("mod(abs(x), 2) > 1"),
                            p.values(p.variable("x", INTEGER)));
                })
                .matches(rewrittenTo("mod(abs(x), 2) > 1"));
    }

    @Test
    public void testAnotherFunctionOfTheSameArityIsLeftAlone()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager(), () -> ImmutableSet.of("abs")))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("sign(x) > 1"),
                            p.values(p.variable("x", INTEGER)));
                })
                .doesNotFire();
    }

    @Test
    public void testConjunctWithoutTheFunctionIsLeftBelowTheProjection()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager(), () -> ImmutableSet.of("abs")))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("abs(x) > 1 AND x < 10"),
                            p.values(p.variable("x", INTEGER)));
                })
                .matches(project(
                        ImmutableMap.of("x", PlanMatchPattern.expression("x")),
                        filter(
                                "greater_than",
                                project(
                                        ImmutableMap.of(
                                                "greater_than", PlanMatchPattern.expression("abs(x) > 1"),
                                                "x", PlanMatchPattern.expression("x")),
                                        filter(
                                                "x < 10",
                                                values(ImmutableMap.of("x", 0)))))));
    }

    @Test
    public void testEveryConjunctCallingTheFunctionMovesTogether()
    {
        tester().assertThat(new RewriteFilterWithExternalFunctionToProject(getFunctionManager(), () -> ImmutableSet.of("abs")))
                .on(p -> {
                    p.variable("x", INTEGER);
                    return p.filter(
                            p.rowExpression("abs(x) > 1 AND abs(x) < 10"),
                            p.values(p.variable("x", INTEGER)));
                })
                .matches(project(
                        ImmutableMap.of("x", PlanMatchPattern.expression("x")),
                        filter(
                                "expr",
                                project(
                                        ImmutableMap.of(
                                                "expr", PlanMatchPattern.expression("abs(x) > 1 AND abs(x) < 10"),
                                                "x", PlanMatchPattern.expression("x")),
                                        values(ImmutableMap.of("x", 0))))));
    }

    /**
     * The predicate moves into a projection below the filter, the filter tests the variable that
     * projection assigns, and a projection above restores the node's original outputs. Every case
     * here compares with {@code >}, so the variable allocator names that variable after the
     * comparison.
     */
    private static PlanMatchPattern rewrittenTo(String predicate)
    {
        return project(
                ImmutableMap.of("x", PlanMatchPattern.expression("x")),
                filter(
                        "greater_than",
                        project(
                                ImmutableMap.of(
                                        "greater_than", PlanMatchPattern.expression(predicate),
                                        "x", PlanMatchPattern.expression("x")),
                                values(ImmutableMap.of("x", 0)))));
    }
}
