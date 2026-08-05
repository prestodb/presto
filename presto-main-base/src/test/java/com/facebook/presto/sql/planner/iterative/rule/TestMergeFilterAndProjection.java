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
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestMergeFilterAndProjection
        extends BaseRuleTest
{
    @Test
    public void testInlinesPredicateAndReorders()
    {
        // Filter(doubled > 10, Project(doubled := x * 2)) becomes
        // Project(doubled := x * 2, Filter(x * 2 > 10)) so the predicate and projection are
        // expressed over the same inputs.
        tester().assertThat(new MergeFilterAndProjection(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("doubled");
                    return p.filter(
                            p.rowExpression("doubled > 10"),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("doubled"), p.rowExpression("x * 2"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .matches(
                        project(
                                ImmutableMap.of("doubled", PlanMatchPattern.expression("x * 2")),
                                filter(
                                        "x * 2 > 10",
                                        values(ImmutableMap.of("x", 0)))));
    }

    @Test
    public void testDoesNotFireWhenPredicateReferencesNonInlinableOutput()
    {
        // The predicate references a non-deterministic child output more than once, so inlining
        // would duplicate the side-effecting computation. The rule must decline.
        tester().assertThat(new MergeFilterAndProjection(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("r");
                    return p.filter(
                            p.rowExpression("r > 10 AND r < 50"),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("r"), p.rowExpression("random(100)"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenPredicateReferencesSingleUseNonDeterministicOutput()
    {
        // Even a single reference to a non-deterministic child output is unsafe to inline here:
        // the project is retained and still computes random(100), so inlining it into the predicate
        // would evaluate random(100) independently in the filter and the projection (two different
        // draws). The rule must decline.
        tester().assertThat(new MergeFilterAndProjection(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("r");
                    return p.filter(
                            p.rowExpression("r > 10"),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("r"), p.rowExpression("random(100)"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenPredicateWrapsProjectedOutputInTry()
    {
        // The predicate references the child output `d` only inside TRY(...). Inlining `d := x * 2`
        // into the TRY would change which errors are masked, so the rule must bail via the
        // extractTryArguments path even though the child expression is deterministic.
        tester().assertThat(new MergeFilterAndProjection(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("x");
                    p.variable("d");
                    return p.filter(
                            p.rowExpression("try(1 / d) IS NULL"),
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
        // The predicate references only a passthrough/identity output, so reordering would not
        // co-locate any computation.
        tester().assertThat(new MergeFilterAndProjection(getFunctionManager()))
                .setSystemProperty(OPTIMIZE_CASCADING_FILTERS_AND_PROJECTIONS, "true")
                .on(p -> {
                    p.variable("value");
                    return p.filter(
                            p.rowExpression("value > 10"),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("value"), p.variable("value"))
                                            .build(),
                                    p.values(p.variable("value"))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new MergeFilterAndProjection(getFunctionManager()))
                .on(p -> {
                    p.variable("x");
                    p.variable("doubled");
                    return p.filter(
                            p.rowExpression("doubled > 10"),
                            p.project(
                                    Assignments.builder()
                                            .put(p.variable("doubled"), p.rowExpression("x * 2"))
                                            .build(),
                                    p.values(p.variable("x"))));
                })
                .doesNotFire();
    }
}
