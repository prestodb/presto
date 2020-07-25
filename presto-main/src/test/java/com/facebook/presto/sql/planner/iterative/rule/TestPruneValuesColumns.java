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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPruneValuesColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneValuesColumns())
                .on(p ->
                        p.project(
                                assignment(p.variable("y"), expression("x")),
                                p.values(
                                        ImmutableList.of(p.variable("unused"), p.variable("x")),
                                        ImmutableList.of(
                                                constantExpressions(BIGINT, 1L, 2L),
                                                constantExpressions(BIGINT, 3L, 4L)))))
                .matches(
                        project(
                                ImmutableMap.of("y", PlanMatchPattern.expression("x")),
                                values(
                                        ImmutableList.of("x"),
                                        ImmutableList.of(
                                                ImmutableList.of(expression("2")),
                                                ImmutableList.of(expression("4"))))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneValuesColumns())
                .on(p ->
                        p.project(
                                assignment(p.variable("y"), expression("x")),
                                p.values(p.variable("x"))))
                .doesNotFire();
    }
}
