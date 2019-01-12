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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.Assignments;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPruneValuesColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneValuesColumns())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("y"), expression("x")),
                                p.values(
                                        ImmutableList.of(p.symbol("unused"), p.symbol("x")),
                                        ImmutableList.of(
                                                ImmutableList.of(expression("1"), expression("2")),
                                                ImmutableList.of(expression("3"), expression("4"))))))
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
                                Assignments.of(p.symbol("y"), expression("x")),
                                p.values(p.symbol("x"))))
                .doesNotFire();
    }
}
