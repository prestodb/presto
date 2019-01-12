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

import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import org.testng.annotations.Test;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneLimitColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneLimitColumns())
                .on(p -> buildProjectedLimit(p, symbol -> symbol.getName().equals("b")))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("b")),
                                limit(
                                        1,
                                        strictProject(
                                                ImmutableMap.of("b", expression("b")),
                                                values("a", "b")))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneLimitColumns())
                .on(p -> buildProjectedLimit(p, alwaysTrue()))
                .doesNotFire();
    }

    private ProjectNode buildProjectedLimit(PlanBuilder planBuilder, Predicate<Symbol> projectionFilter)
    {
        Symbol a = planBuilder.symbol("a");
        Symbol b = planBuilder.symbol("b");
        return planBuilder.project(
                Assignments.identity(Stream.of(a, b).filter(projectionFilter).collect(toImmutableSet())),
                planBuilder.limit(1, planBuilder.values(a, b)));
    }
}
