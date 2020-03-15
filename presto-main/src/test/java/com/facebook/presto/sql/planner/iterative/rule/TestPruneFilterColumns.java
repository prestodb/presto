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

import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class TestPruneFilterColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneFilterColumns())
                .on(p -> buildProjectedFilter(p, variable -> variable.getName().equals("b")))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("b")),
                                filter(
                                        "b > 5",
                                        strictProject(
                                                ImmutableMap.of("b", expression("b")),
                                                values("a", "b")))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneFilterColumns())
                .on(p -> buildProjectedFilter(p, symbol -> symbol.getName().equals("a")))
                .doesNotFire();
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneFilterColumns())
                .on(p -> buildProjectedFilter(p, alwaysTrue()))
                .doesNotFire();
    }

    private ProjectNode buildProjectedFilter(PlanBuilder planBuilder, Predicate<VariableReferenceExpression> projectionFilter)
    {
        VariableReferenceExpression a = planBuilder.variable("a");
        VariableReferenceExpression b = planBuilder.variable("b");
        return planBuilder.project(
                identityAssignmentsAsSymbolReferences(Stream.of(a, b).filter(projectionFilter).collect(toImmutableSet())),
                planBuilder.filter(
                        planBuilder.expression("b > 5"),
                        planBuilder.values(a, b)));
    }
}
