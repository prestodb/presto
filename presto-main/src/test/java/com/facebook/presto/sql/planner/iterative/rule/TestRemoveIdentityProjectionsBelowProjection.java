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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;

public class TestRemoveIdentityProjectionsBelowProjection
        extends BaseRuleTest
{
    @Test
    public void testTopProjectUseAllOfIdentityProject()
    {
        tester().assertThat(new RemoveIdentityProjectionsBelowProjection())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.project(
                            assignment(a, p.rowExpression("b+1")),
                            p.project(
                                    identityAssignments(b),
                                    p.values(c, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("b+1")),
                                values("c", "b")));
    }

    @Test
    public void testTopProjectUseSubSetOfIdentityProject()
    {
        tester().assertThat(new RemoveIdentityProjectionsBelowProjection())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.project(
                            assignment(a, p.rowExpression("b+1")),
                            p.project(
                                    identityAssignments(b, c),
                                    p.values(c, b, d)));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("b+1")),
                                values("c", "b", "d")));
    }
}
