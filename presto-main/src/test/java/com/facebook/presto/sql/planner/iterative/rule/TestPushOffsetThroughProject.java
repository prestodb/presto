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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OFFSET_CLAUSE_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.offset;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.castToRowExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class TestPushOffsetThroughProject
        extends BaseRuleTest
{
    @Test
    public void testPushdownOffsetNonIdentityProjection()
    {
        tester().assertThat(new PushOffsetThroughProject())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    return p.offset(
                            5,
                            p.project(
                                    Assignments.of(a, castToRowExpression(TRUE_LITERAL.toString())),
                                    p.values()));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("true")),
                                offset(5, values())));
    }

    @Test
    public void testDoNotPushdownOffsetThroughIdentityProjection()
    {
        tester().assertThat(new PushOffsetThroughProject())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    return p.offset(
                            5,
                            p.project(
                                    Assignments.of(a, castToRowExpression(a.getName())),
                                    p.values(a)));
                }).doesNotFire();
    }
}
