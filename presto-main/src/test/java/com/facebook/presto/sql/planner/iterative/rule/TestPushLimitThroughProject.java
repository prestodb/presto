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
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class TestPushLimitThroughProject
        extends BaseRuleTest
{
    @Test
    public void testPushdownLimitNonIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    return p.limit(1,
                            p.project(
                                    assignment(a, TRUE_LITERAL),
                                    p.values()));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("true")),
                                limit(1, values())));
    }

    @Test
    public void testDoesntPushdownLimitThroughIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    return p.limit(1,
                            p.project(
                                    assignment(a, asSymbolReference(a)),
                                    p.values(a)));
                }).doesNotFire();
    }
}
