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
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.castToRowExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

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
    public void testPushdownLimitWithTiesNNonIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    VariableReferenceExpression projectedA = p.variable("projectedA");
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression projectedB = p.variable("projectedB");
                    VariableReferenceExpression b = p.variable("b");
                    return p.limit(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(projectedA, castToRowExpression("a"), projectedB, castToRowExpression("b")),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                limit(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testPushdownLimitWithTiesThroughProjectionWithExpression()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    VariableReferenceExpression projectedA = p.variable("projectedA");
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression projectedC = p.variable("projectedC");
                    VariableReferenceExpression b = p.variable("b");
                    return p.limit(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(
                                            projectedA, castToRowExpression("a"),
                                            projectedC, OriginalExpressionUtils.castToRowExpression(new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("b")))),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("projectedA", new ExpressionMatcher("a"), "projectedC", new ExpressionMatcher("a + b")),
                                limit(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testDoNotPushdownLimitWithTiesThroughProjectionWithExpression()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    VariableReferenceExpression projectedA = p.variable("projectedA");
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression projectedC = p.variable("projectedC");
                    VariableReferenceExpression b = p.variable("b");
                    return p.limit(
                            1,
                            ImmutableList.of(projectedC),
                            p.project(
                                    Assignments.of(
                                            projectedA, castToRowExpression("a"),
                                            projectedC, OriginalExpressionUtils.castToRowExpression(new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("b")))),
                                    p.values(a, b)));
                })
                .doesNotFire();
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
