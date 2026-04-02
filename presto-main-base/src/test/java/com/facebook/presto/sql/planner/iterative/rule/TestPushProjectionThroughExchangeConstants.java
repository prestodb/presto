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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.constant;

/**
 * Tests for the interaction between PushProjectionThroughExchange and constant projections.
 * Verifies that identity+constant projections are NOT pushed through exchanges
 * (to prevent infinite loops with PullConstantProjectionAboveExchange),
 * while projections containing non-trivial expressions ARE still pushed.
 */
public class TestPushProjectionThroughExchangeConstants
        extends BaseRuleTest
{
    @Test
    public void testDoesNotPushIdentityAndConstantOnly()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression constVar = p.variable("const_var", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(a, a)
                                    .put(constVar, constant(42L, BIGINT))
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .singleDistributionPartitioningScheme(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushAllConstants()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression c1 = p.variable("c1", BIGINT);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(c1, constant(10L, BIGINT))
                                    .put(c2, constant(20L, BIGINT))
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .singleDistributionPartitioningScheme(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testStillPushesNonTrivialExpressions()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression aTimes5 = p.variable("a_times_5", BIGINT);
                    VariableReferenceExpression constVar = p.variable("const_var", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(aTimes5, p.binaryOperation(MULTIPLY, a, constant(5L, BIGINT)))
                                    .put(constVar, constant(42L, BIGINT))
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .singleDistributionPartitioningScheme(a)));
                })
                .matches(
                        exchange(
                                project(
                                        values(ImmutableList.of("a")))
                                        .withAlias("a_times_5", expression("a * 5"))
                                        .withAlias("const_var", expression("42"))));
    }

    @Test
    public void testDoesNotPushIdentityOnly()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(a, a)
                                    .put(b, b)
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(p.values(a, b))
                                    .addInputsSet(a, b)
                                    .singleDistributionPartitioningScheme(a, b)));
                })
                .doesNotFire();
    }
}
