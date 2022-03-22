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

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushProjectionThroughExchange
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireNoExchange()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p ->
                        p.project(
                                assignment(p.variable("x"), constant(3L, BIGINT)),
                                p.values(p.variable("a"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireNarrowingProjection()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");

                    return p.project(
                            Assignments.builder()
                                    .put(a, a)
                                    .put(b, b)
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(p.values(a, b, c))
                                    .addInputsSet(a, b, c)
                                    .singleDistributionPartitioningScheme(a, b, c)));
                })
                .doesNotFire();
    }

    @Test
    public void testSimpleMultipleInputs()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression c2 = p.variable("c2");
                    VariableReferenceExpression x = p.variable("x");
                    return p.project(
                            assignment(
                                    x, constant(3L, BIGINT),
                                    c2, c),
                            p.exchange(e -> e
                                    .addSource(
                                            p.values(a))
                                    .addSource(
                                            p.values(b))
                                    .addInputsSet(a)
                                    .addInputsSet(b)
                                    .singleDistributionPartitioningScheme(c)));
                })
                .matches(
                        exchange(
                                project(
                                        values(ImmutableList.of("a")))
                                        .withAlias("x1", expression("3")),
                                project(
                                        values(ImmutableList.of("b")))
                                        .withAlias("x2", expression("3")))
                                // verify that data originally on symbols aliased as x1 and x2 is part of exchange output
                                .withAlias("x1")
                                .withAlias("x2"));
    }

    @Test
    public void testPartitioningColumnAndHashWithoutIdentityMappingInProjection()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression h = p.variable("h");
                    VariableReferenceExpression aTimes5 = p.variable("a_times_5");
                    VariableReferenceExpression bTimes5 = p.variable("b_times_5");
                    VariableReferenceExpression hTimes5 = p.variable("h_times_5");
                    return p.project(
                            Assignments.builder()
                                    .put(aTimes5, p.binaryOperation(MULTIPLY, a, constant(5L, BIGINT)))
                                    .put(bTimes5, p.binaryOperation(MULTIPLY, b, constant(5L, BIGINT)))
                                    .put(hTimes5, p.binaryOperation(MULTIPLY, h, constant(5L, BIGINT)))
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(
                                            p.values(a, b, h))
                                    .addInputsSet(a, b, h)
                                    .fixedHashDistributionParitioningScheme(
                                            ImmutableList.of(a, b, h),
                                            ImmutableList.of(b),
                                            h)));
                })
                .matches(
                        project(
                                exchange(
                                        project(
                                                values(
                                                        ImmutableList.of("a", "b", "h"))
                                        ).withNumberOfOutputColumns(5)
                                                .withAlias("b", expression("b"))
                                                .withAlias("h", expression("h"))
                                                .withAlias("a_times_5", expression("a * 5"))
                                                .withAlias("b_times_5", expression("b * 5"))
                                                .withAlias("h_times_5", expression("h * 5")))
                        ).withNumberOfOutputColumns(3)
                                .withExactOutputs("a_times_5", "b_times_5", "h_times_5"));
    }

    @Test
    public void testOrderingColumnsArePreserved()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression h = p.variable("h");
                    VariableReferenceExpression aTimes5 = p.variable("a_times_5");
                    VariableReferenceExpression bTimes5 = p.variable("b_times_5");
                    VariableReferenceExpression hTimes5 = p.variable("h_times_5");
                    VariableReferenceExpression sortVariable = p.variable("sortVariable");
                    OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(new Ordering(sortVariable, SortOrder.ASC_NULLS_FIRST)));
                    return p.project(
                            Assignments.builder()
                                    .put(aTimes5, p.binaryOperation(MULTIPLY, a, constant(5L, BIGINT)))
                                    .put(bTimes5, p.binaryOperation(MULTIPLY, b, constant(5L, BIGINT)))
                                    .put(hTimes5, p.binaryOperation(MULTIPLY, h, constant(5L, BIGINT)))
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(
                                            p.values(a, b, h, sortVariable))
                                    .addInputsSet(a, b, h, sortVariable)
                                    .singleDistributionPartitioningScheme(
                                            ImmutableList.of(a, b, h, sortVariable))
                                    .setEnsureSourceOrdering(true)
                                    .orderingScheme(orderingScheme)));
                })
                .matches(
                        project(
                                exchange(REMOTE_STREAMING, GATHER, ImmutableList.of(sort("sortSymbol", ASCENDING, FIRST)),
                                        project(
                                                values(
                                                        ImmutableList.of("a", "b", "h", "sortSymbol")))
                                                .withNumberOfOutputColumns(4)
                                                .withAlias("a_times_5", expression("a * 5"))
                                                .withAlias("b_times_5", expression("b * 5"))
                                                .withAlias("h_times_5", expression("h * 5"))
                                                .withAlias("sortSymbol", expression("sortSymbol")))
                        ).withNumberOfOutputColumns(3)
                                .withExactOutputs("a_times_5", "b_times_5", "h_times_5"));
    }
}
