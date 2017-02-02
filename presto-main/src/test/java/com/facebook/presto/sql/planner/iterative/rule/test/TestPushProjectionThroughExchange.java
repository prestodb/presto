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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughExchange;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushProjectionThroughExchange
{
    private final RuleTester tester = new RuleTester();

    @Test
    public void testDoesNotFireNoExchange()
            throws Exception
    {
        tester.assertThat(new PushProjectionThroughExchange())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x", BIGINT), new LongLiteral("3")),
                                p.values(p.symbol("a", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireNarrowingProjection()
            throws Exception
    {
        tester.assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    Symbol c = p.symbol("c", BIGINT);

                    return p.project(
                            Assignments.builder()
                                    .put(a, a.toSymbolReference())
                                    .put(b, b.toSymbolReference())
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
            throws Exception
    {
        tester.assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    Symbol c = p.symbol("c", BIGINT);
                    Symbol c2 = p.symbol("c2", BIGINT);
                    Symbol x = p.symbol("x", BIGINT);
                    return p.project(
                            Assignments.of(
                                    x, new LongLiteral("3"),
                                    c2, new SymbolReference("c")
                            ),
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
                                        values(ImmutableList.of("a"))
                                )
                                        .withAlias("x1", expression("3")),
                                project(
                                        values(ImmutableList.of("b"))
                                )
                                        .withAlias("x2", expression("3"))
                        )
                                // verify that data originally on symbols aliased as x1 and x2 is part of exchange output
                                .withAlias("x1")
                                .withAlias("x2"));
    }

    @Test
    public void testPartitioningColumnAndHashWithoutIdentityMappingInProjection()
            throws Exception
    {
        tester.assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    Symbol h = p.symbol("h", BIGINT);
                    Symbol aTimes5 = p.symbol("a_times_5", BIGINT);
                    Symbol bTimes5 = p.symbol("b_times_5", BIGINT);
                    Symbol hTimes5 = p.symbol("h_times_5", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(aTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.MULTIPLY, new SymbolReference("a"), new LongLiteral("5")))
                                    .put(bTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.MULTIPLY, new SymbolReference("b"), new LongLiteral("5")))
                                    .put(hTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.MULTIPLY, new SymbolReference("h"), new LongLiteral("5")))
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
                                                        ImmutableList.of("a", "b", "h")
                                                )
                                        ).withNumberOfOutputColumns(5)
                                                .withAlias("b", expression("b"))
                                                .withAlias("h", expression("h"))
                                                .withAlias("a_times_5", expression("a * 5"))
                                                .withAlias("b_times_5", expression("b * 5"))
                                                .withAlias("h_times_5", expression("h * 5"))
                                )
                        ).withNumberOfOutputColumns(3)
                                .withExactOutputs("a_times_5", "b_times_5", "h_times_5")
                );
    }
}
