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
import com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughUnion;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushProjectionThroughUnion
{
    private final RuleTester tester = new RuleTester();

    @Test
    public void testDoesNotFire()
            throws Exception
    {
        tester.assertThat(new PushProjectionThroughUnion())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x", BIGINT), new LongLiteral("3")),
                                p.values(p.symbol("a", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void test()
            throws Exception
    {
        tester.assertThat(new PushProjectionThroughUnion())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    Symbol c = p.symbol("c", BIGINT);
                    Symbol cTimes3 = p.symbol("c_times_3", BIGINT);
                    return p.project(
                            Assignments.of(cTimes3, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.MULTIPLY, c.toSymbolReference(), new LongLiteral("3"))),
                            p.union(
                                    ImmutableList.of(
                                            p.values(a),
                                            p.values(b)),
                                    ImmutableListMultimap.<Symbol, Symbol>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .build(),
                                    ImmutableList.of(c)));
                })
                .matches(
                        union(
                                project(
                                        ImmutableMap.of("a_times_3", expression("a * 3")),
                                        values(ImmutableList.of("a"))),
                                project(
                                        ImmutableMap.of("b_times_3", expression("b * 3")),
                                        values(ImmutableList.of("b"))))
                                // verify that data originally on symbols aliased as x1 and x2 is part of exchange output
                                .withNumberOfOutputColumns(1)
                                .withAlias("a_times_3")
                                .withAlias("b_times_3"));
    }
}
