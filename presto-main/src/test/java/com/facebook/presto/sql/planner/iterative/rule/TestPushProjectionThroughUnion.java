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
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.LongLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class TestPushProjectionThroughUnion
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new PushProjectionThroughUnion())
                .on(p ->
                        p.project(
                                assignment(p.variable("x"), new LongLiteral("3")),
                                p.values(p.variable("a"))))
                .doesNotFire();
    }

    @Test
    public void test()
    {
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager());
        tester().assertThat(new PushProjectionThroughUnion())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.project(
                            assignment(
                                    p.variable("c_times_3"),
                                    call("c * 3", functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT), BIGINT, c, constant(3L, BIGINT))),
                            p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(a),
                                            p.values(b))));
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
