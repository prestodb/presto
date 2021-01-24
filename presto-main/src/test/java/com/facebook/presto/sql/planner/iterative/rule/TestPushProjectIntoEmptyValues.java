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

import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.spi.relation.ConstantExpression.createConstantExpression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPushProjectIntoEmptyValues
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new PushConstantProjectIntoEmptyValuesNode())
                .on(p ->
                        p.project(
                                assignment(p.variable("y"), createConstantExpression(createIntsBlock(1), IntegerType.INTEGER)),
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))
                .matches(values(
                        ImmutableList.of("y"),
                        ImmutableList.of(ImmutableList.of(expression("1")))));
    }

    @Test
    public void testDoesNotFireIfValuesNotEmpty()
    {
        tester().assertThat(new PushConstantProjectIntoEmptyValuesNode())
                .on(p ->
                        p.project(
                                assignment(p.variable("y"), createConstantExpression(createIntsBlock(1), IntegerType.INTEGER)),
                                p.values(
                                        5,
                                        p.variable("var"))))
                .doesNotFire();
    }
}
