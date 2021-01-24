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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.spi.relation.ConstantExpression.createConstantExpression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestMergeConstantValuesUnderUnion
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new MergeConstantValuesUnderUnion())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.union(
                            ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                    .put(c, a)
                                    .put(c, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(
                                            ImmutableList.of(a),
                                            ImmutableList.of(ImmutableList.of(createConstantExpression(createIntsBlock(1), IntegerType.INTEGER)))),
                                    p.values(
                                            ImmutableList.of(b),
                                            ImmutableList.of(ImmutableList.of(createConstantExpression(createIntsBlock(2), IntegerType.INTEGER))))));
                })
                .matches(values(
                        ImmutableList.of("c"),
                        ImmutableList.of(
                                ImmutableList.of(expression("1")),
                                ImmutableList.of(expression("2")))));
    }
}
