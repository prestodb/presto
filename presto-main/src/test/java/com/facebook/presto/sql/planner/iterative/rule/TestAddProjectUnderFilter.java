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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestAddProjectUnderFilter
        extends BaseRuleTest
{
    @Test
    public void testTriggerForArrayAccess()
    {
        tester().assertThat(new AddProjectUnderFilter(getFunctionManager()))
                .setSystemProperty(REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION, "true")
                .on(p ->
                {
                    VariableReferenceExpression array_input = p.variable("array_input", new ArrayType(BIGINT));
                    VariableReferenceExpression sum = p.variable("sum", BIGINT);
                    return p.project(
                            assignment(sum, p.rowExpression("array_sum(array_input)")),
                            p.filter(
                                    p.rowExpression("array_sum(array_input) > 10"),
                                    p.values(array_input)));
                })
                .matches(project(
                        ImmutableMap.of("sum", expression("sum_exp")),
                        filter(
                                "sum_exp > 10",
                                project(
                                        ImmutableMap.of("sum_exp", expression("array_sum(array_input)")),
                                        values("array_input")))));
    }

    @Test
    public void testTriggerForLambdaAccess()
    {
        tester().assertThat(new AddProjectUnderFilter(getFunctionManager()))
                .setSystemProperty(REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION, "true")
                .on(p ->
                {
                    VariableReferenceExpression array_input = p.variable("array_input", new ArrayType(BIGINT));
                    VariableReferenceExpression result = p.variable("result", BIGINT);
                    return p.project(
                            assignment(result, p.rowExpression("cardinality(filter(array_input, x -> x > 1))")),
                            p.filter(
                                    p.rowExpression("cardinality(filter(array_input, x -> x > 1)) > 2"),
                                    p.values(array_input)));
                })
                .matches(project(
                        ImmutableMap.of("result", expression("result_exp")),
                        filter(
                                "result_exp > 2",
                                project(
                                        ImmutableMap.of("result_exp", expression("cardinality(filter(array_input, x -> x > 1))")),
                                        values("array_input")))));
    }

    @Test
    public void testNotTriggerForNonDeterministic()
    {
        tester().assertThat(new AddProjectUnderFilter(getFunctionManager()))
                .setSystemProperty(REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION, "true")
                .on(p ->
                {
                    VariableReferenceExpression array_input = p.variable("array_input", new ArrayType(BIGINT));
                    VariableReferenceExpression result = p.variable("result", BIGINT);
                    return p.project(
                            assignment(result, p.rowExpression("cardinality(filter(array_input, x -> x > random()))")),
                            p.filter(
                                    p.rowExpression("cardinality(filter(array_input, x -> x > random())) > 2"),
                                    p.values(array_input)));
                }).doesNotFire();
    }
}
