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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestReplaceConditionalApproxDistinct
        extends BaseRuleTest
{
    @Test
    public void testReplaceConditionalConstant()
    {
        tester().assertThat(new ReplaceConditionalApproxDistinct(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression original = p.variable("original", BOOLEAN);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    return p.aggregation((builder) -> builder
                                .addAggregation(
                                        p.variable("output"),
                                        p.rowExpression("approx_distinct(original)"))
                                .globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .source(
                                    p.project(
                                        p.assignment(
                                            original, p.rowExpression("if(a > b, 'constant')")),
                                        p.values(a, b))));
                })
                .matches(
                    project(
                        ImmutableMap.of(
                            "output", expression("coalesce(intermediate, 0)")),
                        aggregation(
                            ImmutableMap.of("intermediate",
                                functionCall("arbitrary", ImmutableList.of("expression"))),
                            SINGLE,
                            project(
                                ImmutableMap.of(
                                    "original", expression("if(a > b, 'constant')"),
                                    "expression", expression("if(a > b, 1, NULL)")),
                                values("a", "b")))));
    }

    @Test
    public void testReplaceConditionalErrorBounds()
    {
        tester().assertThat(new ReplaceConditionalApproxDistinct(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression original = p.variable("original", BOOLEAN);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression bounds = p.variable("bounds", DOUBLE);
                    return p.aggregation((builder) -> builder
                                .addAggregation(
                                        p.variable("output"),
                                        p.rowExpression("approx_distinct(original, bounds)"))
                                .globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .source(
                                    p.project(
                                        p.assignment(
                                            original, p.rowExpression("if(a > b, 'constant')"),
                                            bounds, p.rowExpression("0.0040625", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE)),
                                        p.values(a, b))));
                })
                .matches(
                    project(
                        ImmutableMap.of(
                            "output", expression("coalesce(intermediate, 0)")),
                        aggregation(
                            ImmutableMap.of("intermediate",
                                functionCall("arbitrary", ImmutableList.of("expression"))),
                            SINGLE,
                            project(
                                ImmutableMap.of(
                                    "original", expression("if(a > b, 'constant')"),
                                    "expression", expression("if(a > b, 1, NULL)")),
                                values("a", "b")))));
    }

    @Test
    public void testReplaceMultipleConditionalConstant()
    {
        tester().assertThat(new ReplaceConditionalApproxDistinct(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression original1 = p.variable("original1", BOOLEAN);
                    VariableReferenceExpression original2 = p.variable("original2", BOOLEAN);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    return p.aggregation((builder) -> builder
                                .addAggregation(
                                        p.variable("output1"),
                                        p.rowExpression("approx_distinct(original1)"))
                                .addAggregation(
                                        p.variable("output2"),
                                        p.rowExpression("approx_distinct(original2)"))
                                .globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .source(
                                    p.project(
                                        p.assignment(
                                            original1, p.rowExpression("if(a > b, 'constant')"),
                                            original2, p.rowExpression("if(a < b, NULL, 'constant')")),
                                        p.values(a, b))));
                })
                .matches(
                    project(
                        ImmutableMap.of(
                            "output1", expression("coalesce(intermediate1, 0)"),
                            "output2", expression("coalesce(intermediate2, 0)")),
                        aggregation(
                            ImmutableMap.of(
                                "intermediate1", functionCall("arbitrary", ImmutableList.of("expression1")),
                                "intermediate2", functionCall("arbitrary", ImmutableList.of("expression2"))),
                            SINGLE,
                            project(
                                ImmutableMap.of(
                                    "original1", expression("if(a > b, 'constant')"),
                                    "original2", expression("if(a < b, NULL, 'constant')"),
                                    "expression1", expression("if(a > b, 1, NULL)"),
                                    "expression2", expression("if(a < b, NULL, 1)")),
                                values("a", "b")))));
    }

    @Test
    public void testDontReplaceConstant()
    {
        tester().assertThat(new ReplaceConditionalApproxDistinct(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", VARCHAR);
                    return p.aggregation((builder) -> builder
                                .addAggregation(
                                        p.variable("output"),
                                        p.rowExpression("approx_distinct(input)"))
                                .globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .source(
                                    p.project(
                                        p.assignment(input, p.rowExpression("'constant'")),
                                        p.values())));
                }).doesNotFire();
    }

    @Test
    public void testDontReplaceVariable()
    {
        tester().assertThat(new ReplaceConditionalApproxDistinct(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression input = p.variable("input", VARCHAR);
                    VariableReferenceExpression nonconstant = p.variable("nonconstant", VARCHAR);
                    return p.aggregation((builder) -> builder
                                .addAggregation(
                                        p.variable("output"),
                                        p.rowExpression("approx_distinct(input)"))
                                .globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .source(
                                    p.project(
                                        p.assignment(input, p.rowExpression("nonconstant")),
                                        p.values(nonconstant))));
                }).doesNotFire();
    }

    @Test
    public void testDontReplaceConditionalVariable()
    {
        tester().assertThat(new ReplaceConditionalApproxDistinct(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression original = p.variable("original", BOOLEAN);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression nonconstant = p.variable("nonconstant", BIGINT);
                    return p.aggregation((builder) -> builder
                                .addAggregation(
                                        p.variable("output"),
                                        p.rowExpression("approx_distinct(original)"))
                                .globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .source(
                                    p.project(
                                        p.assignment(
                                            original, p.rowExpression("if(a > b, nonconstant)")),
                                        p.values(a, b, nonconstant))));
                }).doesNotFire();
    }
}
