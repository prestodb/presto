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

import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.ExpressionMatcher;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestRewriteAggregationIfToFilter
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireForNonIf()
    {
        // The aggregation expression is not an if expression.
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BooleanType.BOOLEAN);
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr"), p.rowExpression("count(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("ds > '2021-07-01'")),
                                    p.values(ds))));
                }).doesNotFire();
    }

    @Test
    public void testDoesNotFireForIfWithElse()
    {
        // The if expression has an else branch. We cannot rewrite it.
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr"), p.rowExpression("count(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-07-01', 1, 2)")),
                                    p.values(ds))));
                }).doesNotFire();
    }

    @Test
    public void testFireOneAggregation()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr"), p.rowExpression("count(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-07-01', 1)")),
                                    p.values(ds))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("expr"), functionCall("count", ImmutableList.of("expr_0"))),
                                ImmutableMap.of(new Symbol("expr"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(ImmutableMap.of(
                                                "a", expression("IF(ds > '2021-07-01', 1)"),
                                                "expr_0", expression("1"),
                                                "greater_than", expression("ds > '2021-07-01'")),
                                                values("ds")))));
    }

    @Test
    public void testFireTwoAggregations()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("count(a)"))
                            .addAggregation(p.variable("expr1"), p.rowExpression("count(b)"))
                            .source(p.project(
                                    assignment(
                                            a, p.rowExpression("IF(ds > '2021-07-01', 1)"),
                                            b, p.rowExpression("IF(ds > '2021-06-01', 2)")),
                                    p.values(ds))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("expr0"), functionCall("count", ImmutableList.of("expr")),
                                        Optional.of("expr1"), functionCall("count", ImmutableList.of("expr_1"))),
                                ImmutableMap.of(
                                        new Symbol("expr0"), new Symbol("greater_than"),
                                        new Symbol("expr1"), new Symbol("greater_than_0")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than or greater_than_0",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("a", expression("IF(ds > '2021-07-01', 1)"))
                                                        .put("b", expression("IF(ds > '2021-06-01', 2)"))
                                                        .put("expr", expression("1"))
                                                        .put("expr_1", expression("2"))
                                                        .put("greater_than", expression("ds > '2021-07-01'"))
                                                        .put("greater_than_0", expression("ds > '2021-06-01'"))
                                                        .build(),
                                                values("ds")))));
    }

    @Test
    public void testFireTwoAggregationsWithSharedInput()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression column0 = p.variable("column0", BIGINT);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("MIN(a)"))
                            .addAggregation(p.variable("expr1"), p.rowExpression("MAX(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-06-01', column0)")),
                                    p.values(ds, column0))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("expr0"), functionCall("min", ImmutableList.of("expr")),
                                        Optional.of("expr1"), functionCall("max", ImmutableList.of("expr"))),
                                ImmutableMap.of(
                                        new Symbol("expr0"), new Symbol("greater_than"),
                                        new Symbol("expr1"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("a", expression("IF(ds > '2021-06-01', column0)"))
                                                        .put("expr", expression("column0"))
                                                        .put("greater_than", expression("ds > '2021-06-01'"))
                                                        .build(),
                                                values("ds", "column0")))));
    }

    @Test
    public void testFireForOneOfTwoAggregations()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("count(a)"))
                            .addAggregation(p.variable("expr1"), p.rowExpression("count(b)"))
                            .source(p.project(
                                    assignment(
                                            a, p.rowExpression("IF(ds > '2021-07-01', 1)"),
                                            b, p.rowExpression("ds")),
                                    p.values(ds))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("expr0"), functionCall("count", ImmutableList.of("expr")),
                                        Optional.of("expr1"), functionCall("count", ImmutableList.of("b"))),
                                ImmutableMap.of(new Symbol("expr0"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "true",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("a", expression("IF(ds > '2021-07-01', 1)"))
                                                        .put("b", expression("ds"))
                                                        .put("greater_than", expression("ds > '2021-07-01'"))
                                                        .put("expr", expression("1"))
                                                        .build(),
                                                values("ds")))));
    }
}
