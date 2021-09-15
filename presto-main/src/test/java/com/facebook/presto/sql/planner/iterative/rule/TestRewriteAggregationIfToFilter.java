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

import static com.facebook.presto.SystemSessionProperties.AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
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
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
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
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
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
    public void testDoesNotFireForNonDeterministicFunction()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", DOUBLE);
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr"), p.rowExpression("sum(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-07-01', random())")),
                                    p.values(ds))));
                }).doesNotFire();
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr"), p.rowExpression("sum(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(random() > DOUBLE '0.1', 1)")),
                                    p.values(ds))));
                }).doesNotFire();
    }

    @Test
    public void testFireCount()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
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
                                ImmutableMap.of(Optional.of("expr"), functionCall("count", ImmutableList.of("a"))),
                                ImmutableMap.of(new Symbol("expr"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(ImmutableMap.of(
                                                "a", expression("IF(ds > '2021-07-01', 1)"),
                                                "greater_than", expression("ds > '2021-07-01'")),
                                                values("ds")))));
    }

    @Test
    public void testUnwrapIf()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "unwrap_if")
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
                                ImmutableMap.of(Optional.of("expr"), functionCall("count", ImmutableList.of("expr0"))),
                                ImmutableMap.of(new Symbol("expr"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(ImmutableMap.of(
                                                "a", expression("IF(ds > '2021-07-01', 1)"),
                                                "greater_than", expression("ds > '2021-07-01'"),
                                                "expr0", expression("1")),
                                                values("ds")))));
    }

    @Test
    public void testFireMin()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression column0 = p.variable("column0", BIGINT);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("MIN(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-06-01', column0)")),
                                    p.values(ds, column0))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("expr0"), functionCall("min", ImmutableList.of("a"))),
                                ImmutableMap.of(new Symbol("expr0"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("a", expression("IF(ds > '2021-06-01', column0)"))
                                                        .put("greater_than", expression("ds > '2021-06-01'"))
                                                        .build(),
                                                values("ds", "column0")))));
    }

    @Test
    public void testFireMax()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression column0 = p.variable("column0", BIGINT);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("MAX(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-06-01', column0)")),
                                    p.values(ds, column0))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("expr0"), functionCall("max", ImmutableList.of("a"))),
                                ImmutableMap.of(new Symbol("expr0"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("a", expression("IF(ds > '2021-06-01', column0)"))
                                                        .put("greater_than", expression("ds > '2021-06-01'"))
                                                        .build(),
                                                values("ds", "column0")))));
    }

    @Test
    public void testFireArbitrary()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression column0 = p.variable("column0", BIGINT);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("ARBITRARY(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-06-01', column0)")),
                                    p.values(ds, column0))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("expr0"), functionCall("arbitrary", ImmutableList.of("a"))),
                                ImmutableMap.of(new Symbol("expr0"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("a", expression("IF(ds > '2021-06-01', column0)"))
                                                        .put("greater_than", expression("ds > '2021-06-01'"))
                                                        .build(),
                                                values("ds", "column0")))));
    }

    @Test
    public void testFireSum()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression column0 = p.variable("column0", BIGINT);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("SUM(a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-06-01', column0)")),
                                    p.values(ds, column0))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("expr0"), functionCall("sum", ImmutableList.of("a"))),
                                ImmutableMap.of(new Symbol("expr0"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("a", expression("IF(ds > '2021-06-01', column0)"))
                                                        .put("greater_than", expression("ds > '2021-06-01'"))
                                                        .build(),
                                                values("ds", "column0")))));
    }

    @Test
    public void testDoesNotFireForMaxBy()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression column0 = p.variable("column0", BIGINT);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("MAX_BY(a, a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-06-01', column0)")),
                                    p.values(ds, column0))));
                }).doesNotFire();
    }

    @Test
    public void testDoesNotFireForMinBy()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression column0 = p.variable("column0", BIGINT);
                    return p.aggregation(ap -> ap.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("MIN_BY(a, a)"))
                            .source(p.project(
                                    assignment(a, p.rowExpression("IF(ds > '2021-06-01', column0)")),
                                    p.values(ds, column0))));
                }).doesNotFire();
    }

    @Test
    public void testFireTwoAggregations()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
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
                                        Optional.of("expr0"), functionCall("count", ImmutableList.of("a")),
                                        Optional.of("expr1"), functionCall("count", ImmutableList.of("b"))),
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
                                                        .put("greater_than", expression("ds > '2021-07-01'"))
                                                        .put("greater_than_0", expression("ds > '2021-06-01'"))
                                                        .build(),
                                                values("ds")))));
    }

    @Test
    public void testFireTwoAggregationsWithSharedInput()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
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
                                        Optional.of("expr0"), functionCall("min", ImmutableList.of("a")),
                                        Optional.of("expr1"), functionCall("max", ImmutableList.of("a"))),
                                ImmutableMap.of(
                                        new Symbol("expr0"), new Symbol("greater_than"),
                                        new Symbol("expr1"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("a", expression("IF(ds > '2021-06-01', column0)"))
                                                        .put("greater_than", expression("ds > '2021-06-01'"))
                                                        .build(),
                                                values("ds", "column0")))));
    }

    @Test
    public void testFireForOneOfTwoAggregations()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if")
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
                                        Optional.of("expr0"), functionCall("count", ImmutableList.of("a")),
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
                                                        .build(),
                                                values("ds")))));
    }

    @Test
    public void testArrayOffset()
    {
        for (String strategy : new String[] {"filter_with_if", "unwrap_if"}) {
            tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                    .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, strategy)
                    .on(p -> {
                        VariableReferenceExpression arrayColumn = p.variable("arrayColumn", new ArrayType(BIGINT));
                        VariableReferenceExpression arrayElement = p.variable("arrayElement", BIGINT);
                        return p.aggregation(aggregationBuilder -> aggregationBuilder.globalGrouping().step(AggregationNode.Step.FINAL)
                                .addAggregation(p.variable("expr0"), p.rowExpression("SUM(arrayElement)"))
                                .source(p.project(
                                        assignment(arrayElement, p.rowExpression("IF(CARDINALITY(arrayColumn) > 0, arrayColumn[1])")),
                                        p.values(arrayColumn))));
                    })
                    .matches(
                            aggregation(
                                    globalAggregation(),
                                    ImmutableMap.of(Optional.of("expr0"), functionCall("SUM", ImmutableList.of("arrayElement"))),
                                    ImmutableMap.of(new Symbol("expr0"), new Symbol("greater_than")),
                                    Optional.empty(),
                                    AggregationNode.Step.FINAL,
                                    filter(
                                            "greater_than",
                                            project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                            .put("arrayElement", expression("IF(CARDINALITY(arrayColumn) > 0, arrayColumn[1])"))
                                                            .put("greater_than", expression("CARDINALITY(arrayColumn) > 0"))
                                                            .build(),
                                                    values("arrayColumn")))));
        }
    }

    @Test
    public void testDivide()
    {
        for (String strategy : new String[] {"filter_with_if", "unwrap_if"}) {
            tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                    .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, strategy)
                    .on(p -> {
                        VariableReferenceExpression a = p.variable("a", BIGINT);
                        VariableReferenceExpression b = p.variable("b", BIGINT);
                        VariableReferenceExpression result = p.variable("result", BIGINT);
                        return p.aggregation(aggregationBuilder -> aggregationBuilder.globalGrouping().step(AggregationNode.Step.FINAL)
                                .addAggregation(p.variable("expr0"), p.rowExpression("SUM(result)"))
                                .source(p.project(
                                        assignment(result, p.rowExpression("IF(b != 0, a / b)")),
                                        p.values(a, b))));
                    })
                    .matches(
                            aggregation(
                                    globalAggregation(),
                                    ImmutableMap.of(Optional.of("expr0"), functionCall("SUM", ImmutableList.of("result"))),
                                    ImmutableMap.of(new Symbol("expr0"), new Symbol("not_equal")),
                                    Optional.empty(),
                                    AggregationNode.Step.FINAL,
                                    filter(
                                            "not_equal",
                                            project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                            .put("result", expression("IF(b != 0, a / b)"))
                                                            .put("not_equal", expression("b != 0"))
                                                            .build(),
                                                    values("a", "b")))));
        }

        // The condition expression doesn't reference the variables in the true branch. The IF can be unwrapped.
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "unwrap_if")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression ds = p.variable("ds", VARCHAR);
                    VariableReferenceExpression result = p.variable("result", BIGINT);
                    return p.aggregation(aggregationBuilder -> aggregationBuilder.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("SUM(result)"))
                            .source(p.project(
                                    assignment(result, p.rowExpression("IF(ds > '2021-07-01', a / b)")),
                                    p.values(ds, a, b))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("expr0"), functionCall("SUM", ImmutableList.of("result"))),
                                ImmutableMap.of(new Symbol("expr0"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("result", expression("a / b"))
                                                        .put("greater_than", expression("ds > '2021-07-01'"))
                                                        .build(),
                                                values("ds", "a", "b")))));
    }

    @Test
    public void testUnwrapIfForOneOfTwoAggregations()
    {
        tester().assertThat(new RewriteAggregationIfToFilter(getFunctionManager()))
                .setSystemProperty(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "unwrap_if")
                .on(p -> {
                    VariableReferenceExpression result0 = p.variable("result0", BIGINT);
                    VariableReferenceExpression result1 = p.variable("result1", BIGINT);
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    return p.aggregation(aggregationBuilder -> aggregationBuilder.globalGrouping().step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("expr0"), p.rowExpression("count(result0)"))
                            .addAggregation(p.variable("expr1"), p.rowExpression("count(result1)"))
                            .source(p.project(
                                    assignment(
                                            result0, p.rowExpression("IF(b != 0, a / b)"),
                                            result1, p.rowExpression("IF(b > 0, b)")),
                                    p.values(a, b))));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("expr0"), functionCall("count", ImmutableList.of("result0")),
                                        Optional.of("expr1"), functionCall("count", ImmutableList.of("b_0"))),
                                ImmutableMap.of(new Symbol("expr0"), new Symbol("not_equal"),
                                        new Symbol("expr1"), new Symbol("greater_than")),
                                Optional.empty(),
                                AggregationNode.Step.FINAL,
                                filter(
                                        "greater_than or not_equal",
                                        project(new ImmutableMap.Builder<String, ExpressionMatcher>()
                                                        .put("result0", expression("IF(b != 0, a / b)"))
                                                        .put("result1", expression("IF(b > 0, b)"))
                                                        .put("b_0", expression("b"))
                                                        .put("not_equal", expression("b != 0"))
                                                        .put("greater_than", expression("b > 0"))
                                                        .build(),
                                                values("a", "b")))));
    }
}
