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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_AGGREGATIONS_OVER_CONSTANT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;

public class TestSimplifyAggregationsOverConstant
        extends BaseRuleTest
{
    @Test
    public void testFoldsMinOverConstant()
    {
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 7L)))));
                })
                .matches(values(ImmutableMap.of("min_1", 0)));
    }

    @Test
    public void testFoldsMaxOverConstant()
    {
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("max_1", BIGINT),
                                    p.rowExpression("max(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 3L)))));
                })
                .matches(values(ImmutableMap.of("max_1", 0)));
    }

    @Test
    public void testFoldsArbitraryOverConstant()
    {
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("arb_1", BIGINT),
                                    p.rowExpression("arbitrary(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 9L)))));
                })
                .matches(values(ImmutableMap.of("arb_1", 0)));
    }

    @Test
    public void testFoldsApproxDistinctOverConstant()
    {
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("approx_1", BIGINT),
                                    p.rowExpression("approx_distinct(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 42L)))));
                })
                .matches(values(ImmutableMap.of("approx_1", 0)));
    }

    @Test
    public void testFoldsMinOverConstantWithNonScalarSource()
    {
        // Simulates: SELECT min(orderkey) FROM orders WHERE orderkey = 7
        // After constant propagation, plan is: Agg[min(x)] -> Project[x := 7] -> Filter -> TableScan
        // MIN of the same constant value over any number of rows is still that value
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.project(
                                    com.facebook.presto.spi.plan.Assignments.builder()
                                            .put(x, p.rowExpression("BIGINT '7'"))
                                            .build(),
                                    p.filter(
                                            p.rowExpression("true"),
                                            p.tableScan(ImmutableList.of(), ImmutableMap.of())))));
                })
                .matches(values(ImmutableMap.of("min_1", 0)));
    }

    @Test
    public void testFoldsMinOverDerivedConstantExpression()
    {
        // Tests that RowExpressionOptimizer resolves non-literal constant expressions
        // e.g., CAST(7 AS BIGINT) is a CallExpression, not a ConstantExpression,
        // but the optimizer can evaluate it to a constant
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.project(
                                    com.facebook.presto.spi.plan.Assignments.builder()
                                            .put(x, p.rowExpression("CAST(7 AS BIGINT)"))
                                            .build(),
                                    p.tableScan(ImmutableList.of(), ImmutableMap.of()))));
                })
                .matches(values(ImmutableMap.of("min_1", 0)));
    }

    @Test
    public void testFoldsWithGroupByOverConstant()
    {
        // MIN(constant) with GROUP BY should remove the aggregation and project constant
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression key = p.variable("key", BIGINT);
                    VariableReferenceExpression val = p.variable("val", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(val)"))
                            .singleGroupingSet(key)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(key, val),
                                    ImmutableList.of(constantExpressions(BIGINT, 1L, 5L)))));
                })
                .matches(project(
                        aggregation(
                                ImmutableMap.of(),
                                values(ImmutableMap.of("key", 0, "val", 1)))));
    }

    @Test
    public void testDoesNotFoldSumOverConstant()
    {
        // SUM depends on row count, should NOT be folded
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_1", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 5L)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFoldCountOverConstant()
    {
        // COUNT depends on row count, should NOT be folded
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("count_1", BIGINT),
                                    p.rowExpression("count(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 5L)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFoldCountStar()
    {
        // COUNT(*) has no arguments, should NOT be folded by this rule
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> p.aggregation(a -> a
                        .addAggregation(
                                p.variable("count_1", BIGINT),
                                p.rowExpression("count()"))
                        .globalGrouping()
                        .step(AggregationNode.Step.SINGLE)
                        .source(p.values(
                                ImmutableList.of(p.variable("x", BIGINT)),
                                ImmutableList.of(constantExpressions(BIGINT, 1L))))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonConstantArgument()
    {
        // MIN over a non-constant variable should not fire
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    p.registerVariable(p.variable("x", BIGINT));
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.tableScan(ImmutableList.of(), ImmutableMap.of())));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnPartialStep()
    {
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.PARTIAL)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 1L)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "false")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 1L)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnFilteredAggregation()
    {
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression filterVar = p.variable("f", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(x)"),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.of(filterVar))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x, filterVar),
                                    ImmutableList.of(constantExpressions(BIGINT, 5L, 1L)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonConstantSourceWithSum()
    {
        // SUM over non-constant source should not fire
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    p.registerVariable(p.variable("x", DOUBLE));
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_1", DOUBLE),
                                    p.rowExpression("sum(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.tableScan(ImmutableList.of(), ImmutableMap.of())));
                })
                .doesNotFire();
    }

    @Test
    public void testFoldsMixedAggregationsPartially()
    {
        // When some aggregations can be folded (MIN) and others cannot (SUM),
        // fold the ones that can and keep the rest
        tester().assertThat(new SimplifyAggregationsOverConstant(getFunctionManager()))
                .setSystemProperty(SIMPLIFY_AGGREGATIONS_OVER_CONSTANT, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("min_1", BIGINT),
                                    p.rowExpression("min(x)"))
                            .addAggregation(
                                    p.variable("sum_1", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(constantExpressions(BIGINT, 5L)))));
                })
                .matches(project(
                        aggregation(
                                ImmutableMap.of("sum_1", com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall("sum", ImmutableList.of("x"))),
                                values(ImmutableMap.of("x", 0)))));
    }
}
