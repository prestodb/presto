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

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.singletonList;

public class TestCombineApproxDistinctFunctions
        extends BaseRuleTest
{
    @BeforeClass
    @Override
    public void setUp()
    {
        tester = new RuleTester(singletonList(new SqlInvokedFunctionsPlugin()));
    }

    @Test
    public void testBasicApproxDistinct()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col = p.variable("col", VARCHAR);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_2"), p.rowExpression("approx_distinct(col)", AS_DOUBLE))
                            .source(p.values(col));
                }))
                .doesNotFire();
    }

    @Test
    public void testTwoDistinctExpressions()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", VARCHAR);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col1)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_2"), p.rowExpression("approx_distinct(col2)", AS_DOUBLE))
                            .source(p.values(col1, col2));
                }))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "approx_distinct_1", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 1)))), CAST(0 AS bigint))"),
                                        "approx_distinct_2", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 2)))), CAST(0 AS bigint))")),
                                project(
                                        ImmutableMap.of("transpose_result", expression("array_transpose(set_agg_result)")),
                                        aggregation(
                                                ImmutableMap.of("set_agg_result", PlanMatchPattern.functionCall("set_agg", ImmutableList.of("array_expr"))),
                                                project(
                                                        ImmutableMap.of("array_expr", expression("array_constructor(col1, col2)")),
                                                        values("col1", "col2"))))));
    }

    @Test
    public void testMultipleSameTypeExpressions()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", VARCHAR);
                    VariableReferenceExpression col3 = p.variable("col3", VARCHAR);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col1)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_2"), p.rowExpression("approx_distinct(col2)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_3"), p.rowExpression("approx_distinct(col3)", AS_DOUBLE))
                            .source(p.values(col1, col2, col3));
                }))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "approx_distinct_1", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 1)))), CAST(0 AS bigint))"),
                                        "approx_distinct_2", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 2)))), CAST(0 AS bigint))"),
                                        "approx_distinct_3", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 3)))), CAST(0 AS bigint))")),
                                project(
                                        ImmutableMap.of("transpose_result", expression("array_transpose(set_agg_result)")),
                                        aggregation(
                                                ImmutableMap.of("set_agg_result", PlanMatchPattern.functionCall("set_agg", ImmutableList.of("array_expr"))),
                                                project(
                                                        ImmutableMap.of("array_expr", expression("array_constructor(col1, col2, col3)")),
                                                        values("col1", "col2", "col3"))))));
    }

    @Test
    public void testMixedTypes()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", VARCHAR);
                    VariableReferenceExpression col3 = p.variable("col3", BIGINT);
                    VariableReferenceExpression col4 = p.variable("col4", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col1)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_2"), p.rowExpression("approx_distinct(col2)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_3"), p.rowExpression("approx_distinct(col3)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_4"), p.rowExpression("approx_distinct(col4)", AS_DOUBLE))
                            .source(p.values(col1, col2, col3, col4));
                }))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "approx_distinct_1", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(varchar_transpose_result, 1)))), CAST(0 AS bigint))"),
                                        "approx_distinct_2", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(varchar_transpose_result, 2)))), CAST(0 AS bigint))"),
                                        "approx_distinct_3", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(bigint_transpose_result, 1)))), CAST(0 AS bigint))"),
                                        "approx_distinct_4", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(bigint_transpose_result, 2)))), CAST(0 AS bigint))")),
                                project(
                                        ImmutableMap.of(
                                                "varchar_transpose_result", expression("array_transpose(varchar_set_agg_result)"),
                                                "bigint_transpose_result", expression("array_transpose(bigint_set_agg_result)")),
                                        aggregation(
                                                ImmutableMap.of(
                                                        "varchar_set_agg_result", PlanMatchPattern.functionCall("set_agg", ImmutableList.of("varchar_array_expr")),
                                                        "bigint_set_agg_result", PlanMatchPattern.functionCall("set_agg", ImmutableList.of("bigint_array_expr"))),
                                                project(
                                                        ImmutableMap.of(
                                                                "varchar_array_expr", expression("array_constructor(col1, col2)"),
                                                                "bigint_array_expr", expression("array_constructor(col3, col4)")),
                                                        values("col1", "col2", "col3", "col4"))))));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col = p.variable("col", VARCHAR);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col)", AS_DOUBLE))
                            .source(p.values(col));
                }))
                .doesNotFire();
    }

    @Test
    public void testDifferentTypes()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col1)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_2"), p.rowExpression("approx_distinct(col2)", AS_DOUBLE))
                            .source(p.values(col1, col2));
                }))
                .doesNotFire();
    }

    @Test
    public void testWithDuplicate()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", VARCHAR);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col1)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_2"), p.rowExpression("approx_distinct(col1)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_3"), p.rowExpression("approx_distinct(col2)", AS_DOUBLE))
                            .source(p.values(col1, col2));
                }))
                .doesNotFire();
    }

    @Test
    public void testWithOtherAggregations()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", VARCHAR);
                    VariableReferenceExpression col3 = p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col1)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_2"), p.rowExpression("approx_distinct(col2)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("count_col3"), p.rowExpression("count(col3)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(col1, col2, col3));
                }))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "approx_distinct_1", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 1)))), CAST(0 AS bigint))"),
                                        "approx_distinct_2", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 2)))), CAST(0 AS bigint))"),
                                        "count_col3", expression("count_col3")),
                                project(
                                        ImmutableMap.of("transpose_result", expression("array_transpose(set_agg_result)")),
                                        aggregation(
                                                ImmutableMap.of(
                                                        "set_agg_result", PlanMatchPattern.functionCall("set_agg", ImmutableList.of("array_expr")),
                                                        "count_col3", PlanMatchPattern.functionCall("count", ImmutableList.of("col3"))),
                                                project(
                                                        ImmutableMap.of("array_expr", expression("array_constructor(col1, col2)")),
                                                        values("col1", "col2", "col3"))))));
    }

    @Test
    public void testTwoArgumentApproxDistinctWithSameError()
    {
        tester().assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(p -> p.aggregation(af -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", VARCHAR);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_distinct_1"), p.rowExpression("approx_distinct(col1, 0.01)", AS_DOUBLE))
                            .addAggregation(p.variable("approx_distinct_2"), p.rowExpression("approx_distinct(col2, 0.01)", AS_DOUBLE))
                            .source(p.values(col1, col2));
                }))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "approx_distinct_1", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 1)))), CAST(0 AS bigint))"),
                                        "approx_distinct_2", expression("coalesce(cardinality(array_distinct(remove_nulls(element_at(transpose_result, 2)))), CAST(0 AS bigint))")),
                                project(
                                        ImmutableMap.of("transpose_result", expression("array_transpose(set_agg_result)")),
                                        aggregation(
                                                ImmutableMap.of("set_agg_result", PlanMatchPattern.functionCall("set_agg", ImmutableList.of("array_expr"))),
                                                project(
                                                        ImmutableMap.of("array_expr", expression("array_constructor(col1, col2)")),
                                                        values("col1", "col2"))))));
    }

    @Test
    public void testTwoArgumentApproxDistinctDifferentTypesWithSameError()
    {
        tester()
                .assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(
                        SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(
                        p ->
                                p.aggregation(
                                        af -> {
                                            VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                                            VariableReferenceExpression col2 = p.variable("col2", BIGINT);
                                            af.globalGrouping()
                                                    .addAggregation(
                                                            p.variable("approx_distinct_1"),
                                                            p.rowExpression("approx_distinct(col1, 0.01)", AS_DOUBLE))
                                                    .addAggregation(
                                                            p.variable("approx_distinct_2"),
                                                            p.rowExpression("approx_distinct(col2, 0.01)", AS_DOUBLE))
                                                    .source(p.values(col1, col2));
                                        }))
                .doesNotFire();
    }

    @Test
    public void testTwoArgumentApproxDistinctWithDuplicates()
    {
        tester()
                .assertThat(new CombineApproxDistinctFunctions(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(
                        SystemSessionProperties.OPTIMIZE_MULTIPLE_APPROX_DISTINCT_ON_SAME_TYPE, "true")
                .on(
                        p ->
                                p.aggregation(
                                        af -> {
                                            VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                                            VariableReferenceExpression col2 = p.variable("col2", VARCHAR);
                                            af.globalGrouping()
                                                    .addAggregation(
                                                            p.variable("approx_distinct_1"),
                                                            p.rowExpression("approx_distinct(col1, 0.01)", AS_DOUBLE))
                                                    .addAggregation(
                                                            p.variable("approx_distinct_2"),
                                                            p.rowExpression("approx_distinct(col1, 0.01)", AS_DOUBLE))
                                                    .addAggregation(
                                                            p.variable("approx_distinct_3"),
                                                            p.rowExpression("approx_distinct(col2, 0.01)", AS_DOUBLE))
                                                    .source(p.values(col1, col2));
                                        }))
                .doesNotFire();
    }
}
