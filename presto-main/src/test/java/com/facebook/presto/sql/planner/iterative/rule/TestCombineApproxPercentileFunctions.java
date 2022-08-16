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

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;

public class TestCombineApproxPercentileFunctions
        extends BaseRuleTest
{
    @Test
    public void testPercentile()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, 0.1)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col, 0.2)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col")));
                }))
                .matches(
                        project(
                                ImmutableMap.of("approx_percentile_1", expression("element_at(approx_percentile_3, 1)"), "approx_percentile_2", expression("element_at(approx_percentile_3, 2)")),
                                aggregation(
                                        ImmutableMap.of("approx_percentile_3", PlanMatchPattern.functionCall("approx_percentile", ImmutableList.of("col", "percentile"))),
                                        project(
                                                ImmutableMap.of("col", expression("col"), "percentile", expression("array[0.1, 0.2]", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE)),
                                                values("col")))));
    }

    @Test
    public void testMultiplePercentile()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, 0.1)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col, 0.2)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_3"), p.rowExpression("approx_percentile(col2, 0.3)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_4"), p.rowExpression("approx_percentile(col2, 0.4)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_5"), p.rowExpression("approx_percentile(col2, 0.5)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col"), p.variable("col2")));
                }))
                .matches(
                        project(
                                ImmutableMap.of("approx_percentile_1", expression("element_at(approx_percentile_6, 1)"), "approx_percentile_2", expression("element_at(approx_percentile_6, 2)"),
                                        "approx_percentile_3", expression("element_at(approx_percentile_7, 1)"), "approx_percentile_4", expression("element_at(approx_percentile_7, 2)"),
                                        "approx_percentile_5", expression("element_at(approx_percentile_7, 3)")),
                                aggregation(
                                        ImmutableMap.of("approx_percentile_6", PlanMatchPattern.functionCall("approx_percentile", ImmutableList.of("col", "percentile")),
                                                "approx_percentile_7", PlanMatchPattern.functionCall("approx_percentile", ImmutableList.of("col2", "percentile2"))),
                                        project(
                                                ImmutableMap.of("col", expression("col"), "col2", expression("col2"), "percentile", expression("array[0.1, 0.2]", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE),
                                                        "percentile2", expression("array[0.3, 0.4, 0.5]", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE)),
                                                values("col", "col2")))));
    }

    @Test
    public void testArrayPercentile()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, array[0.1, 0.3])", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col, array[0.4, 0.5])", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col")));
                })).doesNotFire();
    }

    @Test
    public void testMixedPercentile()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, array[0.1, 0.3])", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col, 0.2)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col")));
                })).doesNotFire();
    }

    @Test
    public void testPercentileAndAccuracy()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    p.variable("accuracy", DOUBLE);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, 0.1, accuracy)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col, 0.2, accuracy)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col"), p.variable("accuracy", DOUBLE)));
                }))
                .matches(
                        project(
                                ImmutableMap.of("approx_percentile_1", expression("element_at(approx_percentile_3, 1)"), "approx_percentile_2", expression("element_at(approx_percentile_3, 2)")),
                                aggregation(
                                        ImmutableMap.of("approx_percentile_3", PlanMatchPattern.functionCall("approx_percentile", ImmutableList.of("col", "percentile", "accuracy"))),
                                        project(
                                                ImmutableMap.of("col", expression("col"), "percentile", expression("array[0.1, 0.2]", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE), "accuracy", expression("accuracy")),
                                                values("col", "accuracy")))));
    }

    @Test
    public void testWeightAndPercentile()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    p.variable("weight", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, weight, 0.1)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col, weight, 0.2)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col"), p.variable("weight", BIGINT)));
                }))
                .matches(
                        project(
                                ImmutableMap.of("approx_percentile_1", expression("element_at(approx_percentile_3, 1)"), "approx_percentile_2", expression("element_at(approx_percentile_3, 2)")),
                                aggregation(
                                        ImmutableMap.of("approx_percentile_3", PlanMatchPattern.functionCall("approx_percentile", ImmutableList.of("col", "weight", "percentile"))),
                                        project(
                                                ImmutableMap.of("col", expression("col"), "percentile", expression("array[0.1, 0.2]", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE), "weight", expression("weight")),
                                                values("col", "weight")))));
    }

    @Test
    public void testWeightPercentileAndAccuracy()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    p.variable("weight", BIGINT);
                    p.variable("accuracy", DOUBLE);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, weight, 0.1, accuracy)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col, weight, 0.2, accuracy)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col"), p.variable("weight", BIGINT), p.variable("accuracy", DOUBLE)));
                }))
                .matches(
                        project(
                                ImmutableMap.of("approx_percentile_1", expression("element_at(approx_percentile_3, 1)"), "approx_percentile_2", expression("element_at(approx_percentile_3, 2)")),
                                aggregation(
                                        ImmutableMap.of("approx_percentile_3", PlanMatchPattern.functionCall("approx_percentile", ImmutableList.of("col", "weight", "percentile", "accuracy"))),
                                        project(
                                                ImmutableMap.of("col", expression("col"), "percentile", expression("array[0.1, 0.2]", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE), "weight", expression("weight"), "accuracy", expression("accuracy")),
                                                values("col", "weight", "accuracy")))));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, 0.1)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col")));
                }))
                .doesNotFire();
    }

    @Test
    public void testMixedWeightAndPercentile()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    p.variable("weight", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, 0.1)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col, weight, 0.2)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col"), p.variable("weight", BIGINT)));
                }))
                .doesNotFire();
    }

    @Test
    public void testNoChange()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("approx_percentile_1"), p.rowExpression("approx_percentile(col, 0.1)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .addAggregation(p.variable("approx_percentile_2"), p.rowExpression("approx_percentile(col2, 0.2)", ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE))
                            .source(p.values(p.variable("col"), p.variable("col2")));
                })).doesNotFire();
    }

    @Test
    public void testArrayLimit()
    {
        tester().assertThat(new CombineApproxPercentileFunctions(getMetadata().getFunctionAndTypeManager()))
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    af.globalGrouping().source(p.values(p.variable("col")));
                    int arraySize = 255;
                    for (int i = 0; i < arraySize; ++i) {
                        double percentile = (i + 1) * 1.0 / (arraySize + 1);
                        String approxVariable = format("approx_percentile_%d", i + 1);
                        String sql = format("approx_percentile(col, %f)", percentile);
                        af.addAggregation(p.variable(approxVariable), p.rowExpression(sql, ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE));
                    }
                })).doesNotFire();
    }
}
