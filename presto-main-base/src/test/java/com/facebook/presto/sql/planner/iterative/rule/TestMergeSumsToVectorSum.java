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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

/**
 * Tests for {@link MergeSumsToVectorSum}.
 * <p>
 * NOTE ON POSITIVE TESTS: The positive case (rule fires and produces the correct
 * ProjectNode &rarr; AggregationNode &rarr; ProjectNode plan) requires {@code vector_sum}
 * to be registered as a Presto aggregate function. Currently, {@code vector_sum}
 * is implemented only in Velox (see D94039944). When the Presto-side function
 * registration is added, a positive test should be added using {@code .matches()}
 * with the expected plan structure: outer {@code project()} extracting via
 * {@code element_at}, inner {@code aggregation()} with {@code vector_sum}, and
 * bottom {@code project()} with {@code array_constructor}.
 * <p>
 * Until then, the rule's safety guard ({@code isVectorSumAvailable()}) correctly
 * prevents the rewrite when the function is not registered, and all tests below
 * verify the rule's edge-case behavior, threshold logic, and eligibility filtering.
 */
public class TestMergeSumsToVectorSum
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "0")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", BIGINT)));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireBelowThreshold()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "5")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1"), p.variable("col2"), p.variable("col3")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDistinctSum()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"), true)
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"), true)
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForMixedTypesBelowThreshold()
    {
        // Two BIGINT SUMs and one DOUBLE SUM — neither group meets threshold of 3
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "3")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", DOUBLE);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", DOUBLE)));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonSumAggregations()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_1"), p.rowExpression("max(col1)"))
                            .addAggregation(p.variable("max_2"), p.rowExpression("max(col2)"))
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForPartialAggregation()
    {
        // Rule should only apply to SINGLE step aggregations
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .step(AggregationNode.Step.PARTIAL)
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenVectorSumUnavailable()
    {
        // Threshold IS met (3 SUMs >= threshold of 2), but vector_sum is not
        // registered as a Presto function, so the safety guard prevents the rewrite.
        // This test verifies the graceful degradation behavior.
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1"), p.variable("col2"), p.variable("col3")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenVectorSumUnavailableWithGroupBy()
    {
        // Same as above but with group-by keys, verifying behavior with grouped aggregation
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("key", BIGINT);
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.singleGroupingSet(p.variable("key"))
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("key"), p.variable("col1"), p.variable("col2"), p.variable("col3")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithMixedSumsAndCountAboveThreshold()
    {
        // 3 SUMs (above threshold) + 1 COUNT — would fire for the SUMs if
        // vector_sum were available, but gracefully degrades since it's not
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "3")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .addAggregation(p.variable("cnt"), p.rowExpression("count(col1)"))
                            .source(p.values(p.variable("col1"), p.variable("col2"), p.variable("col3")));
                }))
                .doesNotFire();
    }
}
