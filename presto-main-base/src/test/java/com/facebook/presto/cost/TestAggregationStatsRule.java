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
package com.facebook.presto.cost;

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.FACT;

public class TestAggregationStatsRule
        extends BaseStatsCalculatorTest
{
    private static final VariableReferenceExpression VARIABLE_X = new VariableReferenceExpression(Optional.empty(), "x", BIGINT);
    private static final VariableReferenceExpression VARIABLE_Y = new VariableReferenceExpression(Optional.empty(), "y", BIGINT);
    private static final VariableReferenceExpression VARIABLE_Z = new VariableReferenceExpression(Optional.empty(), "z", BIGINT);

    @Test
    public void testAggregationWhenAllStatisticsAreKnown()
    {
        Consumer<PlanNodeStatsAssertion> outputRowCountAndZStatsAreCalculated = check -> check
                .outputRowsCount(15)
                .variableStats(VARIABLE_Z, symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(10)
                        .highValue(15)
                        .distinctValuesCount(4)
                        .nullsFraction(0.2))
                .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(0)
                        .highValue(3)
                        .distinctValuesCount(3)
                        .nullsFraction(0));

        testAggregation(
                VariableStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .build())
                .check(outputRowCountAndZStatsAreCalculated);

        testAggregation(
                VariableStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .build())
                .check(outputRowCountAndZStatsAreCalculated);

        Consumer<PlanNodeStatsAssertion> outputRowsCountAndZStatsAreNotFullyCalculated = check -> check
                .outputRowsCountUnknown()
                .variableStats(VARIABLE_Z, symbolStatsAssertion -> symbolStatsAssertion
                        .unknownRange()
                        .distinctValuesCountUnknown()
                        .nullsFractionUnknown())
                .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                        .unknownRange()
                        .nullsFractionUnknown()
                        .distinctValuesCountUnknown());

        testAggregation(
                VariableStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setNullsFraction(0.1)
                        .build())
                .check(outputRowsCountAndZStatsAreNotFullyCalculated);

        testAggregation(
                VariableStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .build())
                .check(outputRowsCountAndZStatsAreNotFullyCalculated);
    }

    private StatsCalculatorAssertion testAggregation(VariableStatsEstimate zStats)
    {
        return tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .addAggregation(pb.variable("count_on_x", BIGINT), pb.rowExpression("count(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT), pb.variable("z", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT), pb.variable("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Z, zStats)
                        .build())
                .check(check -> check
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "sum", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "count", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "count_on_x", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .variableStats(VARIABLE_X, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()));
    }

    @Test
    public void testAggregationStatsCappedToInputRows()
    {
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count_on_x", BIGINT), pb.rowExpression("count(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT), pb.variable("z", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT), pb.variable("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .addVariableStatistics(VARIABLE_Z, VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(check -> check.outputRowsCount(100));
    }

    /**
     * Verifies that a global aggregation (no grouping keys) always produces
     * exactly one output row with FACT confidence level, regardless of the
     * input statistics.
     */
    @Test
    public void testGlobalAggregationReturnsOneRow()
    {
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .globalGrouping()
                        .source(pb.values(pb.variable("x", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(100)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(1)
                        .confident(FACT)
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "sum", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "count", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()));
    }

    /**
     * Verifies that a global aggregation with zero input rows still produces
     * exactly one output row with FACT confidence. This is the expected behavior
     * for queries like {@code SELECT count(*) FROM empty_table}.
     */
    @Test
    public void testGlobalAggregationWithZeroInputRows()
    {
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .globalGrouping()
                        .source(pb.values(pb.variable("x", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(0)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setDistinctValuesCount(0)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(1)
                        .confident(FACT));
    }

    /**
     * Verifies that a PARTIAL aggregation step does not reduce the estimated
     * row count. The rule pessimistically assumes no reduction for partial
     * aggregations and forwards the source row count directly.
     */
    @Test
    public void testPartialAggregationPreservesSourceRowCount()
    {
        double sourceRowCount = 500;
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .step(AggregationNode.Step.PARTIAL)
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(sourceRowCount)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(sourceRowCount)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(3)
                                .distinctValuesCount(3)
                                .nullsFraction(0)));
    }

    /**
     * Verifies that an INTERMEDIATE aggregation step behaves identically to a
     * PARTIAL step: no reduction in estimated row count, source stats forwarded.
     */
    @Test
    public void testIntermediateAggregationPreservesSourceRowCount()
    {
        double sourceRowCount = 500;
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .step(AggregationNode.Step.INTERMEDIATE)
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(sourceRowCount)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(sourceRowCount)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(3)
                                .distinctValuesCount(3)
                                .nullsFraction(0)));
    }

    /**
     * Verifies that for a SINGLE-step aggregation with a single grouping key,
     * the output row count equals the distinct value count of the grouping key.
     * Also verifies that the grouping key's nulls fraction is set to zero when
     * the source has no nulls in that column.
     */
    @Test
    public void testSingleGroupingKeyNoNulls()
    {
        // y has 10 distinct values with no nulls, source has 200 rows
        // Expected output: 10 rows (= NDV of y)
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(200)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(50)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(9)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(10)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(9)
                                .distinctValuesCount(10)
                                .nullsFraction(0)));
    }

    /**
     * Verifies that when a grouping key has a non-zero nulls fraction, the
     * output row count accounts for the null group (NDV + 1 for the null row).
     * Also checks that the resulting nulls fraction for the grouping key is
     * adjusted to {@code 1 / (NDV + 1)}.
     */
    @Test
    public void testSingleGroupingKeyWithNulls()
    {
        // y has 10 distinct values with 20% nulls, source has 200 rows
        // Expected output: 10 + 1 = 11 rows (NDV + 1 for null group)
        // Expected nulls fraction: 1 / (10 + 1) = 1/11
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(200)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(50)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(9)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0.2)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(11)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(9)
                                .distinctValuesCount(10)
                                .nullsFraction(1.0 / 11)));
    }

    /**
     * Verifies the row count estimate for multiple grouping keys with nulls.
     * The output row count is the product of (NDV + null_row) for each key,
     * capped at the source row count. The nulls fractions of grouping keys
     * are each adjusted to {@code 1 / (NDV + 1)}.
     */
    @Test
    public void testMultipleGroupingKeysWithNulls()
    {
        // y: NDV=3, nullsFraction=0.1 -> contributes 3+1=4
        // z: NDV=5, nullsFraction=0.2 -> contributes 5+1=6
        // Product = 4 * 6 = 24, source has 200 rows, so 24 is used
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT), pb.variable("z", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT), pb.variable("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(200)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(2)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(VARIABLE_Z, VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(14)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.2)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(24)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(2)
                                .distinctValuesCount(3)
                                .nullsFraction(1.0 / 4))
                        .variableStats(VARIABLE_Z, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(10)
                                .highValue(14)
                                .distinctValuesCount(5)
                                .nullsFraction(1.0 / 6)));
    }

    /**
     * Verifies that when grouping key statistics are completely unknown
     * (all NaN), the output row count estimate is also unknown. This mirrors
     * how join stats handle missing column statistics.
     */
    @Test
    public void testAggregationWithUnknownGroupingKeyStats()
    {
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.unknown())
                        .build())
                .check(check -> check
                        .outputRowsCountUnknown()
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .unknownRange()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()));
    }

    /**
     * Verifies that a FINAL-step aggregation with a single grouping key
     * produces the same estimates as a SINGLE-step aggregation, since both
     * are handled by the same {@code groupBy} code path.
     */
    @Test
    public void testFinalAggregationMatchesSingleStep()
    {
        double sourceRowCount = 500;
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .step(AggregationNode.Step.FINAL)
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(sourceRowCount)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(19)
                                .setDistinctValuesCount(20)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(20)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(19)
                                .distinctValuesCount(20)
                                .nullsFraction(0)));
    }

    /**
     * Verifies that a partial aggregation with a global grouping (no grouping keys)
     * preserves the full source row count, since partial aggregations assume
     * pessimistic (no) reduction.
     */
    @Test
    public void testPartialGlobalAggregationPreservesSourceRows()
    {
        double sourceRowCount = 300;
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .globalGrouping()
                        .step(AggregationNode.Step.PARTIAL)
                        .source(pb.values(pb.variable("x", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(sourceRowCount)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(100)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(sourceRowCount));
    }

    /**
     * Verifies that the aggregation output row count is correctly capped at the
     * source row count when multiple grouping keys with no nulls produce a
     * product of NDVs that exceeds the number of input rows.
     */
    @Test
    public void testMultipleGroupingKeysCappedToInputRows()
    {
        // y: NDV=50, no nulls -> contributes 50
        // z: NDV=50, no nulls -> contributes 50
        // Product = 50 * 50 = 2500, but source has only 100 rows => capped to 100
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT), pb.variable("z", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT), pb.variable("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(49)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Z, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(49)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check.outputRowsCount(100));
    }

    /**
     * Verifies that aggregation statistics are correctly computed for a
     * SINGLE-step aggregation when the grouping key has a high NDV but the
     * source row count is low. The output row count should equal the source
     * row count since NDV cannot exceed it.
     */
    @Test
    public void testGroupingKeyNdvExceedsSourceRows()
    {
        // y: NDV=200, no nulls, but source only has 50 rows => capped to 50
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(50)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(199)
                                .setDistinctValuesCount(200)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check.outputRowsCount(50));
    }
}
