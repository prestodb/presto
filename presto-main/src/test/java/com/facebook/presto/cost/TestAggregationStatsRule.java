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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class TestAggregationStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testAggregationWhenAllStatisticsAreKnown()
    {
        Consumer<PlanNodeStatsAssertion> outputRowCountAndZStatsAreCalculated = check -> check
                .outputRowsCount(15)
                .variableStats(new VariableReferenceExpression(Optional.empty(), "z", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(10)
                        .highValue(15)
                        .distinctValuesCount(4)
                        .nullsFraction(0.2))
                .variableStats(new VariableReferenceExpression(Optional.empty(), "y", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
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
                .variableStats(new VariableReferenceExpression(Optional.empty(), "z", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                        .unknownRange()
                        .distinctValuesCountUnknown()
                        .nullsFractionUnknown())
                .variableStats(new VariableReferenceExpression(Optional.empty(), "y", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
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
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "x", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "y", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "z", BIGINT), zStats)
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
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "x", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
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
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "y", BIGINT), VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "z", BIGINT), VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(check -> check.outputRowsCount(100));
    }
}
