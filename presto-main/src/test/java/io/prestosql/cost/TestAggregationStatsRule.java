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
package io.prestosql.cost;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.Symbol;
import org.testng.annotations.Test;

import java.util.function.Consumer;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestAggregationStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testAggregationWhenAllStatisticsAreKnown()
    {
        Consumer<PlanNodeStatsAssertion> outputRowCountAndZStatsAreCalculated = check -> check
                .outputRowsCount(15)
                .symbolStats("z", symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(10)
                        .highValue(15)
                        .distinctValuesCount(4)
                        .nullsFraction(0.2))
                .symbolStats("y", symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(0)
                        .highValue(3)
                        .distinctValuesCount(3)
                        .nullsFraction(0));

        testAggregation(
                SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .build())
                .check(outputRowCountAndZStatsAreCalculated);

        testAggregation(
                SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .build())
                .check(outputRowCountAndZStatsAreCalculated);

        Consumer<PlanNodeStatsAssertion> outputRowsCountAndZStatsAreNotFullyCalculated = check -> check
                .outputRowsCountUnknown()
                .symbolStats("z", symbolStatsAssertion -> symbolStatsAssertion
                        .unknownRange()
                        .distinctValuesCountUnknown()
                        .nullsFractionUnknown())
                .symbolStats("y", symbolStatsAssertion -> symbolStatsAssertion
                        .unknownRange()
                        .nullsFractionUnknown()
                        .distinctValuesCountUnknown());

        testAggregation(
                SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setNullsFraction(0.1)
                        .build())
                .check(outputRowsCountAndZStatsAreNotFullyCalculated);

        testAggregation(
                SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .build())
                .check(outputRowsCountAndZStatsAreNotFullyCalculated);
    }

    private StatsCalculatorAssertion testAggregation(SymbolStatsEstimate zStats)
    {
        return tester().assertStatsFor(pb -> pb
                .aggregation(ab -> ab
                        .addAggregation(pb.symbol("sum", BIGINT), expression("sum(x)"), ImmutableList.of(BIGINT))
                        .addAggregation(pb.symbol("count", BIGINT), expression("count()"), ImmutableList.of())
                        .addAggregation(pb.symbol("count_on_x", BIGINT), expression("count(x)"), ImmutableList.of(BIGINT))
                        .singleGroupingSet(pb.symbol("y", BIGINT), pb.symbol("z", BIGINT))
                        .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT), pb.symbol("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("x"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addSymbolStatistics(new Symbol("y"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("z"), zStats)
                        .build())
                .check(check -> check
                        .symbolStats("sum", symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .symbolStats("count", symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .symbolStats("count_on_x", symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .symbolStats("x", symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()));
    }

    @Test
    public void testAggregationStatsCappedToInputRows()
    {
        tester().assertStatsFor(pb -> pb
                .aggregation(ab -> ab
                        .addAggregation(pb.symbol("count_on_x", BIGINT), expression("count(x)"), ImmutableList.of(BIGINT))
                        .singleGroupingSet(pb.symbol("y", BIGINT), pb.symbol("z", BIGINT))
                        .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT), pb.symbol("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("y"), SymbolStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .addSymbolStatistics(new Symbol("z"), SymbolStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(check -> check.outputRowsCount(100));
    }
}
