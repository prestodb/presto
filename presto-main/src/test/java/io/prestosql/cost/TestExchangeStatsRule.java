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

import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Collections.emptyList;

public class TestExchangeStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testExchange()
    {
        // test cases origins from TestUnionStatsRule
        // i11, i21 have separated low/high ranges and known all stats, unknown distinct values count
        // i12, i22 have overlapping low/high ranges and known all stats, unknown nulls fraction
        // i13, i23 have some unknown range stats
        // i14, i24 have the same stats

        tester().assertStatsFor(pb -> pb
                .exchange(exchangeBuilder -> exchangeBuilder
                        .addInputsSet(pb.symbol("i11", BIGINT), pb.symbol("i12", BIGINT), pb.symbol("i13", BIGINT), pb.symbol("i14", BIGINT))
                        .addInputsSet(pb.symbol("i21", BIGINT), pb.symbol("i22", BIGINT), pb.symbol("i23", BIGINT), pb.symbol("i24", BIGINT))
                        .fixedHashDistributionParitioningScheme(
                                ImmutableList.of(pb.symbol("o1", BIGINT), pb.symbol("o2", BIGINT), pb.symbol("o3", BIGINT), pb.symbol("o4", BIGINT)),
                                emptyList())
                        .addSource(pb.values(pb.symbol("i11", BIGINT), pb.symbol("i12", BIGINT), pb.symbol("i13", BIGINT), pb.symbol("i14", BIGINT)))
                        .addSource(pb.values(pb.symbol("i21", BIGINT), pb.symbol("i22", BIGINT), pb.symbol("i23", BIGINT), pb.symbol("i24", BIGINT)))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i11"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addSymbolStatistics(new Symbol("i12"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i13"), SymbolStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .addSymbolStatistics(new Symbol("i14"), SymbolStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("i21"), SymbolStatsEstimate.builder()
                                .setLowValue(11)
                                .setHighValue(20)
                                .setNullsFraction(0.4)
                                .build())
                        .addSymbolStatistics(new Symbol("i22"), SymbolStatsEstimate.builder()
                                .setLowValue(2)
                                .setHighValue(7)
                                .setDistinctValuesCount(3)
                                .build())
                        .addSymbolStatistics(new Symbol("i23"), SymbolStatsEstimate.builder()
                                .setDistinctValuesCount(6)
                                .setNullsFraction(0.2)
                                .build())
                        .addSymbolStatistics(new Symbol("i24"), SymbolStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(30)
                        .symbolStats("o1", assertion -> assertion
                                .lowValue(1)
                                .highValue(20)
                                .distinctValuesCountUnknown()
                                .nullsFraction(0.3666666))
                        .symbolStats("o2", assertion -> assertion
                                .lowValue(0)
                                .highValue(7)
                                .distinctValuesCount(4)
                                .nullsFractionUnknown())
                        .symbolStats("o3", assertion -> assertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCount(6)
                                .nullsFraction(0.1666667))
                        .symbolStats("o4", assertion -> assertion
                                .lowValue(10)
                                .highValue(15)
                                .distinctValuesCount(4)
                                .nullsFraction(0.1)));
    }
}
