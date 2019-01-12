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

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;

public class TestRowNumberStatsRule
        extends BaseStatsCalculatorTest
{
    private SymbolStatsEstimate xStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(5.0)
            .setNullsFraction(0)
            .build();
    private SymbolStatsEstimate yStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(5.0)
            .setNullsFraction(0.5)
            .build();

    @Test
    public void testSingleGroupingKey()
    {
        // grouping on a key with 0 nulls fraction without max rows per partition limit
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.symbol("x", BIGINT)),
                        Optional.empty(),
                        pb.symbol("z", BIGINT),
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("x"), xStats)
                        .addSymbolStatistics(new Symbol("y"), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(10)
                        .symbolStats("x", assertion -> assertion.isEqualTo(xStats))
                        .symbolStats("y", assertion -> assertion.isEqualTo(yStats))
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(2)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // grouping on a key with 0 nulls fraction with max rows per partition limit
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.symbol("x", BIGINT)),
                        Optional.of(1),
                        pb.symbol("z", BIGINT),
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("x"), xStats)
                        .addSymbolStatistics(new Symbol("y"), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(5)
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(1)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // grouping on a key with non zero nulls fraction
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.symbol("y", BIGINT)),
                        Optional.empty(),
                        pb.symbol("z", BIGINT),
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(60)
                        .addSymbolStatistics(new Symbol("x"), xStats)
                        .addSymbolStatistics(new Symbol("y"), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(60)
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(10)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // unknown input row count
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.symbol("x", BIGINT)),
                        Optional.of(1),
                        pb.symbol("z", BIGINT),
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .addSymbolStatistics(new Symbol("x"), xStats)
                        .addSymbolStatistics(new Symbol("y"), yStats)
                        .build())
                .check(PlanNodeStatsAssertion::outputRowsCountUnknown);
    }

    @Test
    public void testMultipleGroupingKeys()
    {
        // grouping on multiple keys with the number of estimated groups less than the row count
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT)),
                        Optional.empty(),
                        pb.symbol("z", BIGINT),
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(60)
                        .addSymbolStatistics(new Symbol("x"), xStats)
                        .addSymbolStatistics(new Symbol("y"), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(60)
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(2)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // grouping on multiple keys with the number of estimated groups greater than the row count
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT)),
                        Optional.empty(),
                        pb.symbol("z", BIGINT),
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("x"), xStats)
                        .addSymbolStatistics(new Symbol("y"), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(20)
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(1)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // grouping on multiple keys with stats for one of the keys are unknown
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT)),
                        Optional.empty(),
                        pb.symbol("z", BIGINT),
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("x"), xStats)
                        .addSymbolStatistics(new Symbol("y"), SymbolStatsEstimate.unknown())
                        .build())
                .check(PlanNodeStatsAssertion::outputRowsCountUnknown);
    }
}
