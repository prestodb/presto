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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import org.testng.annotations.Test;

import static com.facebook.presto.cost.SymbolStatsEstimate.ZERO_STATS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class TestIntersectStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testIntersectWhenRangesAreOverlapping()
    {
        tester().assertStatsFor(pb -> pb
                .intersect(
                        ImmutableList.<PlanNode>of(
                                pb.values(pb.symbol("i11", BIGINT)),
                                pb.values(pb.symbol("i21", BIGINT))),
                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                .putAll(pb.symbol("o1", BIGINT), pb.symbol("i11", BIGINT), pb.symbol("i21", BIGINT))
                                .build(),
                        ImmutableList.of(pb.symbol("o1", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i11"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(10)
                                .setDistinctValuesCount(8)
                                .setNullsFraction(0.2)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("i21"), SymbolStatsEstimate.builder()
                                .setLowValue(5)
                                .setHighValue(15)
                                .setNullsFraction(0.4)
                                .setDistinctValuesCount(4)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(5)
                        .symbolStats("o1", assertion -> assertion
                                .lowValue(5)
                                .highValue(10)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0.2)));
    }

    @Test
    public void testIntersectWhenRangesAreSeparated()
    {
        tester().assertStatsFor(pb -> pb
                .intersect(
                        ImmutableList.<PlanNode>of(
                                pb.values(pb.symbol("i11", BIGINT)),
                                pb.values(pb.symbol("i21", BIGINT))),
                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                .putAll(pb.symbol("o1", BIGINT), pb.symbol("i11", BIGINT), pb.symbol("i21", BIGINT))
                                .build(),
                        ImmutableList.of(pb.symbol("o1", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i11"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("i21"), SymbolStatsEstimate.builder()
                                .setLowValue(11)
                                .setHighValue(20)
                                .setNullsFraction(0.4)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(1)
                        .symbolStats("o1", assertion -> assertion.isEqualTo(ZERO_STATS)));
    }

    @Test
    public void testIntersectWhenLeftHasUnknownRange()
    {
        tester().assertStatsFor(pb -> pb
                .intersect(
                        ImmutableList.<PlanNode>of(
                                pb.values(pb.symbol("i11", BIGINT)),
                                pb.values(pb.symbol("i21", BIGINT))),
                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                .putAll(pb.symbol("o1", BIGINT), pb.symbol("i11", BIGINT), pb.symbol("i21", BIGINT))
                                .build(),
                        ImmutableList.of(pb.symbol("o1", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i11"), SymbolStatsEstimate.builder()
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("i21"), SymbolStatsEstimate.builder()
                                .setLowValue(11)
                                .setHighValue(20)
                                .setNullsFraction(0.4)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(2.25)
                        .symbolStats("o1", assertion -> assertion
                                .lowValue(11)
                                .highValue(20)
                                .dataSizeUnknown()
                                .distinctValuesCount(1.25)
                                .nullsFraction(0.44444444)));
    }

    @Test
    public void testIntersectWhenRightIsUnknown()
    {
        tester().assertStatsFor(pb -> pb
                .intersect(
                        ImmutableList.<PlanNode>of(
                                pb.values(pb.symbol("i11", BIGINT)),
                                pb.values(pb.symbol("i21", BIGINT))),
                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                .putAll(pb.symbol("o1", BIGINT), pb.symbol("i11", BIGINT), pb.symbol("i21", BIGINT))
                                .build(),
                        ImmutableList.of(pb.symbol("o1", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i11"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("i21"), SymbolStatsEstimate.UNKNOWN_STATS)
                        .build())
                .check(check -> check
                        .outputRowsCount(6)
                        .symbolStats("o1", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(5)
                                .nullsFraction(0.166666667)));
    }

    @Test
    public void testIntersectWhenNullFractionsAreUnknown()
    {
        tester().assertStatsFor(pb -> pb
                .intersect(
                        ImmutableList.<PlanNode>of(
                                pb.values(pb.symbol("i11", BIGINT)),
                                pb.values(pb.symbol("i21", BIGINT))),
                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                .putAll(pb.symbol("o1", BIGINT), pb.symbol("i11", BIGINT), pb.symbol("i21", BIGINT))
                                .build(),
                        ImmutableList.of(pb.symbol("o1", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i11"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(10)
                                .setDistinctValuesCount(8)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("i21"), SymbolStatsEstimate.builder()
                                .setLowValue(5)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(5)
                        .symbolStats("o1", assertion -> assertion
                                .lowValue(5)
                                .highValue(10)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0.2)));
    }

    @Test
    public void testIntersectWithoutNulls()
    {
        tester().assertStatsFor(pb -> pb
                .intersect(
                        ImmutableList.<PlanNode>of(
                                pb.values(pb.symbol("i11", BIGINT)),
                                pb.values(pb.symbol("i21", BIGINT))),
                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                .putAll(pb.symbol("o1", BIGINT), pb.symbol("i11", BIGINT), pb.symbol("i21", BIGINT))
                                .build(),
                        ImmutableList.of(pb.symbol("o1", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i11"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(10)
                                .setDistinctValuesCount(8)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("i21"), SymbolStatsEstimate.builder()
                                .setLowValue(5)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(4)
                        .symbolStats("o1", assertion -> assertion
                                .lowValue(5)
                                .highValue(10)
                                .dataSizeUnknown()
                                // TODO DVC should be 2 as right side has two times lower values density
                                .distinctValuesCount(4)
                                .nullsFraction(0)));
    }
}
