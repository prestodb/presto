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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.cost.PlanNodeStatsAssertion.assertThat;
import static com.facebook.presto.cost.SemiJoinStatsCalculator.computeAntiJoin;
import static com.facebook.presto.cost.SemiJoinStatsCalculator.computeSemiJoin;
import static com.facebook.presto.cost.SymbolStatsEstimate.UNKNOWN_STATS;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestSemiJoinStatsCalculator
{
    private PlanNodeStatsEstimate inputStatistics;
    private SymbolStatsEstimate uStats;
    private SymbolStatsEstimate wStats;
    private SymbolStatsEstimate xStats;
    private SymbolStatsEstimate yStats;
    private SymbolStatsEstimate zStats;
    private SymbolStatsEstimate leftOpenStats;
    private SymbolStatsEstimate rightOpenStats;
    private SymbolStatsEstimate unknownRangeStats;
    private SymbolStatsEstimate emptyRangeStats;

    private Symbol u = new Symbol("u");
    private Symbol w = new Symbol("w");
    private Symbol x = new Symbol("x");
    private Symbol y = new Symbol("y");
    private Symbol z = new Symbol("z");
    private Symbol leftOpen = new Symbol("leftOpen");
    private Symbol rightOpen = new Symbol("rightOpen");
    private Symbol unknownRange = new Symbol("unknownRange");
    private Symbol emptyRange = new Symbol("emptyRange");
    private Symbol unknown = new Symbol("unknown");

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        uStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(300)
                .setLowValue(0)
                .setHighValue(20)
                .setNullsFraction(0.1)
                .build();
        wStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(30)
                .setLowValue(0)
                .setHighValue(20)
                .setNullsFraction(0.1)
                .build();
        xStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(40.0)
                .setLowValue(-10.0)
                .setHighValue(10.0)
                .setNullsFraction(0.25)
                .build();
        yStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(20.0)
                .setLowValue(0.0)
                .setHighValue(5.0)
                .setNullsFraction(0.5)
                .build();
        zStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(5.0)
                .setLowValue(-100.0)
                .setHighValue(100.0)
                .setNullsFraction(0.1)
                .build();
        leftOpenStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(15.0)
                .setNullsFraction(0.1)
                .build();
        rightOpenStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(-15.0)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        unknownRangeStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        emptyRangeStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(0.0)
                .setLowValue(NaN)
                .setHighValue(NaN)
                .setNullsFraction(NaN)
                .build();
        inputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(u, uStats)
                .addSymbolStatistics(w, wStats)
                .addSymbolStatistics(x, xStats)
                .addSymbolStatistics(y, yStats)
                .addSymbolStatistics(z, zStats)
                .addSymbolStatistics(leftOpen, leftOpenStats)
                .addSymbolStatistics(rightOpen, rightOpenStats)
                .addSymbolStatistics(unknownRange, unknownRangeStats)
                .addSymbolStatistics(emptyRange, emptyRangeStats)
                .addSymbolStatistics(unknown, UNKNOWN_STATS)
                .setOutputRowCount(1000.0)
                .build();
    }

    @Test
    public void testSemiJoin()
    {
        // overlapping ranges
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, x, w))
                .symbolStats(x, stats -> stats
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .nullsFraction(0)
                        .distinctValuesCount(wStats.getDistinctValuesCount()))
                .symbolStats(w, stats -> stats.isEqualTo(wStats))
                .symbolStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCount(inputStatistics.getOutputRowCount() * xStats.getValuesFraction() * (wStats.getDistinctValuesCount() / xStats.getDistinctValuesCount()));

        // overlapping ranges, nothing filtered out
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, x, u))
                .symbolStats(x, stats -> stats
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .nullsFraction(0)
                        .distinctValuesCount(xStats.getDistinctValuesCount()))
                .symbolStats(u, stats -> stats.isEqualTo(uStats))
                .symbolStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCount(inputStatistics.getOutputRowCount() * xStats.getValuesFraction());

        // source stats are unknown
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, unknown, u))
                .symbolStats(unknown, stats -> stats
                        .nullsFraction(0)
                        .distinctValuesCountUnknown()
                        .unknownRange())
                .symbolStats(u, stats -> stats.isEqualTo(uStats))
                .symbolStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCountUnknown();

        // filtering stats are unknown
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, x, unknown))
                .symbolStats(x, stats -> stats
                        .nullsFraction(0)
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .distinctValuesCountUnknown())
                .symbolStatsUnknown(unknown)
                .symbolStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCountUnknown();
    }

    @Test
    public void testAntiJoin()
    {
        // overlapping ranges
        assertThat(computeAntiJoin(inputStatistics, inputStatistics, u, x))
                .symbolStats(u, stats -> stats
                        .lowValue(uStats.getLowValue())
                        .highValue(uStats.getHighValue())
                        .nullsFraction(0)
                        .distinctValuesCount(uStats.getDistinctValuesCount() - xStats.getDistinctValuesCount()))
                .symbolStats(x, stats -> stats.isEqualTo(xStats))
                .symbolStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCount(inputStatistics.getOutputRowCount() * uStats.getValuesFraction() * (1 - xStats.getDistinctValuesCount() / uStats.getDistinctValuesCount()));

        // overlapping ranges, everything filtered out (but we leave 0.5 due to safety coeeficient)
        assertThat(computeAntiJoin(inputStatistics, inputStatistics, x, u))
                .symbolStats(x, stats -> stats
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .nullsFraction(0)
                        .distinctValuesCount(xStats.getDistinctValuesCount() * 0.5))
                .symbolStats(u, stats -> stats.isEqualTo(uStats))
                .symbolStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCount(inputStatistics.getOutputRowCount() * xStats.getValuesFraction() * 0.5);

        // source stats are unknown
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, unknown, u))
                .symbolStats(unknown, stats -> stats
                        .nullsFraction(0)
                        .distinctValuesCountUnknown()
                        .unknownRange())
                .symbolStats(u, stats -> stats.isEqualTo(uStats))
                .symbolStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCountUnknown();

        // filtering stats are unknown
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, x, unknown))
                .symbolStats(x, stats -> stats
                        .nullsFraction(0)
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .distinctValuesCountUnknown())
                .symbolStatsUnknown(unknown)
                .symbolStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCountUnknown();
    }
}
