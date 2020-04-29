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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.cost.PlanNodeStatsAssertion.assertThat;
import static com.facebook.presto.cost.SemiJoinStatsCalculator.computeAntiJoin;
import static com.facebook.presto.cost.SemiJoinStatsCalculator.computeSemiJoin;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestSemiJoinStatsCalculator
{
    private PlanNodeStatsEstimate inputStatistics;
    private VariableStatsEstimate uStats;
    private VariableStatsEstimate wStats;
    private VariableStatsEstimate xStats;
    private VariableStatsEstimate yStats;
    private VariableStatsEstimate zStats;
    private VariableStatsEstimate leftOpenStats;
    private VariableStatsEstimate rightOpenStats;
    private VariableStatsEstimate unknownRangeStats;
    private VariableStatsEstimate emptyRangeStats;
    private VariableStatsEstimate fractionalNdvStats;

    private VariableReferenceExpression u = new VariableReferenceExpression("u", BIGINT);
    private VariableReferenceExpression w = new VariableReferenceExpression("w", BIGINT);
    private VariableReferenceExpression x = new VariableReferenceExpression("x", BIGINT);
    private VariableReferenceExpression y = new VariableReferenceExpression("y", BIGINT);
    private VariableReferenceExpression z = new VariableReferenceExpression("z", BIGINT);
    private VariableReferenceExpression leftOpen = new VariableReferenceExpression("leftOpen", BIGINT);
    private VariableReferenceExpression rightOpen = new VariableReferenceExpression("rightOpen", BIGINT);
    private VariableReferenceExpression unknownRange = new VariableReferenceExpression("unknownRange", BIGINT);
    private VariableReferenceExpression emptyRange = new VariableReferenceExpression("emptyRange", BIGINT);
    private VariableReferenceExpression unknown = new VariableReferenceExpression("unknown", BIGINT);
    private VariableReferenceExpression fractionalNdv = new VariableReferenceExpression("fractionalNdv", BIGINT);

    @BeforeClass
    public void setUp()
            throws Exception
    {
        uStats = VariableStatsEstimate.builder()
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(300)
                .setLowValue(0)
                .setHighValue(20)
                .setNullsFraction(0.1)
                .build();
        wStats = VariableStatsEstimate.builder()
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(30)
                .setLowValue(0)
                .setHighValue(20)
                .setNullsFraction(0.1)
                .build();
        xStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(40.0)
                .setLowValue(-10.0)
                .setHighValue(10.0)
                .setNullsFraction(0.25)
                .build();
        yStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(20.0)
                .setLowValue(0.0)
                .setHighValue(5.0)
                .setNullsFraction(0.5)
                .build();
        zStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(5.0)
                .setLowValue(-100.0)
                .setHighValue(100.0)
                .setNullsFraction(0.1)
                .build();
        leftOpenStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(15.0)
                .setNullsFraction(0.1)
                .build();
        rightOpenStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(-15.0)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        unknownRangeStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        emptyRangeStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(0.0)
                .setLowValue(NaN)
                .setHighValue(NaN)
                .setNullsFraction(NaN)
                .build();
        fractionalNdvStats = VariableStatsEstimate.builder()
                .setAverageRowSize(NaN)
                .setDistinctValuesCount(0.1)
                .setNullsFraction(0)
                .build();
        inputStatistics = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(u, uStats)
                .addVariableStatistics(w, wStats)
                .addVariableStatistics(x, xStats)
                .addVariableStatistics(y, yStats)
                .addVariableStatistics(z, zStats)
                .addVariableStatistics(leftOpen, leftOpenStats)
                .addVariableStatistics(rightOpen, rightOpenStats)
                .addVariableStatistics(unknownRange, unknownRangeStats)
                .addVariableStatistics(emptyRange, emptyRangeStats)
                .addVariableStatistics(unknown, VariableStatsEstimate.unknown())
                .addVariableStatistics(fractionalNdv, fractionalNdvStats)
                .setOutputRowCount(1000.0)
                .build();
    }

    @Test
    public void testSemiJoin()
    {
        // overlapping ranges
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, x, w))
                .variableStats(x, stats -> stats
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .nullsFraction(0)
                        .distinctValuesCount(wStats.getDistinctValuesCount()))
                .variableStats(w, stats -> stats.isEqualTo(wStats))
                .variableStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCount(inputStatistics.getOutputRowCount() * xStats.getValuesFraction() * (wStats.getDistinctValuesCount() / xStats.getDistinctValuesCount()));

        // overlapping ranges, nothing filtered out
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, x, u))
                .variableStats(x, stats -> stats
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .nullsFraction(0)
                        .distinctValuesCount(xStats.getDistinctValuesCount()))
                .variableStats(u, stats -> stats.isEqualTo(uStats))
                .variableStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCount(inputStatistics.getOutputRowCount() * xStats.getValuesFraction());

        // source stats are unknown
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, unknown, u))
                .variableStats(unknown, stats -> stats
                        .nullsFraction(0)
                        .distinctValuesCountUnknown()
                        .unknownRange())
                .variableStats(u, stats -> stats.isEqualTo(uStats))
                .variableStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCountUnknown();

        // filtering stats are unknown
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, x, unknown))
                .variableStats(x, stats -> stats
                        .nullsFraction(0)
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .distinctValuesCountUnknown())
                .variableStatsUnknown(unknown)
                .variableStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCountUnknown();

        // zero distinct values
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, emptyRange, emptyRange))
                .outputRowsCount(0);

        // fractional distinct values
        assertThat(computeSemiJoin(inputStatistics, inputStatistics, fractionalNdv, fractionalNdv))
                .outputRowsCount(1000)
                .variableStats(fractionalNdv, stats -> stats
                        .nullsFraction(0)
                        .distinctValuesCount(0.1));
    }

    @Test
    public void testAntiJoin()
    {
        // overlapping ranges
        assertThat(computeAntiJoin(inputStatistics, inputStatistics, u, x))
                .variableStats(u, stats -> stats
                        .lowValue(uStats.getLowValue())
                        .highValue(uStats.getHighValue())
                        .nullsFraction(0)
                        .distinctValuesCount(uStats.getDistinctValuesCount() - xStats.getDistinctValuesCount()))
                .variableStats(x, stats -> stats.isEqualTo(xStats))
                .variableStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCount(inputStatistics.getOutputRowCount() * uStats.getValuesFraction() * (1 - xStats.getDistinctValuesCount() / uStats.getDistinctValuesCount()));

        // overlapping ranges, everything filtered out (but we leave 0.5 due to safety coeeficient)
        assertThat(computeAntiJoin(inputStatistics, inputStatistics, x, u))
                .variableStats(x, stats -> stats
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .nullsFraction(0)
                        .distinctValuesCount(xStats.getDistinctValuesCount() * 0.5))
                .variableStats(u, stats -> stats.isEqualTo(uStats))
                .variableStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCount(inputStatistics.getOutputRowCount() * xStats.getValuesFraction() * 0.5);

        // source stats are unknown
        assertThat(computeAntiJoin(inputStatistics, inputStatistics, unknown, u))
                .variableStats(unknown, stats -> stats
                        .nullsFraction(0)
                        .distinctValuesCountUnknown()
                        .unknownRange())
                .variableStats(u, stats -> stats.isEqualTo(uStats))
                .variableStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCountUnknown();

        // filtering stats are unknown
        assertThat(computeAntiJoin(inputStatistics, inputStatistics, x, unknown))
                .variableStats(x, stats -> stats
                        .nullsFraction(0)
                        .lowValue(xStats.getLowValue())
                        .highValue(xStats.getHighValue())
                        .distinctValuesCountUnknown())
                .variableStatsUnknown(unknown)
                .variableStats(z, stats -> stats.isEqualTo(zStats))
                .outputRowsCountUnknown();

        // zero distinct values
        assertThat(computeAntiJoin(inputStatistics, inputStatistics, emptyRange, emptyRange))
                .outputRowsCount(0);

        // fractional distinct values
        assertThat(computeAntiJoin(inputStatistics, inputStatistics, fractionalNdv, fractionalNdv))
                .outputRowsCount(500)
                .variableStats(fractionalNdv, stats -> stats
                        .nullsFraction(0)
                        .distinctValuesCount(0.05));
    }
}
