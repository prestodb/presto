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

import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestHistoricalPlanStatistics
{
    @Test
    public void testWithNoTables()
    {
        HistoricalPlanStatistics stats = HistoricalPlanStatistics.empty();
        stats = updatePlanStatistics(stats, ImmutableList.of(), stats(10, 10));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of()), stats(10, 10));
        stats = updatePlanStatistics(stats, ImmutableList.of(), stats(12, 13));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of()), stats(12, 13));
    }

    @Test
    public void testWithOneTable()
    {
        HistoricalPlanStatistics stats = HistoricalPlanStatistics.empty();
        stats = updatePlanStatistics(stats, ImmutableList.of(stats(100, 100)), stats(10, 10));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(100, 100))), stats(10, 10));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(95, 105))), stats(10, 10));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(80, 80))), PlanStatistics.empty());
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(120, 120))), PlanStatistics.empty());

        stats = updatePlanStatistics(stats, ImmutableList.of(stats(95, 105)), stats(11, 11));
        assertEquals(stats.getLastRunsStatistics().size(), 1);
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(95, 105))), stats(11, 11));
        stats = updatePlanStatistics(stats, ImmutableList.of(stats(80, 120)), stats(15, 15));
        assertEquals(stats.getLastRunsStatistics().size(), 2);
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(100, 100))), stats(11, 11));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(80, 120))), stats(15, 15));
    }

    @Test
    public void testWithTwoTables()
    {
        HistoricalPlanStatistics stats = HistoricalPlanStatistics.empty();
        stats = updatePlanStatistics(stats, ImmutableList.of(stats(100, 100), stats(10, 10)), stats(10, 10));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(100, 100), stats(10, 10))), stats(10, 10));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(95, 105), stats(9, 11))), stats(10, 10));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(80, 80), stats(10, 10))), PlanStatistics.empty());
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(100, 100), stats(10, 8))), PlanStatistics.empty());

        stats = updatePlanStatistics(stats, ImmutableList.of(stats(200, 200), stats(20, 20)), stats(20, 20));
        assertEquals(stats.getLastRunsStatistics().size(), 2);
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(105, 95), stats(11, 9))), stats(10, 10));
        assertEquals(getPredictedPlanStatistics(stats, ImmutableList.of(stats(200, 180), stats(18, 21))), stats(20, 20));
    }

    @Test
    public void testMaxStatistics()
    {
        HistoricalPlanStatistics stats = HistoricalPlanStatistics.empty();
        for (int i = 1; i <= 20; ++i) {
            stats = updatePlanStatistics(stats, ImmutableList.of(stats(100 * i, 100 * i)), stats(10 * i, 10 * i));
        }
        assertEquals(stats.getLastRunsStatistics().size(), 10);
    }

    private PlanStatistics stats(double rows, double size)
    {
        return new PlanStatistics(Estimate.of(rows), Estimate.of(size), 1);
    }

    private static HistoricalPlanStatistics updatePlanStatistics(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            PlanStatistics current)
    {
        return HistoricalPlanStatisticsUtil.updatePlanStatistics(
                historicalPlanStatistics,
                inputTableStatistics,
                current,
                new HistoryBasedOptimizationConfig());
    }

    private static PlanStatistics getPredictedPlanStatistics(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics)
    {
        return HistoricalPlanStatisticsUtil.getPredictedPlanStatistics(
                historicalPlanStatistics,
                inputTableStatistics,
                new HistoryBasedOptimizationConfig());
    }
}
