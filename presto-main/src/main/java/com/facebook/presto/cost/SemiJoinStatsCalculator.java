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

import java.util.function.BiFunction;

import static java.lang.Double.max;
import static java.lang.Double.min;

public class SemiJoinStatsCalculator
{
    // TODO implementation does not take into account overlapping of ranges for source and filtering source.
    //      Basically it works as low and high values were the same for source and filteringSource and just looks at NDVs.

    public PlanNodeStatsEstimate computeSemiJoin(PlanNodeStatsEstimate sourceStats, PlanNodeStatsEstimate filteringSourceStats, Symbol sourceJoinSymbol, Symbol filteringSourceJoinSymbol)
    {
        return compute(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol,
                (sourceJoinSymbolStats, filteringSourceJoinSymbolStats) ->
                        min(filteringSourceJoinSymbolStats.getDistinctValuesCount(), sourceJoinSymbolStats.getDistinctValuesCount()));
    }

    // arbitrary value to be on the safe side when filtering using Anti Join and when value set for filter symbol does not actually overlap with source symbol very much
    private static final double MAX_ANTI_JOIN_FILTER_COEFFICIENT = 0.5;

    public PlanNodeStatsEstimate computeAntiJoin(PlanNodeStatsEstimate sourceStats, PlanNodeStatsEstimate filteringSourceStats, Symbol sourceJoinSymbol, Symbol filteringSourceJoinSymbol)
    {
        return compute(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol,
                (sourceJoinSymbolStats, filteringSourceJoinSymbolStats) ->
                        max(sourceJoinSymbolStats.getDistinctValuesCount() * MAX_ANTI_JOIN_FILTER_COEFFICIENT,
                                sourceJoinSymbolStats.getDistinctValuesCount() - filteringSourceJoinSymbolStats.getDistinctValuesCount()));
    }

    private PlanNodeStatsEstimate compute(
            PlanNodeStatsEstimate sourceStats,
            PlanNodeStatsEstimate filteringSourceStats,
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            BiFunction<SymbolStatsEstimate, SymbolStatsEstimate, Double> retainedNdvProvider)
    {
        SymbolStatsEstimate sourceJoinSymbolStats = sourceStats.getSymbolStatistics(sourceJoinSymbol);
        SymbolStatsEstimate filteringSourceJoinSymbolStats = filteringSourceStats.getSymbolStatistics(filteringSourceJoinSymbol);

        double retainedNDV = retainedNdvProvider.apply(sourceJoinSymbolStats, filteringSourceJoinSymbolStats);
        double filterFactor = sourceJoinSymbolStats.getValuesFraction() * retainedNDV / sourceJoinSymbolStats.getDistinctValuesCount();

        SymbolStatsEstimate newSourceJoinSymbolStats = SymbolStatsEstimate.buildFrom(sourceJoinSymbolStats)
                .setNullsFraction(0)
                .setDistinctValuesCount(retainedNDV)
                .build();
        PlanNodeStatsEstimate outputStats = sourceStats.mapSymbolColumnStatistics(sourceJoinSymbol, oldStats -> newSourceJoinSymbolStats);
        outputStats = outputStats.mapOutputRowCount(rowCount -> rowCount * filterFactor);

        return outputStats;
    }
}
