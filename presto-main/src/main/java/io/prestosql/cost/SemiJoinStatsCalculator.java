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

import io.prestosql.sql.planner.Symbol;

import java.util.function.BiFunction;

import static java.lang.Double.max;
import static java.lang.Double.min;

public final class SemiJoinStatsCalculator
{
    private SemiJoinStatsCalculator() {}

    // arbitrary value to be on the safe side when filtering using Anti Join and when value set for filter symbol does not actually overlap with source symbol very much
    private static final double MIN_ANTI_JOIN_FILTER_COEFFICIENT = 0.5;

    // TODO implementation does not take into account overlapping of ranges for source and filtering source.
    //      Basically it works as low and high values were the same for source and filteringSource and just looks at NDVs.

    public static PlanNodeStatsEstimate computeSemiJoin(PlanNodeStatsEstimate sourceStats, PlanNodeStatsEstimate filteringSourceStats, Symbol sourceJoinSymbol, Symbol filteringSourceJoinSymbol)
    {
        return compute(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol,
                (sourceJoinSymbolStats, filteringSourceJoinSymbolStats) ->
                        min(filteringSourceJoinSymbolStats.getDistinctValuesCount(), sourceJoinSymbolStats.getDistinctValuesCount()));
    }

    public static PlanNodeStatsEstimate computeAntiJoin(PlanNodeStatsEstimate sourceStats, PlanNodeStatsEstimate filteringSourceStats, Symbol sourceJoinSymbol, Symbol filteringSourceJoinSymbol)
    {
        return compute(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol,
                (sourceJoinSymbolStats, filteringSourceJoinSymbolStats) ->
                        max(sourceJoinSymbolStats.getDistinctValuesCount() * MIN_ANTI_JOIN_FILTER_COEFFICIENT,
                                sourceJoinSymbolStats.getDistinctValuesCount() - filteringSourceJoinSymbolStats.getDistinctValuesCount()));
    }

    private static PlanNodeStatsEstimate compute(
            PlanNodeStatsEstimate sourceStats,
            PlanNodeStatsEstimate filteringSourceStats,
            Symbol sourceJoinSymbol,
            Symbol filteringSourceJoinSymbol,
            BiFunction<SymbolStatsEstimate, SymbolStatsEstimate, Double> retainedNdvProvider)
    {
        SymbolStatsEstimate sourceJoinSymbolStats = sourceStats.getSymbolStatistics(sourceJoinSymbol);
        SymbolStatsEstimate filteringSourceJoinSymbolStats = filteringSourceStats.getSymbolStatistics(filteringSourceJoinSymbol);

        double retainedNdv = retainedNdvProvider.apply(sourceJoinSymbolStats, filteringSourceJoinSymbolStats);
        SymbolStatsEstimate newSourceJoinSymbolStats = SymbolStatsEstimate.buildFrom(sourceJoinSymbolStats)
                .setNullsFraction(0)
                .setDistinctValuesCount(retainedNdv)
                .build();

        double sourceDistinctValuesCount = sourceJoinSymbolStats.getDistinctValuesCount();
        if (sourceDistinctValuesCount == 0) {
            return PlanNodeStatsEstimate.buildFrom(sourceStats)
                    .addSymbolStatistics(sourceJoinSymbol, newSourceJoinSymbolStats)
                    .setOutputRowCount(0)
                    .build();
        }

        double filterFactor = sourceJoinSymbolStats.getValuesFraction() * retainedNdv / sourceDistinctValuesCount;
        double outputRowCount = sourceStats.getOutputRowCount() * filterFactor;
        return PlanNodeStatsEstimate.buildFrom(sourceStats)
                .addSymbolStatistics(sourceJoinSymbol, newSourceJoinSymbolStats)
                .setOutputRowCount(outputRowCount)
                .build();
    }
}
