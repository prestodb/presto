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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Map;

import static java.lang.Double.isNaN;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class CapDistinctValuesCountToOutputRowsCount
        implements ComposableStatsCalculator.Normalizer
{
    @Override
    public PlanNodeStatsEstimate normalize(PlanNode node, PlanNodeStatsEstimate estimate, Map<Symbol, Type> types)
    {
        requireNonNull(node, "node is null");
        requireNonNull(estimate, "estimate is null");
        requireNonNull(types, "types is null");

        double outputRowCount = estimate.getOutputRowCount();
        if (isNaN(outputRowCount)) {
            return estimate;
        }
        for (Symbol symbol : estimate.getSymbolsWithKnownStatistics()) {
            estimate = estimate.mapSymbolColumnStatistics(
                    symbol,
                    symbolStatsEstimate -> symbolStatsEstimate.mapDistinctValuesCount(
                            distinctValuesCount -> {
                                if (!isNaN(distinctValuesCount)) {
                                    return min(distinctValuesCount, outputRowCount);
                                }
                                return distinctValuesCount;
                            }));
        }
        return estimate;
    }
}
