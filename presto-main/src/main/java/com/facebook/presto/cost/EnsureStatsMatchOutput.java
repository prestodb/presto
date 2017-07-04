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

import static com.facebook.presto.cost.PlanNodeStatsEstimate.buildFrom;
import static com.facebook.presto.cost.SymbolStatsEstimate.UNKNOWN_STATS;
import static com.google.common.base.Predicates.not;

public class EnsureStatsMatchOutput
        implements ComposableStatsCalculator.Normalizer
{
    @Override
    public PlanNodeStatsEstimate normalize(PlanNode node, PlanNodeStatsEstimate estimate, Map<Symbol, Type> types)
    {
        PlanNodeStatsEstimate.Builder builder = buildFrom(estimate);

        node.getOutputSymbols().stream()
                .filter(not(estimate.getSymbolsWithKnownStatistics()::contains))
                .forEach(symbol -> builder.addSymbolStatistics(symbol, UNKNOWN_STATS));

        estimate.getSymbolsWithKnownStatistics().stream()
                .filter(not(node.getOutputSymbols()::contains))
                .forEach(builder::removeSymbolStatistics);

        return builder.build();
    }
}
