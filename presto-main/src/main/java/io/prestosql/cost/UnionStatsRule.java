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

import com.google.common.collect.ListMultimap;
import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.UnionNode;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.cost.PlanNodeStatsEstimateMath.addStatsAndCollapseDistinctValues;
import static io.prestosql.sql.planner.plan.Patterns.union;

public class UnionStatsRule
        extends SimpleStatsRule<UnionNode>
{
    private static final Pattern<UnionNode> PATTERN = union();

    public UnionStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<UnionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected final Optional<PlanNodeStatsEstimate> doCalculate(UnionNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        checkArgument(!node.getSources().isEmpty(), "Empty Union is not supported");

        Optional<PlanNodeStatsEstimate> estimate = Optional.empty();
        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);
            PlanNodeStatsEstimate sourceStats = statsProvider.getStats(source);

            PlanNodeStatsEstimate sourceStatsWithMappedSymbols = mapToOutputSymbols(sourceStats, node.getSymbolMapping(), i);

            if (estimate.isPresent()) {
                estimate = Optional.of(addStatsAndCollapseDistinctValues(estimate.get(), sourceStatsWithMappedSymbols));
            }
            else {
                estimate = Optional.of(sourceStatsWithMappedSymbols);
            }
        }

        return estimate;
    }

    private PlanNodeStatsEstimate mapToOutputSymbols(PlanNodeStatsEstimate estimate, ListMultimap<Symbol, Symbol> mapping, int index)
    {
        PlanNodeStatsEstimate.Builder mapped = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(estimate.getOutputRowCount());

        mapping.keySet().stream()
                .forEach(symbol -> mapped.addSymbolStatistics(symbol, estimate.getSymbolStatistics(mapping.get(symbol).get(index))));

        return mapped.build();
    }
}
