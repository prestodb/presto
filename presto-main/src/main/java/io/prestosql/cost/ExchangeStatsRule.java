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

import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.prestosql.cost.PlanNodeStatsEstimateMath.addStatsAndMaxDistinctValues;
import static io.prestosql.sql.planner.plan.Patterns.exchange;

public class ExchangeStatsRule
        extends SimpleStatsRule<ExchangeNode>
{
    private static final Pattern<ExchangeNode> PATTERN = exchange();

    public ExchangeStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(ExchangeNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        Optional<PlanNodeStatsEstimate> estimate = Optional.empty();
        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);
            PlanNodeStatsEstimate sourceStats = statsProvider.getStats(source);

            PlanNodeStatsEstimate sourceStatsWithMappedSymbols = mapToOutputSymbols(sourceStats, node.getInputs().get(i), node.getOutputSymbols());

            if (estimate.isPresent()) {
                estimate = Optional.of(addStatsAndMaxDistinctValues(estimate.get(), sourceStatsWithMappedSymbols));
            }
            else {
                estimate = Optional.of(sourceStatsWithMappedSymbols);
            }
        }

        verify(estimate.isPresent());
        return estimate;
    }

    private PlanNodeStatsEstimate mapToOutputSymbols(PlanNodeStatsEstimate estimate, List<Symbol> inputs, List<Symbol> outputs)
    {
        checkArgument(inputs.size() == outputs.size(), "Input symbols count does not match output symbols count");
        PlanNodeStatsEstimate.Builder mapped = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(estimate.getOutputRowCount());

        for (int i = 0; i < inputs.size(); i++) {
            mapped.addSymbolStatistics(outputs.get(i), estimate.getSymbolStatistics(inputs.get(i)));
        }

        return mapped.build();
    }
}
