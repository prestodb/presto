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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.Streams;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;

public class PruneAggregationSourceColumns
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        Set<Symbol> requiredInputs = Streams.concat(
                aggregationNode.getGroupingKeys().stream(),
                aggregationNode.getHashSymbol().map(Stream::of).orElse(Stream.empty()),
                aggregationNode.getAggregations().values().stream()
                        .flatMap(PruneAggregationSourceColumns::getAggregationInputs))
                .collect(toImmutableSet());

        return restrictChildOutputs(context.getIdAllocator(), aggregationNode, requiredInputs)
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }

    private static Stream<Symbol> getAggregationInputs(AggregationNode.Aggregation aggregation)
    {
        return Streams.concat(
                SymbolsExtractor.extractUnique(aggregation.getCall()).stream(),
                aggregation.getMask().map(Stream::of).orElse(Stream.empty()));
    }
}
