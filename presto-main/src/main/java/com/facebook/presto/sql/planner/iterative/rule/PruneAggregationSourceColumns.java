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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils;
import com.google.common.collect.Streams;

import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

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
        Set<VariableReferenceExpression> requiredInputs = Streams.concat(
                aggregationNode.getGroupingKeys().stream(),
                aggregationNode.getHashVariable().map(Stream::of).orElse(Stream.empty()),
                aggregationNode.getAggregations().values().stream()
                        .flatMap(aggregation -> getAggregationInputs(aggregation, context.getVariableAllocator().getTypes())))
                .collect(toImmutableSet());

        return restrictChildOutputs(context.getIdAllocator(), aggregationNode, requiredInputs)
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }

    private static Stream<VariableReferenceExpression> getAggregationInputs(AggregationNode.Aggregation aggregation, TypeProvider types)
    {
        return Streams.concat(
                AggregationNodeUtils.extractAggregationUniqueVariables(aggregation, types).stream(),
                aggregation.getMask().map(Stream::of).orElse(Stream.empty()));
    }
}
