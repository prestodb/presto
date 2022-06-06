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
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.spi.plan.AggregationNode.Aggregation.removeDistinct;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;

/**
 * Removes distinct from aggregates where the combination of aggregate columns and grouping variables contain a unique key.
 * Ultimately this optimization needs to happen before the mark distinct optimization occurs.
 * This will require moving the operations that transform away original expressions earlier in the sequence
 * as logical property computation is designed to sit behind that transformation. For now this rule
 * can be tested by disabling the mark distinct rule.
 */
public class RemoveRedundantAggregateDistinct
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(RemoveRedundantAggregateDistinct::aggregateDistinctOfUniqueKey);

    private static boolean aggregateDistinctOfUniqueKey(AggregationNode node)
    {
        return !node.getAggregations().isEmpty() &&
                ((GroupReference) node.getSource()).getLogicalProperties().isPresent() &&
                node.getAggregations().values().stream()
                        .filter(AggregationNode.Aggregation::isDistinct)
                        .anyMatch(a -> ((GroupReference) node.getSource()).getLogicalProperties().get().isDistinct(
                                Stream.concat(node.getGroupingKeys().stream().map(VariableReferenceExpression.class::cast),
                                        a.getArguments().stream().map(VariableReferenceExpression.class::cast)).collect(Collectors.toSet())));
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        //create new AggregateNode same as original but with distinct turned off for
        //any aggregate function whose argument variables + grouping variables form a unique key
        return Result.ofPlanNode(new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getSource(),
                node.getAggregations().entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e ->
                        (e.getValue().isDistinct() &&
                                ((GroupReference) node.getSource()).getLogicalProperties().get().isDistinct(
                                        Stream.concat(node.getGroupingKeys().stream().map(VariableReferenceExpression.class::cast),
                                                (e.getValue()).getArguments().stream().map(VariableReferenceExpression.class::cast)).collect(Collectors.toSet()))) ?
                                removeDistinct(e.getValue()) : (e.getValue()))),
                node.getGroupingSets(),
                node.getPreGroupedVariables(),
                node.getStep(),
                node.getHashVariable(),
                node.getGroupIdVariable()));
    }
}
