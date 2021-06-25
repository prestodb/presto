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
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;

/**
 * Removes distinct operations where the grouping variables contain a unique key.
 */
public class RemoveRedundantDistinct
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(AggregationNode::isDistinct)
            .matching(RemoveRedundantDistinct::distinctOfUniqueKey);

    private static boolean distinctOfUniqueKey(AggregationNode node)
    {
        return node.hasNonEmptyGroupingSet() &&
                node.getAggregations().isEmpty() &&
                ((GroupReference) node.getSource()).getLogicalProperties().isPresent() &&
                ((GroupReference) node.getSource()).getLogicalProperties().get().isDistinct(node.getGroupingKeys().stream().collect(Collectors.toSet()));
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        return Result.ofPlanNode(node.getSource());
    }
}
