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
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.Patterns.distinctLimit;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Replace DistinctLimit node
 * 1. With a empty ValuesNode when count is 0
 * 2. With a Distinct node when the subplan is guaranteed to produce fewer rows than count
 * 3. With its source when the subplan produces only one row
 */
public class RemoveRedundantDistinctLimit
        implements Rule<DistinctLimitNode>
{
    private static final Pattern<DistinctLimitNode> PATTERN = distinctLimit();

    @Override
    public Pattern<DistinctLimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(DistinctLimitNode node, Captures captures, Context context)
    {
        checkArgument(!node.getHashVariable().isPresent(), "HashSymbol should be empty");

        if (isScalar(node.getSource(), context.getLookup())) {
            return Result.ofPlanNode(node.getSource());
        }
        if (isAtMost(node.getSource(), context.getLookup(), node.getLimit())) {
            return Result.ofPlanNode(new AggregationNode(
                    node.getId(),
                    node.getSource(),
                    ImmutableMap.of(),
                    singleGroupingSet(node.getDistinctVariables()),
                    ImmutableList.of(),
                    SINGLE,
                    node.getHashVariable(),
                    Optional.empty()));
        }
        return Result.empty();
    }
}
