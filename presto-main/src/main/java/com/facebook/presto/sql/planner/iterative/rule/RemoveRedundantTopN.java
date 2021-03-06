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
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SortNode;

import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.Patterns.topN;

/**
 * Replace TopN node
 * 1. With its source when the subplan is at most one row
 * 2. With a Sort node when the subplan is guaranteed to produce fewer rows than N
 */
public class RemoveRedundantTopN
        implements Rule<TopNNode>
{
    private static final Pattern<TopNNode> PATTERN = topN();

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode node, Captures captures, Context context)
    {
        if (isAtMostScalar(node.getSource(), context.getLookup())) {
            return Result.ofPlanNode(node.getSource());
        }
        if (isAtMost(node.getSource(), context.getLookup(), node.getCount())) {
            return Result.ofPlanNode(new SortNode(context.getIdAllocator().getNextId(), node.getSource(), node.getOrderingScheme(), false));
        }
        return Result.empty();
    }
}
