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
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.UnmergeableFilterNode;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.sql.planner.plan.Patterns.unmergeableFilter;

/**
 * Terminal rewrite that converts every {@link UnmergeableFilterNode} back into a regular {@link FilterNode}.
 * This rule runs after all other optimizers, so by the time the plan reaches fragmentation and execution
 * (Java {@code LocalExecutionPlanner} and the native protocol), no unmergeable filters remain.
 */
public class RewriteUnmergeableFilterToFilter
        implements Rule<UnmergeableFilterNode>
{
    private static final Pattern<UnmergeableFilterNode> PATTERN = unmergeableFilter();

    @Override
    public Pattern<UnmergeableFilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(UnmergeableFilterNode node, Captures captures, Context context)
    {
        return Result.ofPlanNode(new FilterNode(
                node.getSourceLocation(),
                node.getId(),
                node.getStatsEquivalentPlanNode(),
                node.getSource(),
                node.getPredicate()));
    }
}
