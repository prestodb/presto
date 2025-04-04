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
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.sql.planner.iterative.rule.Util.pruneOrderingColumns;
import static com.facebook.presto.sql.planner.plan.Patterns.topN;

/**
 * Removes TopN columns from input if the source has a Key that refers to the ordering columns
 */
public class RemoveRedundantTopNColumns
        implements Rule<TopNNode>
{
    private static final Pattern<TopNNode> PATTERN = topN().matching(p -> ((GroupReference) p.getSource()).getLogicalProperties().isPresent());

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode node, Captures captures, Context context)
    {
        OrderingScheme orderingScheme = node.getOrderingScheme();

        LogicalProperties sourceLogicalProperties = ((GroupReference) node.getSource()).getLogicalProperties().get();
        OrderingScheme newOrderingScheme = pruneOrderingColumns(orderingScheme, sourceLogicalProperties);

        if (newOrderingScheme.equals(orderingScheme)) {
            return Result.empty();
        }

        return Result.ofPlanNode(new TopNNode(node.getSourceLocation(), node.getId(), node.getSource(), node.getCount(), newOrderingScheme, node.getStep()));
    }
}
