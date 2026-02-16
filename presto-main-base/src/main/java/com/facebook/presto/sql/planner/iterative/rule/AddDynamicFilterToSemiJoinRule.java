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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterEnabled;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;

/**
 * Populates SemiJoinNode.dynamicFilters for distributed dynamic partition pruning.
 * Creates one filter for the single semi-join clause.
 */
public class AddDynamicFilterToSemiJoinRule
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin()
            .matching(node -> node.getDynamicFilters().isEmpty());

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isDistributedDynamicFilterEnabled(session);
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        String filterId = context.getIdAllocator().getNextId().toString();
        Map<String, VariableReferenceExpression> dynamicFilters = ImmutableMap.of(
                filterId, node.getFilteringSourceJoinVariable());

        return Result.ofPlanNode(new SemiJoinNode(
                node.getSourceLocation(),
                node.getId(),
                node.getStatsEquivalentPlanNode(),
                node.getSource(),
                node.getFilteringSource(),
                node.getSourceJoinVariable(),
                node.getFilteringSourceJoinVariable(),
                node.getSemiJoinOutput(),
                node.getSourceHashVariable(),
                node.getFilteringSourceHashVariable(),
                node.getDistributionType(),
                dynamicFilters));
    }
}
