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
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isDistributedDynamicFilterEnabled;
import static com.facebook.presto.sql.planner.plan.Patterns.join;

/**
 * Populates JoinNode.dynamicFilters for distributed dynamic partition pruning.
 * Creates a filter ID per equi-join clause, mapping it to the build-side variable.
 */
public class AddDynamicFilterRule
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(node -> (node.getType() == JoinType.INNER || node.getType() == JoinType.RIGHT)
                    && node.getDynamicFilters().isEmpty()
                    && !node.getCriteria().isEmpty());

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isDistributedDynamicFilterEnabled(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        Map<String, VariableReferenceExpression> dynamicFilters = new LinkedHashMap<>();
        for (EquiJoinClause clause : node.getCriteria()) {
            String filterId = context.getIdAllocator().getNextId().toString();
            dynamicFilters.put(filterId, clause.getRight());
        }

        return Result.ofPlanNode(new JoinNode(
                node.getSourceLocation(),
                node.getId(),
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                node.getLeft(),
                node.getRight(),
                node.getCriteria(),
                node.getOutputVariables(),
                node.getFilter(),
                node.getLeftHashVariable(),
                node.getRightHashVariable(),
                node.getDistributionType(),
                dynamicFilters));
    }
}
