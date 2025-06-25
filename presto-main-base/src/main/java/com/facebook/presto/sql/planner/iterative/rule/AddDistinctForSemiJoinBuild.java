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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isAddDistinctBelowSemiJoinBuildEnabled;
import static com.facebook.presto.spi.plan.AggregationNode.isDistinct;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;

/**
 * Add a distinct aggregation under the build side of semi join, for example:
 * Rewrite query from
 * <pre>
 *     - SemiJoin
 *          l.col in r.col
 *          - scan l
 *              col
 *          - scan r
 *              col
 * </pre>
 * into
 * <pre>
 *     - SemiJoin
 *          l.col in r.col
 *          - scan l
 *              col
 *          - Aggregate
 *              group by r.col
 *              - scan r
 *                  col
 * </pre>
 */
public class AddDistinctForSemiJoinBuild
        implements Rule<SemiJoinNode>
{
    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return semiJoin();
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAddDistinctBelowSemiJoinBuildEnabled(session);
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        PlanNode filterSource = context.getLookup().resolve(node.getFilteringSource());
        VariableReferenceExpression filteringSourceVariable = node.getFilteringSourceJoinVariable();
        if (isOutputDistinct(filterSource, filteringSourceVariable, context)) {
            return Result.empty();
        }
        AggregationNode.GroupingSetDescriptor groupingSetDescriptor = singleGroupingSet(ImmutableList.of(node.getFilteringSourceJoinVariable()));
        AggregationNode distinctAggregation = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                filterSource,
                ImmutableMap.of(),
                groupingSetDescriptor,
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(node.getSource(), distinctAggregation)));
    }

    boolean isOutputDistinct(PlanNode node, VariableReferenceExpression output, Context context)
    {
        if (node instanceof AggregationNode) {
            AggregationNode aggregationNode = (AggregationNode) node;
            return isDistinct(aggregationNode) && aggregationNode.getGroupingKeys().size() == 1 && aggregationNode.getGroupingKeys().contains(output);
        }
        else if (node instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) node;
            RowExpression inputExpression = projectNode.getAssignments().get(output);
            if (inputExpression instanceof VariableReferenceExpression) {
                return isOutputDistinct(context.getLookup().resolve(projectNode.getSource()), (VariableReferenceExpression) inputExpression, context);
            }
            return false;
        }
        else if (node instanceof FilterNode) {
            return isOutputDistinct(context.getLookup().resolve(((FilterNode) node).getSource()), output, context);
        }
        return false;
    }
}
