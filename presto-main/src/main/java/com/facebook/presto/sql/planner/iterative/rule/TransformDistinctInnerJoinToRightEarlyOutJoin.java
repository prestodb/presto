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
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getJoinReorderingStrategy;
import static com.facebook.presto.SystemSessionProperties.getPushAggregationBelowJoinByteReductionThreshold;
import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.SystemSessionProperties.isInPredicatesAsInnerJoinsEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.Patterns.Join.type;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

/**
 * This optimizer looks for a Distinct aggregation above an inner join and
 * determines whether it can be pushed down into the left input of the join.
 * <p/>
 * Plan before optimizer:
 * <pre>
 * Aggregation (distinct)
 *   Join (inner)
 *     left
 *     right
 * </pre>
 * <p/>
 * Plan after optimizer:
 * <pre>
 * Aggregation (distinct)
 *   Join (inner)
 *     Aggregation (distinct)
 *       left
 *     right
 * </pre>
 */
public class TransformDistinctInnerJoinToRightEarlyOutJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();
    private static final Pattern<AggregationNode> PATTERN = aggregation()
                            .matching(AggregationNode::isDistinct)
                            .with(source().matching(
                                    join()
                                            .capturedAs(JOIN)
                                            .with(type()
                                                    .matching(type -> type == INNER))));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isInPredicatesAsInnerJoinsEnabled(session) &&
                isExploitConstraints(session) &&
                getJoinReorderingStrategy(session) == AUTOMATIC;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        JoinNode innerJoin = captures.get(JOIN);

        if (!canAggregationBePushedDown(aggregationNode, innerJoin, context)) {
            return Result.empty();
        }

        AggregationNode aggregationBelowJoin = new AggregationNode(
                innerJoin.getLeft().getSourceLocation(),
                context.getIdAllocator().getNextId(),
                innerJoin.getLeft(),
                ImmutableMap.of(),
                singleGroupingSet(innerJoin.getLeft().getOutputVariables()),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        JoinNode newInnerJoin = new JoinNode(
                innerJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                innerJoin.getType(),
                aggregationBelowJoin,
                innerJoin.getRight(),
                innerJoin.getCriteria(),
                innerJoin.getOutputVariables(),
                innerJoin.getFilter(),
                innerJoin.getLeftHashVariable(),
                innerJoin.getRightHashVariable(),
                innerJoin.getDistributionType(),
                innerJoin.getDynamicFilters());

        AggregationNode newDistinctNode = new AggregationNode(
                aggregationNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                newInnerJoin,
                aggregationNode.getAggregations(),
                aggregationNode.getGroupingSets(),
                aggregationNode.getPreGroupedVariables(),
                aggregationNode.getStep(),
                aggregationNode.getHashVariable(),
                aggregationNode.getGroupIdVariable());

        return Result.ofPlanNode(newDistinctNode);
    }

    private boolean canAggregationBePushedDown(AggregationNode aggregationNode, JoinNode joinNode, Context context)
    {
        if (!context.getLogicalPropertiesProvider().isPresent() ||
                !((GroupReference) joinNode.getLeft()).getLogicalProperties().isPresent()) {
            return false;
        }

        if (joinNode.isCrossJoin() || isJoinCardinalityReducing(joinNode, context)) {
            return false;
        }

        Set<VariableReferenceExpression> groupingVariables = ImmutableSet.copyOf(aggregationNode.getGroupingKeys());
        Set<VariableReferenceExpression> joinLeftInputVariables = ImmutableSet.copyOf(joinNode.getLeft().getOutputVariables());

        LogicalProperties joinLeftInputLogicalProperties = ((GroupReference) joinNode.getLeft()).getLogicalProperties().get();
        LogicalProperties aggregationNodelogicalProperties = context.getLogicalPropertiesProvider().get().getAggregationProperties(aggregationNode);

        if (!aggregationNodelogicalProperties.canBeHomogenized(joinLeftInputVariables, groupingVariables)) {
            return false;
        }

        if (joinLeftInputLogicalProperties.isDistinct(joinLeftInputVariables)) {
            return false;
        }

        return true;
    }

    private boolean isJoinCardinalityReducing(JoinNode joinNode, Context context)
    {
        StatsProvider stats = context.getStatsProvider();
        PlanNodeStatsEstimate joinStats = stats.getStats(joinNode);
        PlanNodeStatsEstimate leftStats = stats.getStats(joinNode.getLeft());

        double inputBytes = leftStats.getOutputSizeInBytes(joinNode.getLeft());
        double outputBytes = joinStats.getOutputSizeInBytes(joinNode);
        return outputBytes <= inputBytes * getPushAggregationBelowJoinByteReductionThreshold(context.getSession());
    }
}
