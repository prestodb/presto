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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getJoinReorderingStrategy;
import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.SystemSessionProperties.isInPredicatesAsInnerJoinsEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.Patterns.Join.type;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.Iterators.getOnlyElement;

/**
 * This optimizer looks for a distinct aggregation above an inner join and
 * determines whether it can be pushed down into the right input of the join.
 * This effectively converts the inner join into a semi join
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
 *   Project
 *     Filter (semijoinvariable)
 *     SemiJoin
 *       source: left
 *       filteringSource: right
 *       semijoinOutput: semijoinvariable
 * </pre>
 */
public class TransformDistinctInnerJoinToLeftEarlyOutJoin
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

        EquiJoinClause equiJoinClause = getOnlyElement(innerJoin.getCriteria().listIterator());
        VariableReferenceExpression sourceJoinVariable = equiJoinClause.getLeft();
        VariableReferenceExpression filteringSourceJoinVariable = equiJoinClause.getRight();
        VariableReferenceExpression semiJoinVariable = context.getVariableAllocator().newVariable("semijoinvariable", BOOLEAN, "eoj");

        SemiJoinNode semiJoinNode = new SemiJoinNode(
                innerJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                innerJoin.getLeft(),
                innerJoin.getRight(),
                sourceJoinVariable,
                filteringSourceJoinVariable,
                semiJoinVariable,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        FilterNode filterNode = new FilterNode(
                semiJoinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                semiJoinNode,
                semiJoinVariable);

        Assignments.Builder assignments = Assignments.builder();
        filterNode.getOutputVariables()
                .stream()
                .filter(variable -> !variable.equals(semiJoinVariable))
                .forEach(variable -> assignments.put(variable, variable));

        ProjectNode projectNode = new ProjectNode(
                filterNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                filterNode,
                assignments.build(),
                LOCAL);

        AggregationNode newAggregationNode = new AggregationNode(
                aggregationNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                projectNode,
                aggregationNode.getAggregations(),
                aggregationNode.getGroupingSets(),
                aggregationNode.getPreGroupedVariables(),
                aggregationNode.getStep(),
                aggregationNode.getHashVariable(),
                aggregationNode.getGroupIdVariable());

        return Result.ofPlanNode(newAggregationNode);
    }

    private boolean canAggregationBePushedDown(AggregationNode aggregationNode, JoinNode joinNode, Context context)
    {
        if (!context.getLogicalPropertiesProvider().isPresent()) {
            return false;
        }

        // Semijoin can have only one filtering condition
        if (joinNode.isCrossJoin() || joinNode.getCriteria().size() != 1) {
            return false;
        }

        Set<VariableReferenceExpression> groupingVariables = ImmutableSet.copyOf(aggregationNode.getGroupingKeys());
        Set<VariableReferenceExpression> joinInputVariablesRight = ImmutableSet.copyOf(joinNode.getRight().getOutputVariables());
        Set<VariableReferenceExpression> joinInputVariablesLeft = ImmutableSet.copyOf(joinNode.getLeft().getOutputVariables());
        Set<VariableReferenceExpression> joinOutputVariables = ImmutableSet.copyOf(joinNode.getOutputVariables());

        LogicalProperties aggregationNodelogicalProperties = context.getLogicalPropertiesProvider().get().getAggregationProperties(aggregationNode);
        if (!aggregationNodelogicalProperties.canBeHomogenized(joinInputVariablesRight, groupingVariables)) {
            return false;
        }

        if (!joinInputVariablesLeft.equals(joinOutputVariables)) {
            return false;
        }

        return true;
    }
}
