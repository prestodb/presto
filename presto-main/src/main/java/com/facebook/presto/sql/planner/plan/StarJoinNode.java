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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.LogicalPropertiesProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.SortExpressionContext;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.planner.SortExpressionExtractor.extractSortExpression;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class StarJoinNode
        extends InternalPlanNode
{
    private final JoinNode.Type type;
    private final PlanNode left;
    private final List<PlanNode> right;
    private final List<JoinNode.EquiJoinClause> criteria;
    private final List<VariableReferenceExpression> outputVariables;
    private final List<Optional<RowExpression>> filter;
    private final Optional<VariableReferenceExpression> leftHashVariable;
    private final List<Optional<VariableReferenceExpression>> rightHashVariables;
    private final Optional<JoinNode.DistributionType> distributionType;
    private final Map<String, VariableReferenceExpression> dynamicFilters;

    @JsonCreator
    public StarJoinNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") JoinNode.Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") List<PlanNode> right,
            @JsonProperty("criteria") List<JoinNode.EquiJoinClause> criteria,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("filter") List<Optional<RowExpression>> filter,
            @JsonProperty("leftHashVariable") Optional<VariableReferenceExpression> leftHashVariable,
            @JsonProperty("rightHashVariables") List<Optional<VariableReferenceExpression>> rightHashVariables,
            @JsonProperty("distributionType") Optional<JoinNode.DistributionType> distributionType,
            @JsonProperty("dynamicFilters") Map<String, VariableReferenceExpression> dynamicFilters)
    {
        this(sourceLocation, id, Optional.empty(), type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariables, distributionType, dynamicFilters);
    }

    public StarJoinNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            JoinNode.Type type,
            PlanNode left,
            List<PlanNode> right,
            List<JoinNode.EquiJoinClause> criteria,
            List<VariableReferenceExpression> outputVariables,
            List<Optional<RowExpression>> filter,
            Optional<VariableReferenceExpression> leftHashVariable,
            List<Optional<VariableReferenceExpression>> rightHashVariables,
            Optional<JoinNode.DistributionType> distributionType,
            Map<String, VariableReferenceExpression> dynamicFilters)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        // Assume that only one join key
        checkArgument(right.size() == criteria.size());
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(outputVariables, "outputVariables is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashVariable, "leftHashVariable is null");
        requireNonNull(rightHashVariables, "rightHashVariables is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(dynamicFilters, "dynamicFilters is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.outputVariables = ImmutableList.copyOf(outputVariables);
        this.filter = filter;
        this.leftHashVariable = leftHashVariable;
        this.rightHashVariables = rightHashVariables;
        this.distributionType = distributionType;
        this.dynamicFilters = ImmutableMap.copyOf(dynamicFilters);

        Set<VariableReferenceExpression> inputVariables = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(left.getOutputVariables())
                .addAll(right.stream().flatMap(x -> x.getOutputVariables().stream()).collect(toImmutableList()))
                .build();
        checkArgument(new HashSet<>(inputVariables).containsAll(outputVariables), "Left and right join inputs do not contain all output variables");
        checkArgument(!isCrossJoin() || inputVariables.size() == outputVariables.size(), "Cross join does not support output variables pruning or reordering");

        checkArgument(!(criteria.isEmpty() && leftHashVariable.isPresent()), "Left hash variable is only valid in an equijoin");
//        checkArgument(!(criteria.isEmpty() && rightHashVariables.stream().anyMatch(Optional::isPresent)), "Right hash variable is only valid in an equijoin");

        if (distributionType.isPresent()) {
            // The implementation of full outer join only works if the data is hash partitioned.
            checkArgument(
                    !(distributionType.get() == REPLICATED && type.mustPartition()),
                    "%s join do not work with %s distribution type",
                    type,
                    distributionType.get());
            // It does not make sense to PARTITION when there is nothing to partition on
            checkArgument(
                    !(distributionType.get() == PARTITIONED && type.mustReplicate(criteria)),
                    "Equi criteria are empty, so %s join should not have %s distribution type",
                    type,
                    distributionType.get());
        }

        // Do not support dynamic filters now
        checkState(dynamicFilters.isEmpty());
    }

    @VisibleForTesting
    public static JoinNode.Type flipType(JoinNode.Type type)
    {
        switch (type) {
            case INNER:
                return INNER;
            case FULL:
                return FULL;
            case LEFT:
                return RIGHT;
            case RIGHT:
                return LEFT;
            default:
                throw new IllegalStateException("No inverse defined for join type: " + type);
        }
    }

    private static List<JoinNode.EquiJoinClause> flipJoinCriteria(List<JoinNode.EquiJoinClause> joinCriteria)
    {
        return joinCriteria.stream()
                .map(JoinNode.EquiJoinClause::flip)
                .collect(toImmutableList());
    }

    private static List<VariableReferenceExpression> flipOutputVariables(List<VariableReferenceExpression> outputVariables, PlanNode left, PlanNode right)
    {
        List<VariableReferenceExpression> leftVariables = outputVariables.stream()
                .filter(variable -> left.getOutputVariables().contains(variable))
                .collect(Collectors.toList());
        List<VariableReferenceExpression> rightVariables = outputVariables.stream()
                .filter(variable -> right.getOutputVariables().contains(variable))
                .collect(Collectors.toList());
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(rightVariables)
                .addAll(leftVariables)
                .build();
    }

    public StarJoinNode flipChildren()
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Not supported by StarJoinNode");
    }

    @JsonProperty
    public JoinNode.Type getType()
    {
        return type;
    }

    @JsonProperty
    public PlanNode getLeft()
    {
        return left;
    }

    public PlanNode getProbe()
    {
        return left;
    }

    @JsonProperty
    public List<PlanNode> getRight()
    {
        return right;
    }

    public List<PlanNode> getBuild()
    {
        return right;
    }

    @JsonProperty
    public List<JoinNode.EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty
    public List<Optional<RowExpression>> getFilter()
    {
        return filter;
    }

    public Optional<SortExpressionContext> getSortExpressionContext(FunctionAndTypeManager functionAndTypeManager, int index)
    {
        return filter.get(index)
                .flatMap(filter -> extractSortExpression(ImmutableSet.copyOf(right.stream().flatMap(x -> x.getOutputVariables().stream()).collect(toImmutableList())), filter, functionAndTypeManager));
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getLeftHashVariable()
    {
        return leftHashVariable;
    }

    @JsonProperty
    public List<Optional<VariableReferenceExpression>> getRightHashVariables()
    {
        return rightHashVariables;
    }

    @Override
    public List<PlanNode> getSources()
    {
        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        return builder.add(left).addAll(right).build();
    }

    @Override
    public LogicalProperties computeLogicalProperties(LogicalPropertiesProvider logicalPropertiesProvider)
    {
        requireNonNull(logicalPropertiesProvider, "logicalPropertiesProvider cannot be null.");
        return logicalPropertiesProvider.getJoinProperties(this);
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public Optional<JoinNode.DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty
    public Map<String, VariableReferenceExpression> getDynamicFilters()
    {
        return dynamicFilters;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitStarJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == right.size() + 1, "expected newChildren to contain 2 nodes");
        return new StarJoinNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), type, newChildren.get(0), newChildren.subList(1, newChildren.size()), criteria, outputVariables, filter, leftHashVariable, rightHashVariables, distributionType, dynamicFilters);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new StarJoinNode(getSourceLocation(), getId(), statsEquivalentPlanNode, type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariables, distributionType, dynamicFilters);
    }

    public StarJoinNode withDistributionType(JoinNode.DistributionType distributionType)
    {
        return new StarJoinNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariables, Optional.of(distributionType), dynamicFilters);
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && filter.stream().allMatch(x -> !x.isPresent()) && type == INNER;
    }
}
