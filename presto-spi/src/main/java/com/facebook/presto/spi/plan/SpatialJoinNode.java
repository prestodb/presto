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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class SpatialJoinNode
        extends PlanNode
{
    public enum SpatialJoinType
    {
        INNER("SpatialInnerJoin"),
        LEFT("SpatialLeftJoin");

        private final String joinLabel;

        SpatialJoinType(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public static SpatialJoinType fromJoinNodeType(JoinType joinNodeType)
        {
            switch (joinNodeType) {
                case INNER:
                    return SpatialJoinType.INNER;
                case LEFT:
                    return SpatialJoinType.LEFT;
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + joinNodeType);
            }
        }
    }

    private final SpatialJoinType type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<VariableReferenceExpression> outputVariables;
    private final RowExpression filter;
    private final Optional<VariableReferenceExpression> leftPartitionVariable;
    private final Optional<VariableReferenceExpression> rightPartitionVariable;
    private final Optional<String> kdbTree;
    private final DistributionType distributionType;

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    @JsonCreator
    public SpatialJoinNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") SpatialJoinType type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("filter") RowExpression filter,
            @JsonProperty("leftPartitionVariable") Optional<VariableReferenceExpression> leftPartitionVariable,
            @JsonProperty("rightPartitionVariable") Optional<VariableReferenceExpression> rightPartitionVariable,
            @JsonProperty("kdbTree") Optional<String> kdbTree)
    {
        this(sourceLocation, id, Optional.empty(), type, left, right, outputVariables, filter, leftPartitionVariable, rightPartitionVariable, kdbTree);
    }

    public SpatialJoinNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            SpatialJoinType type,
            PlanNode left,
            PlanNode right,
            List<VariableReferenceExpression> outputVariables,
            RowExpression filter,
            Optional<VariableReferenceExpression> leftPartitionVariable,
            Optional<VariableReferenceExpression> rightPartitionVariable,
            Optional<String> kdbTree)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        this.type = requireNonNull(type, "type is null");
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.outputVariables = unmodifiableList(new ArrayList<>(requireNonNull(outputVariables, "outputVariables is null")));
        this.filter = requireNonNull(filter, "filter is null");
        this.leftPartitionVariable = requireNonNull(leftPartitionVariable, "leftPartitionVariable is null");
        this.rightPartitionVariable = requireNonNull(rightPartitionVariable, "rightPartitionVariable is null");
        this.kdbTree = requireNonNull(kdbTree, "kdbTree is null");

        Set<VariableReferenceExpression> inputSymbols = new LinkedHashSet<>();
        inputSymbols.addAll(left.getOutputVariables());
        inputSymbols.addAll(right.getOutputVariables());
        checkArgument(inputSymbols.containsAll(outputVariables), "Left and right join inputs do not contain all output variables");

        if (kdbTree.isPresent()) {
            checkArgument(leftPartitionVariable.isPresent(), "Left partition variable is missing");
            checkArgument(rightPartitionVariable.isPresent(), "Right partition variable is missing");
            checkArgument(left.getOutputVariables().contains(leftPartitionVariable.get()), "Left join input does not contain left partition variable");
            checkArgument(right.getOutputVariables().contains(rightPartitionVariable.get()), "Right join input does not contain right partition variable");
            this.distributionType = DistributionType.PARTITIONED;
        }
        else {
            checkArgument(!leftPartitionVariable.isPresent(), "KDB tree is missing");
            checkArgument(!rightPartitionVariable.isPresent(), "KDB tree is missing");
            this.distributionType = DistributionType.REPLICATED;
        }
    }

    @JsonProperty
    public SpatialJoinType getType()
    {
        return type;
    }

    @JsonProperty
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty
    public PlanNode getRight()
    {
        return right;
    }

    @JsonProperty
    public RowExpression getFilter()
    {
        return filter;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getLeftPartitionVariable()
    {
        return leftPartitionVariable;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getRightPartitionVariable()
    {
        return rightPartitionVariable;
    }

    @Override
    public List<PlanNode> getSources()
    {
        List<PlanNode> sources = new ArrayList<>();
        sources.add(left);
        sources.add(right);
        return unmodifiableList(sources);
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public DistributionType getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty
    public Optional<String> getKdbTree()
    {
        return kdbTree;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSpatialJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new SpatialJoinNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), type, newChildren.get(0), newChildren.get(1), outputVariables, filter, leftPartitionVariable, rightPartitionVariable, kdbTree);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new SpatialJoinNode(getSourceLocation(), getId(), statsEquivalentPlanNode, type, left, right, outputVariables, filter, leftPartitionVariable, rightPartitionVariable, kdbTree);
    }
}
