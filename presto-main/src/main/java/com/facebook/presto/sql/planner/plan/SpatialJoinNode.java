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

import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class SpatialJoinNode
        extends InternalPlanNode
{
    public enum Type
    {
        INNER("SpatialInnerJoin"),
        LEFT("SpatialLeftJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public static Type fromJoinNodeType(JoinNode.Type joinNodeType)
        {
            switch (joinNodeType) {
                case INNER:
                    return Type.INNER;
                case LEFT:
                    return Type.LEFT;
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + joinNodeType);
            }
        }
    }

    private final Type type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<Symbol> outputSymbols;
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
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("filter") RowExpression filter,
            @JsonProperty("leftPartitionVariable") Optional<VariableReferenceExpression> leftPartitionVariable,
            @JsonProperty("rightPartitionVariable") Optional<VariableReferenceExpression> rightPartitionVariable,
            @JsonProperty("kdbTree") Optional<String> kdbTree)
    {
        super(id);

        this.type = requireNonNull(type, "type is null");
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));
        this.filter = requireNonNull(filter, "filter is null");
        this.leftPartitionVariable = requireNonNull(leftPartitionVariable, "leftPartitionVariable is null");
        this.rightPartitionVariable = requireNonNull(rightPartitionVariable, "rightPartitionVariable is null");
        this.kdbTree = requireNonNull(kdbTree, "kdbTree is null");

        Set<Symbol> inputSymbols = ImmutableSet.<Symbol>builder()
                .addAll(left.getOutputSymbols())
                .addAll(right.getOutputSymbols())
                .build();

        checkArgument(inputSymbols.containsAll(outputSymbols), "Left and right join inputs do not contain all output symbols");
        if (kdbTree.isPresent()) {
            checkArgument(leftPartitionVariable.isPresent(), "Left partition symbol is missing");
            checkArgument(rightPartitionVariable.isPresent(), "Right partition symbol is missing");
            checkArgument(left.getOutputSymbols().contains(new Symbol(leftPartitionVariable.get().getName())), "Left join input does not contain left partition symbol");
            checkArgument(right.getOutputSymbols().contains(new Symbol(rightPartitionVariable.get().getName())), "Right join input does not contain right partition symbol");
            this.distributionType = DistributionType.PARTITIONED;
        }
        else {
            checkArgument(!leftPartitionVariable.isPresent(), "KDB tree is missing");
            checkArgument(!rightPartitionVariable.isPresent(), "KDB tree is missing");
            this.distributionType = DistributionType.REPLICATED;
        }
    }

    @JsonProperty
    public Type getType()
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
        return ImmutableList.of(left, right);
    }

    @Override
    @JsonProperty
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
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
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSpatialJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new SpatialJoinNode(getId(), type, newChildren.get(0), newChildren.get(1), outputSymbols, filter, leftPartitionVariable, rightPartitionVariable, kdbTree);
    }
}
