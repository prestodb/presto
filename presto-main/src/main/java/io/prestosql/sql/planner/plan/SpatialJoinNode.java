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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.tree.Expression;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class SpatialJoinNode
        extends PlanNode
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
    private final Expression filter;
    private final Optional<Symbol> leftPartitionSymbol;
    private final Optional<Symbol> rightPartitionSymbol;
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
            @JsonProperty("filter") Expression filter,
            @JsonProperty("leftPartitionSymbol") Optional<Symbol> leftPartitionSymbol,
            @JsonProperty("rightPartitionSymbol") Optional<Symbol> rightPartitionSymbol,
            @JsonProperty("kdbTree") Optional<String> kdbTree)
    {
        super(id);

        this.type = requireNonNull(type, "type is null");
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));
        this.filter = requireNonNull(filter, "filter is null");
        this.leftPartitionSymbol = requireNonNull(leftPartitionSymbol, "leftPartitionSymbol is null");
        this.rightPartitionSymbol = requireNonNull(rightPartitionSymbol, "rightPartitionSymbol is null");
        this.kdbTree = requireNonNull(kdbTree, "kdbTree is null");

        Set<Symbol> inputSymbols = ImmutableSet.<Symbol>builder()
                .addAll(left.getOutputSymbols())
                .addAll(right.getOutputSymbols())
                .build();

        checkArgument(inputSymbols.containsAll(outputSymbols), "Left and right join inputs do not contain all output symbols");
        if (kdbTree.isPresent()) {
            checkArgument(leftPartitionSymbol.isPresent(), "Left partition symbol is missing");
            checkArgument(rightPartitionSymbol.isPresent(), "Right partition symbol is missing");
            checkArgument(left.getOutputSymbols().contains(leftPartitionSymbol.get()), "Left join input does not contain left partition symbol");
            checkArgument(right.getOutputSymbols().contains(rightPartitionSymbol.get()), "Right join input does not contain right partition symbol");
            this.distributionType = DistributionType.PARTITIONED;
        }
        else {
            checkArgument(!leftPartitionSymbol.isPresent(), "KDB tree is missing");
            checkArgument(!rightPartitionSymbol.isPresent(), "KDB tree is missing");
            this.distributionType = DistributionType.REPLICATED;
        }
    }

    @JsonProperty("type")
    public Type getType()
    {
        return type;
    }

    @JsonProperty("left")
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty("right")
    public PlanNode getRight()
    {
        return right;
    }

    @JsonProperty("filter")
    public Expression getFilter()
    {
        return filter;
    }

    @JsonProperty("leftPartitionSymbol")
    public Optional<Symbol> getLeftPartitionSymbol()
    {
        return leftPartitionSymbol;
    }

    @JsonProperty("rightPartitionSymbol")
    public Optional<Symbol> getRightPartitionSymbol()
    {
        return rightPartitionSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("distributionType")
    public DistributionType getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty("kdbTree")
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
        return new SpatialJoinNode(getId(), type, newChildren.get(0), newChildren.get(1), outputSymbols, filter, leftPartitionSymbol, rightPartitionSymbol, kdbTree);
    }
}
