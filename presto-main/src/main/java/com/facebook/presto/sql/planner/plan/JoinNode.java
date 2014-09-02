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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Join;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class JoinNode
        extends PlanNode
{
    private final Type type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> criteria;
    private final Symbol leftHashSymbol;
    private final Symbol rightHashSymbol;

    @JsonCreator
    public JoinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("leftHashSymbol") Symbol leftHashSymbol,
            @JsonProperty("rightHashSymbol") Symbol rightHashSymbol,
            @JsonProperty("criteria") List<EquiJoinClause> criteria)
    {
        super(id);

        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(left, "left is null");
        Preconditions.checkNotNull(right, "right is null");
        Preconditions.checkNotNull(criteria, "criteria is null");
        Preconditions.checkNotNull(leftHashSymbol, "leftHashSymbol is null");
        Preconditions.checkNotNull(rightHashSymbol, "rightHashSymbol is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.leftHashSymbol = leftHashSymbol;
        this.rightHashSymbol = rightHashSymbol;
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        CROSS("CrossJoin");

        private final String joinLabel;

        private Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public static Type typeConvert(Join.Type joinType)
        {
            // Omit SEMI join types because they must be inferred by the planner and not part of the SQL parse tree
            switch (joinType) {
                case INNER:
                    return Type.INNER;
                case LEFT:
                    return Type.LEFT;
                case RIGHT:
                    return Type.RIGHT;
                case CROSS:
                case IMPLICIT:
                    return Type.CROSS;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + joinType);
            }
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

    @JsonProperty("criteria")
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty("leftHashSymbol")
    public Symbol getLeftHashSymbol()
    {
        return leftHashSymbol;
    }

    @JsonProperty("rightHashSymbol")
    public Symbol getRightHashSymbol()
    {
        return rightHashSymbol;
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
        return ImmutableList.<Symbol>builder()
                .addAll(left.getOutputSymbols())
                .addAll(right.getOutputSymbols())
                .build();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    public static class EquiJoinClause
    {
        private final Symbol left;
        private final Symbol right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") Symbol left, @JsonProperty("right") Symbol right)
        {
            this.left = checkNotNull(left, "left is null");
            this.right = checkNotNull(right, "right is null");
        }

        @JsonProperty("left")
        public Symbol getLeft()
        {
            return left;
        }

        @JsonProperty("right")
        public Symbol getRight()
        {
            return right;
        }

        public static Function<EquiJoinClause, Symbol> leftGetter()
        {
            return new Function<EquiJoinClause, Symbol>()
            {
                @Override
                public Symbol apply(EquiJoinClause input)
                {
                    return input.getLeft();
                }
            };
        }

        public static Function<EquiJoinClause, Symbol> rightGetter()
        {
            return new Function<EquiJoinClause, Symbol>()
            {
                @Override
                public Symbol apply(EquiJoinClause input)
                {
                    return input.getRight();
                }
            };
        }
    }
}
