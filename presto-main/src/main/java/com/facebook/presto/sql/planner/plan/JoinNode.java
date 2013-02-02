package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.analyzer.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;

@Immutable
public class JoinNode
    extends PlanNode
{
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> criteria;

    @JsonCreator
    public JoinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria)
    {
        super(id);

        Preconditions.checkNotNull(left, "left is null");
        Preconditions.checkNotNull(right, "right is null");
        Preconditions.checkNotNull(criteria, "criteria is null");

        this.left = left;
        this.right = right;
        this.criteria = criteria;
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

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(Iterables.concat(left.getOutputSymbols(), right.getOutputSymbols()));
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
            Preconditions.checkNotNull(left, "left is null");
            Preconditions.checkNotNull(right, "right is null");

            this.left = left;
            this.right = right;
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
