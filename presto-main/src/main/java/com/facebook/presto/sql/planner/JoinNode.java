package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class JoinNode
    extends PlanNode
{
    private final PlanNode left;
    private final PlanNode right;
    private final Expression criteria;

    @JsonCreator
    public JoinNode(@JsonProperty("left") PlanNode left, @JsonProperty("right") PlanNode right, @JsonProperty("criteria") Expression criteria)
    {
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
    public Expression getCriteria()
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
}
