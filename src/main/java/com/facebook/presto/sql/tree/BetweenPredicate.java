package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class BetweenPredicate
        extends Expression
{
    private final Expression value;
    private final Expression min;
    private final Expression max;

    public BetweenPredicate(Expression value, Expression min, Expression max)
    {
        this.value = value;
        this.min = min;
        this.max = max;
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getMin()
    {
        return min;
    }

    public Expression getMax()
    {
        return max;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("min", min)
                .add("max", max)
                .toString();
    }
}
