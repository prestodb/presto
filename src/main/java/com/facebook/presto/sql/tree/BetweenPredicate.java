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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBetweenPredicate(this, context);
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BetweenPredicate that = (BetweenPredicate) o;

        if (!max.equals(that.max)) {
            return false;
        }
        if (!min.equals(that.min)) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = value.hashCode();
        result = 31 * result + min.hashCode();
        result = 31 * result + max.hashCode();
        return result;
    }
}
