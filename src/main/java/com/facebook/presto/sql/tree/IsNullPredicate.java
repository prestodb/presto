package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class IsNullPredicate
        extends Expression
{
    private final Expression value;

    public IsNullPredicate(Expression value)
    {
        this.value = value;
    }

    public Expression getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIsNullPredicate(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(value)
                .toString();
    }
}
