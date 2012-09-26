package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class NotExpression
        extends Expression
{
    private final Expression value;

    public NotExpression(Expression value)
    {
        this.value = value;
    }

    public Expression getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(value)
                .toString();
    }
}
