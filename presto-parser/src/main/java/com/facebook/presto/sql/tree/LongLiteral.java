package com.facebook.presto.sql.tree;

import com.google.common.base.Preconditions;

public class LongLiteral
        extends Literal
{
    private final long value;

    public LongLiteral(String value)
    {
        Preconditions.checkNotNull(value, "value is null");
        this.value = Long.parseLong(value);
    }

    public long getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLongLiteral(this, context);
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

        LongLiteral that = (LongLiteral) o;

        if (value != that.value) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (value ^ (value >>> 32));
    }
}
