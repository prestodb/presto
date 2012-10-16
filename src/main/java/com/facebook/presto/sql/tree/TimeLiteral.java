package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class TimeLiteral
        extends Literal
{
    private final String value;

    public TimeLiteral(String value)
    {
        Preconditions.checkNotNull(value, "value is null");
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTimeLiteral(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(value)
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

        TimeLiteral that = (TimeLiteral) o;

        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
