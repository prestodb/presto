package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class BooleanLiteral
    extends Literal
{
    public static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral("true");
    public static final BooleanLiteral FALSE_LITERAL = new BooleanLiteral("false");

    private final String value;

    public BooleanLiteral(String value)
    {
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkArgument(value.toLowerCase().equals("true") || value.toLowerCase().equals("false"));

        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBooleanLiteral(this, context);
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

        BooleanLiteral that = (BooleanLiteral) o;

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

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(value)
                .toString();
    }
}
