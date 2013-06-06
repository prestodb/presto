package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class BooleanLiteral
    extends Literal
{
    public static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral("true");
    public static final BooleanLiteral FALSE_LITERAL = new BooleanLiteral("false");

    private final boolean value;

    public BooleanLiteral(String value)
    {
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkArgument(value.toLowerCase().equals("true") || value.toLowerCase().equals("false"));

        this.value = value.toLowerCase().equals("true");
    }

    public boolean getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final BooleanLiteral other = (BooleanLiteral) obj;
        return Objects.equal(this.value, other.value);
    }
}
