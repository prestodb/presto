package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class IntervalLiteral
        extends Literal
{
    public enum Sign
    {
        POSITIVE, NEGATIVE
    }

    private final String value;
    private final String type;
    private final Sign sign;

    public IntervalLiteral(String value, String type, Sign sign)
    {
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkNotNull(type, "type is null");

        this.value = value;
        this.type = type;
        this.sign = sign;
    }

    public String getValue()
    {
        return value;
    }

    public String getType()
    {
        return type;
    }

    public Sign getSign()
    {
        return sign;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntervalLiteral(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("type", type)
                .add("sign", sign)
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

        IntervalLiteral that = (IntervalLiteral) o;

        if (sign != that.sign) {
            return false;
        }
        if (!type.equals(that.type)) {
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
        result = 31 * result + type.hashCode();
        result = 31 * result + (sign != null ? sign.hashCode() : 0);
        return result;
    }
}
