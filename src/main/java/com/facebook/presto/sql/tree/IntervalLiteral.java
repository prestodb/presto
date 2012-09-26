package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class IntervalLiteral
        extends Expression
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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("type", type)
                .add("sign", sign)
                .toString();
    }
}
