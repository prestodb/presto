package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class DateLiteral
        extends Expression
{
    private final String value;

    public DateLiteral(String value)
    {
        this.value = value;
    }

    public String getValue()
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
