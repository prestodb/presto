package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class NullLiteral
        extends Expression
{
    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).toString();
    }
}
